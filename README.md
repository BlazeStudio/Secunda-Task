# Payments Service

Микросервис асинхронной обработки платежей. Принимает платёж по HTTP, проводит его через эмулятор платёжного шлюза и сообщает результат клиенту webhook'ом.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (asyncio), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Запуск

```bash
docker compose up --build -d
```

Поднимутся:

| Сервис | Назначение |
|---|---|
| `postgres`, `rabbitmq` | инфраструктура |
| `migrations` | `alembic upgrade head`, отрабатывает один раз до старта приложений |
| `api` | HTTP API, http://localhost:8000, Swagger — http://localhost:8000/docs |
| `outbox-relay` | перекладывает события из таблицы `outbox` в RabbitMQ |
| `consumer` | обрабатывает платежи и шлёт webhook'и |
| `webhook-receiver` | тестовый «сервер клиента»: принимает webhook'и и показывает их на http://localhost:8081 |

API-ключ по умолчанию — `dev-api-key-change-me`. Ключ и порты переопределяются через `.env` (см. `.env.example`).

## Проверка всего функционала

```bash
python scripts/e2e_check.py
```

Скрипт без зависимостей (нужен только Python 3.11+) проходит по всем пунктам ТЗ на живом окружении и печатает результат по каждой проверке, в конце сводку. Занимает около 40 секунд:

1. окружение: API, Swagger, RabbitMQ, PostgreSQL и применённые миграции;
2. авторизация по `X-API-Key`;
3. валидация тела, заголовка `Idempotency-Key` и UUID в пути;
4. создание платежа и идемпотентность: повтор, конфликт, 10 параллельных запросов с одним ключом, ровно одна запись в `payments` и `outbox`;
5. топология RabbitMQ: exchange'и, очереди, привязки, DLX, очереди задержки;
6. полный путь outbox → relay → consumer → webhook, тело webhook'а совпадает с `GET`;
7. ретраи: паузы ~2 и ~4 с, повторного списания нет, после третьей неудачи сообщение в DLQ;
8. битые сообщения уходят в DLQ сразу;
9. 20 платежей параллельно, каждый клиент получает ровно один webhook;
10. падение брокера: RabbitMQ останавливается, платёж всё равно принимается и ждёт в outbox, после запуска брокера доходит до клиента. Пропустить: `--skip-outage`.

Код возврата 0, если всё прошло, иначе 1. Этот же скрипт запускается в CI.

## Где смотреть результат

Графического интерфейса у сервиса нет и по ТЗ он не нужен. Всё видно через стандартные инструменты:

| Что | Где |
|---|---|
| Ручные запросы к API | Swagger: http://localhost:8000/docs, кнопка **Authorize**, ключ `dev-api-key-change-me` |
| Пришедшие webhook'и | http://localhost:8081/ — все, http://localhost:8081/demo — только на путь `/demo`; в каждом номер попытки, код ответа, время и тело |
| Очереди, exchange'и, DLQ | http://localhost:15672 (guest / guest): Exchanges → `payments`, `payments.dlx`; Queues → `payments.new`, `payments.new.retry.*`, `payments.new.dlq`; содержимое DLQ — Get messages |
| Что делают процессы | `docker compose logs -f consumer outbox-relay webhook-receiver` |
| Таблицы | `docker compose exec postgres psql -U payments -c "select id, status, processed_at from payments"` и `... "select * from outbox"` |

## Примеры

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments   -H "X-API-Key: dev-api-key-change-me"   -H "Idempotency-Key: 3f1c2a9e-order-1042"   -H "Content-Type: application/json"   -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042},
        "webhook_url": "http://webhook-receiver:8080/demo"
      }'
```

```http
HTTP/1.1 202 Accepted
location: /api/v1/payments/0b5e4a52-7f43-4c55-a5c1-2a8c1d0a7a51

{"payment_id": "0b5e4a52-7f43-4c55-a5c1-2a8c1d0a7a51", "status": "pending", "created_at": "2026-10-14T09:12:03.512345Z"}
```

Повтор того же запроса с тем же `Idempotency-Key` вернёт тот же платёж (и заголовок `Idempotent-Replayed: true`), новый платёж не создаётся. Тот же ключ с другим телом — `409 Conflict`.

Через 2–5 секунд платёж обработан:

```bash
curl http://localhost:8000/api/v1/payments/0b5e4a52-7f43-4c55-a5c1-2a8c1d0a7a51   -H "X-API-Key: dev-api-key-change-me"
```

```json
{
  "payment_id": "0b5e4a52-7f43-4c55-a5c1-2a8c1d0a7a51",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1042",
  "metadata": {"order_id": 1042},
  "status": "succeeded",
  "idempotency_key": "3f1c2a9e-order-1042",
  "webhook_url": "http://webhook-receiver:8080/demo",
  "created_at": "2026-10-14T09:12:03.512345Z",
  "processed_at": "2026-10-14T09:12:07.044120Z"
}
```

Webhook приходит `POST`-запросом на `webhook_url` с этим же JSON в теле; он появится на http://localhost:8081/demo.

Тестовый приёмник умеет имитировать сбои клиента: параметр `?fail=N` в `webhook_url` заставляет его отвечать `503` на первые N запросов. С `http://webhook-receiver:8080/flaky?fail=2` будут видны две неудачные попытки с паузами 2 и 4 секунды и успешная третья. С `http://webhook-receiver:8080/dead?fail=100` после трёх попыток сообщение окажется в очереди `payments.new.dlq`.

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key`.

| Метод | Путь | Ответы |
|---|---|---|
| `POST` | `/api/v1/payments` | `202` принят; `401` нет/неверный ключ; `409` ключ идемпотентности уже использован с другим телом; `422` невалидное тело или нет `Idempotency-Key` |
| `GET` | `/api/v1/payments/{payment_id}` | `200`; `401`; `404` платёж не найден; `422` невалидный UUID |
| `GET` | `/health` | `200`, без ключа — для healthcheck'ов оркестратора |

Валидация тела: `amount` > 0, не больше двух знаков после запятой (хранится как `NUMERIC(18,2)`, в JSON отдаётся строкой, чтобы не терять точность); `currency` — `RUB`, `USD`, `EUR`; `description` — 1..1024 символа; `metadata` — JSON-объект до 16 КБ, необязательный; `webhook_url` — http(s) URL. Неизвестные поля отклоняются. `Idempotency-Key` — 1..255 символов.

## Как это устроено

```
                 POST /api/v1/payments
   client ───────────────────────────────► api
                                            │  одна транзакция:
                                            │  INSERT payments + INSERT outbox
                                            ▼
                                       PostgreSQL
                                            │  SELECT … FOR UPDATE SKIP LOCKED
                                            ▼
                                       outbox-relay
                                            │  publisher confirms
                                            ▼
   [payments] ──payment.created──► payments.new ──► consumer ──► шлюз (эмуляция)
                                        │   ▲            │
              reject после 3-й попытки  │   │ TTL истёк  └──► UPDATE payments, webhook
                                        ▼   │
   [payments.dlx] ──► payments.new.dlq     payments.new.retry.2000ms
                                           payments.new.retry.4000ms
```

### Outbox

Платёж и событие `payment.created` пишутся в одной транзакции, поэтому не бывает ни платежа без события, ни события без платежа. `outbox-relay` раз в 0.5 с выбирает пачку неопубликованных событий через `FOR UPDATE SKIP LOCKED` (несколько экземпляров relay не мешают друг другу), публикует их с publisher confirms и `mandatory`, и только после подтверждения брокера проставляет `published_at`. Если брокер недоступен, события остаются в таблице и уйдут, когда он вернётся.

Гарантия — at-least-once: если relay упадёт между подтверждением от брокера и коммитом, событие уйдёт повторно. Поэтому consumer идемпотентен.

Relay вынесен в отдельный процесс, а не в фоновую задачу API: так его можно масштабировать и перезапускать независимо, а API занимается только запросами.

### Идемпотентность API

Уникальный индекс по `idempotency_key` и `INSERT … ON CONFLICT DO NOTHING RETURNING`. Конкурентные запросы с одним ключом упираются в индекс, и платёж создаёт ровно один из них, остальные получают уже созданный. Если тело повторного запроса отличается от исходного, это скорее ошибка клиента, чем ретрай, поэтому ответ `409`.

### Consumer

1. Читает платёж из БД (в сообщении только `payment_id`, источник правды — база).
2. Если платёж ещё `pending` — вызывает шлюз (2–5 с, 90% успех). Транзакция на время ожидания не держится.
3. Сохраняет результат условным `UPDATE … WHERE status = 'pending'`. Если два обработчика получили одно и то же сообщение, статус выставит только один, а второй отправит в webhook уже сохранённый результат.
4. Отправляет webhook. Успех — только ответ 2xx; редиректы, 4xx/5xx, таймауты и ошибки соединения считаются неудачей.

Отказ шлюза (те самые 10%) — это результат платежа (`failed`), а не ошибка обработки: он сохраняется, клиент получает webhook со статусом `failed`, ретраев нет.

### Ретраи и DLQ

Ретраятся ошибки обработки: недоставленный webhook, недоступная БД и т.п. Номер попытки едет в заголовке `x-attempt`.

При ошибке consumer кладёт копию сообщения в очередь задержки `payments.new.retry.<delay>` (у неё задан `x-message-ttl`, а dead-letter настроен обратно в `payments.new`) и подтверждает исходное сообщение. Задержка растёт экспоненциально: `RETRY_BASE_DELAY * 2^(attempt-1)`, по умолчанию 2 и 4 секунды. По сравнению с `asyncio.sleep` в обработчике такой вариант не занимает слоты prefetch на время ожидания и переживает перезапуск consumer'а. На каждую задержку своя очередь, поэтому нет проблемы, когда сообщение с длинным TTL в голове очереди задерживает сообщения с коротким.

Повторная попытка не вызывает шлюз снова: платёж уже не `pending`, повторяется только отправка webhook'а.

После третьей неудачной попытки сообщение отклоняется (`reject`) и через `payments.dlx` попадает в `payments.new.dlq`. Туда же сразу, без ретраев, уходят сообщения, которые нет смысла повторять: битый JSON, невалидная схема, несуществующий платёж.

Платёж при этом уже имеет финальный статус — застряло только уведомление. Его видно через `GET`, а сообщение из DLQ можно переотправить в `payments.new` (например, shovel'ом из панели RabbitMQ).

## Конфигурация

Настройки читаются из переменных окружения (и из `.env` при локальном запуске).

| Переменная | По умолчанию | Кто использует |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | все |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | все |
| `LOG_LEVEL` | `INFO` | все |
| `API_KEY` | — (обязательная, от 16 символов) | api |
| `OUTBOX_BATCH_SIZE` | `100` | outbox-relay |
| `OUTBOX_POLL_INTERVAL` | `0.5` | outbox-relay |
| `OUTBOX_ERROR_BACKOFF` | `5.0` | outbox-relay |
| `CONSUMER_PREFETCH` | `10` | consumer — сколько платежей обрабатывается параллельно |
| `MAX_ATTEMPTS` | `3` | consumer |
| `RETRY_BASE_DELAY` | `2.0` | consumer |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | consumer |
| `GATEWAY_SUCCESS_RATE` | `0.9` | consumer |
| `WEBHOOK_TIMEOUT` | `10.0` | consumer |

API-ключ получает только `api`: consumer'у и relay он не нужен. Consumer масштабируется горизонтально: `docker compose up -d --scale consumer=3`.

## Разработка

Нужны [uv](https://docs.astral.sh/uv/) и Docker (тесты сами поднимают PostgreSQL и RabbitMQ через testcontainers).

```bash
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run mypy app tests scripts
```

На Docker Desktop под Windows testcontainers иногда не может получить порт своего служебного контейнера Ryuk; помогает `TESTCONTAINERS_RYUK_DISABLED=true`.

Тесты работают с настоящими PostgreSQL и RabbitMQ, схема создаётся реальными миграциями. Покрыто:

- API: создание и чтение, валидация, авторизация, идемпотентность (в том числе 10 параллельных запросов с одним ключом);
- outbox relay: публикация, пачки, сбой брокера посреди пачки, несколько relay параллельно без дублей;
- consumer: обработка, повторная доставка без повторного списания, гонка двух обработчиков, ретраи с задержкой, попадание в DLQ после трёх попыток, битые сообщения;
- миграции: соответствие моделям (`alembic check`) и цикл upgrade → downgrade → upgrade.

В CI (`.github/workflows/ci.yml`) — линтеры, тесты и `scripts/e2e_check.py` на поднятом `docker compose`.

## Структура

```
app/
  api/            FastAPI: приложение, роуты, зависимости (сессия БД, проверка ключа)
  consumer/       FastStream-приложение, обработчик платежа, эмулятор шлюза, отправка webhook'ов
  config.py       настройки (отдельный класс на каждый процесс)
  db.py           движок и фабрика сессий
  messaging.py    топология RabbitMQ: exchange'и, очереди, DLX, очереди задержки
  models.py       таблицы payments и outbox
  relay.py        outbox relay
  repositories.py доступ к данным
  schemas.py      Pydantic-схемы
  services.py     создание платежа: идемпотентность + outbox
migrations/       Alembic
scripts/          e2e-проверка поднятого окружения и тестовый приёмник webhook'ов
tests/
```