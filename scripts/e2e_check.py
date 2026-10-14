"""End-to-end check of a running docker compose stack. Standard library only.

    docker compose up --build -d --wait
    python scripts/e2e_check.py                 # about a minute and a half
    python scripts/e2e_check.py --skip-outage   # without stopping RabbitMQ

Goes through every requirement of the assignment against the real services:
API, validation, auth, idempotency, outbox, RabbitMQ topology, processing,
webhooks, retries with backoff, DLQ and recovery after a broker outage.
Assumes the default retry settings (3 attempts, 2s base delay).
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

ROOT = Path(__file__).resolve().parent.parent

API_URL = os.environ.get("API_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "dev-api-key-change-me")
RABBITMQ_API = os.environ.get("RABBITMQ_API", "http://localhost:15672/api")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest:guest")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8081")
# the same receiver as seen from the consumer container
RECEIVER_IN_NETWORK = "http://webhook-receiver:8080"

RUN_ID = uuid.uuid4().hex[:8]
FINAL_STATUSES = {"succeeded", "failed"}
PAYMENTS = "/api/v1/payments"

Json = Any


# --- plumbing -------------------------------------------------------------------------


class Response(NamedTuple):
    status: int
    headers: dict[str, str]
    body: Json


def http(
    method: str, url: str, body: Json = None, headers: dict[str, str] | None = None
) -> Response:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            status, raw_headers, payload = resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as exc:
        status, raw_headers, payload = exc.code, exc.headers, exc.read()
    try:
        parsed = json.loads(payload) if payload else None
    except ValueError:
        parsed = payload.decode(errors="replace")
    return Response(status, {k.lower(): v for k, v in raw_headers.items()}, parsed)


def api(
    method: str, path: str, body: Json = None, *, key: str | None = API_KEY, idem: str | None = None
) -> Response:
    headers = {}
    if key is not None:
        headers["X-API-Key"] = key
    if idem is not None:
        headers["Idempotency-Key"] = idem
    return http(method, API_URL + path, body, headers)


def rabbit(method: str, path: str, body: Json = None) -> Response:
    token = base64.b64encode(RABBITMQ_USER.encode()).decode()
    return http(method, RABBITMQ_API + path, body, {"Authorization": f"Basic {token}"})


def sql(query: str) -> str:
    psql = ["psql", "-U", "payments", "-d", "payments", "-tAc", query]
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", *psql],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, check=True)


def wait_for(predicate: Callable[[], Json], timeout: float, interval: float = 0.5) -> Json:
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result or time.monotonic() > deadline:
            return result
        time.sleep(interval)


def webhook_url(path: str) -> str:
    return f"{RECEIVER_IN_NETWORK}/e2e/{RUN_ID}/{path}"


def payment_body(path: str, **overrides: Json) -> dict[str, Json]:
    body = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": f"e2e check {RUN_ID}",
        "metadata": {"run": RUN_ID, "nested": {"list": [1, 2, 3]}},
        "webhook_url": webhook_url(path),
    }
    body.update(overrides)
    return body


def create(path: str, **overrides: Json) -> dict[str, Json]:
    response = api("POST", PAYMENTS, payment_body(path, **overrides), idem=str(uuid.uuid4()))
    if response.status != 202:
        raise RuntimeError(f"payment not created: {response}")
    created: dict[str, Json] = response.body
    return created


def get_payment(payment_id: str) -> dict[str, Json]:
    payment: dict[str, Json] = api("GET", f"{PAYMENTS}/{payment_id}").body
    return payment


def outbox_published(payment_id: str) -> bool:
    query = (
        f"select published_at is not null from outbox where payload->>'payment_id' = '{payment_id}'"
    )
    return sql(query) == "t"


def wait_processed(payment_id: str, timeout: float = 20) -> dict[str, Json] | None:
    def processed() -> dict[str, Json] | None:
        payment = get_payment(payment_id)
        return payment if payment["status"] in FINAL_STATUSES else None

    result: dict[str, Json] | None = wait_for(processed, timeout)
    return result


def webhooks(path: str) -> list[dict[str, Json]]:
    """Webhooks the receiver got on the given path of this run (prefix match)."""
    received: list[dict[str, Json]] = http("GET", f"{RECEIVER_URL}/e2e/{RUN_ID}/{path}").body
    return received


def wait_webhooks(path: str, count: int, timeout: float) -> list[dict[str, Json]]:
    def enough() -> list[dict[str, Json]] | None:
        received = webhooks(path)
        return received if len(received) >= count else None

    return wait_for(enough, timeout) or webhooks(path)


def dead_letters() -> list[dict[str, Json]]:
    """Peek at the DLQ without consuming it."""
    response = rabbit(
        "POST",
        "/queues/%2F/payments.new.dlq/get",
        {"count": 10_000, "ackmode": "ack_requeue_true", "encoding": "auto"},
    )
    messages: list[dict[str, Json]] = response.body if response.status == 200 else []
    return messages


def find_dead_letter(marker: str) -> dict[str, Json] | None:
    return next((m for m in dead_letters() if marker in m["payload"]), None)


# --- reporting ------------------------------------------------------------------------


class Report:
    def __init__(self) -> None:
        self.passed = 0
        self.failed: list[str] = []

    def check(self, name: str, ok: Json, details: Json = "") -> bool:
        if ok:
            self.passed += 1
            print(f"  [ok]   {name}")
        else:
            self.failed.append(name)
            print(f"  [FAIL] {name}")
            if details not in ("", None):
                print(f"         {details}")
        return bool(ok)

    def section(self, title: str, body: Callable[["Report"], None]) -> None:
        print(f"\n== {title} ==")
        try:
            body(self)
        except Exception as exc:  # keep going with the next section
            self.check(f"раздел прерван: {exc!r}", False)


def info(text: str) -> None:
    print(f"         {text}")


# --- scenarios ------------------------------------------------------------------------


def environment(r: Report) -> None:
    r.check("API отвечает на /health", api("GET", "/health", key=None).status == 200)
    r.check("Swagger доступен на /docs", api("GET", "/docs", key=None).status == 200)
    r.check("RabbitMQ management API доступен", rabbit("GET", "/overview").status == 200)
    r.check("приёмник webhook'ов доступен", http("GET", f"{RECEIVER_URL}/health").status == 200)
    version = sql("select version_num from alembic_version")
    r.check("PostgreSQL доступен, миграции применены", version == "0001", version)


def auth(r: Report) -> None:
    body, idem = payment_body("auth"), str(uuid.uuid4())
    unknown = f"{PAYMENTS}/{uuid.uuid4()}"
    for name, key in (("без X-API-Key", None), ("с неверным ключом", "wrong-key")):
        status = api("POST", PAYMENTS, body, key=key, idem=idem).status
        r.check(f"POST {name} -> 401", status == 401, status)
        status = api("GET", unknown, key=key).status
        r.check(f"GET {name} -> 401", status == 401, status)


def validation(r: Report) -> None:
    status = api("POST", PAYMENTS, payment_body("validation")).status
    r.check("без Idempotency-Key -> 422", status == 422, status)

    cases: dict[str, dict[str, Json]] = {
        "сумма 0": {"amount": "0"},
        "отрицательная сумма": {"amount": "-5"},
        "три знака после запятой": {"amount": "10.001"},
        "сумма не число": {"amount": "abc"},
        "валюта GBP": {"currency": "GBP"},
        "пустое описание": {"description": ""},
        "webhook_url не URL": {"webhook_url": "not-a-url"},
        "webhook_url не http(s)": {"webhook_url": "ftp://example.com/hook"},
        "metadata не объект": {"metadata": [1, 2]},
        "лишнее поле": {"status": "succeeded"},
    }
    for name, overrides in cases.items():
        body = payment_body("validation", **overrides)
        status = api("POST", PAYMENTS, body, idem=str(uuid.uuid4())).status
        r.check(f"{name} -> 422", status == 422, f"получили {status}")

    r.check("GET с невалидным UUID -> 422", api("GET", f"{PAYMENTS}/123").status == 422)
    r.check(
        "GET несуществующего платежа -> 404", api("GET", f"{PAYMENTS}/{uuid.uuid4()}").status == 404
    )


def creation_and_idempotency(r: Report) -> None:
    idem = f"e2e-{RUN_ID}-idem"
    body = payment_body("idempotency")

    first = api("POST", PAYMENTS, body, idem=idem)
    created = first.body
    r.check("создание -> 202 Accepted", first.status == 202, first)
    r.check(
        "в ответе payment_id, status=pending, created_at",
        set(created) == {"payment_id", "status", "created_at"} and created["status"] == "pending",
        created,
    )
    location = first.headers.get("location")
    r.check(
        "заголовок Location указывает на платёж", location == f"{PAYMENTS}/{created['payment_id']}"
    )

    payment = get_payment(created["payment_id"])
    sent = {k: body[k] for k in ("amount", "currency", "description", "metadata", "webhook_url")}
    r.check("GET возвращает переданные данные", {k: payment[k] for k in sent} == sent, payment)
    r.check("GET возвращает idempotency_key", payment["idempotency_key"] == idem)

    replay = api("POST", PAYMENTS, body, idem=idem)
    r.check(
        "повтор с тем же ключом -> тот же платёж",
        replay.status == 202 and replay.body["payment_id"] == created["payment_id"],
        replay,
    )
    r.check(
        "у повтора заголовок Idempotent-Replayed: true",
        replay.headers.get("idempotent-replayed") == "true",
    )

    conflict = api("POST", PAYMENTS, {**body, "amount": "999.00"}, idem=idem)
    r.check("тот же ключ с другим телом -> 409", conflict.status == 409, conflict)

    parallel_key = f"e2e-{RUN_ID}-parallel"
    with ThreadPoolExecutor(10) as pool:
        responses = list(
            pool.map(lambda _: api("POST", PAYMENTS, body, idem=parallel_key), range(10))
        )
    ids = {resp.body["payment_id"] for resp in responses if resp.status == 202}
    fresh = sum("idempotent-replayed" not in resp.headers for resp in responses)
    r.check("10 параллельных запросов с одним ключом -> один платёж", len(ids) == 1, ids)
    r.check("создал его ровно один из запросов", fresh == 1, f"без Idempotent-Replayed: {fresh}")

    keys = f"('{idem}', '{parallel_key}')"
    rows = sql(f"select count(*) from payments where idempotency_key in {keys}")
    r.check("в БД по одной записи на ключ", rows == "2", f"строк: {rows}")
    events = sql(
        "select count(*) from outbox where payload->>'payment_id' in "
        f"(select id::text from payments where idempotency_key in {keys})"
    )
    r.check("в outbox по одному событию на платёж", events == "2", f"событий: {events}")


def topology(r: Report) -> None:
    def exchange(name: str) -> dict[str, Json]:
        return rabbit("GET", f"/exchanges/%2F/{name}").body or {}

    def queue(name: str) -> dict[str, Json]:
        return rabbit("GET", f"/queues/%2F/{name}").body or {}

    def binding_keys(source: str, destination: str) -> set[str]:
        bindings = rabbit("GET", f"/bindings/%2F/e/{source}/q/{destination}").body or []
        return {b["routing_key"] for b in bindings}

    for name in ("payments", "payments.dlx"):
        ex = exchange(name)
        r.check(
            f"exchange {name}: direct, durable",
            ex.get("type") == "direct" and ex.get("durable"),
            ex,
        )

    main = queue("payments.new")
    args = main.get("arguments", {})
    r.check(
        "очередь payments.new: durable, dead-letter в payments.dlx",
        main.get("durable") and args.get("x-dead-letter-exchange") == "payments.dlx",
        args,
    )
    r.check(
        "payments.new привязана к exchange payments по ключу payment.created",
        "payment.created" in binding_keys("payments", "payments.new"),
    )
    r.check("у payments.new есть подписчик (consumer)", (main.get("consumers") or 0) >= 1)
    r.check(
        "очередь payments.new.dlq привязана к payments.dlx",
        queue("payments.new.dlq").get("durable")
        and "payments.new" in binding_keys("payments.dlx", "payments.new.dlq"),
    )
    for delay in (2000, 4000):
        args = queue(f"payments.new.retry.{delay}ms").get("arguments", {})
        r.check(
            f"очередь payments.new.retry.{delay}ms: TTL {delay} мс, возврат в payments.new",
            args.get("x-message-ttl") == delay
            and args.get("x-dead-letter-routing-key") == "payments.new",
            args,
        )


def processing(r: Report) -> None:
    payment_id = create("basic")["payment_id"]
    info(f"платёж {payment_id}")

    published = wait_for(lambda: outbox_published(payment_id), timeout=5)
    r.check("relay опубликовал событие (outbox.published_at заполнен)", published)

    payment = wait_processed(payment_id)
    r.check("платёж обработан за 20 с (succeeded или failed)", payment is not None)
    if payment is None:
        return
    if r.check("processed_at заполнен", payment["processed_at"] is not None):
        took = datetime.fromisoformat(payment["processed_at"]) - datetime.fromisoformat(
            payment["created_at"]
        )
        seconds = took.total_seconds()
        r.check(f"статус {payment['status']}, обработка заняла {seconds:.1f} с", seconds >= 2)

    received = wait_webhooks("basic", 1, timeout=5)
    r.check("webhook пришёл на webhook_url", received)
    if not received:
        return
    r.check(
        "тело webhook'а совпадает с GET /payments/{id}",
        received[0]["body"] == payment,
        received[0]["body"],
    )
    r.check(
        "webhook отправлен как application/json", received[0]["content_type"] == "application/json"
    )
    time.sleep(2)
    r.check("webhook пришёл ровно один раз", len(webhooks("basic")) == 1)


def retries_and_dlq(r: Report) -> None:
    # the first receiver fails twice and then accepts, the second never accepts
    flaky = create("flaky", webhook_url=webhook_url("flaky?fail=2"))["payment_id"]
    broken = create("broken", webhook_url=webhook_url("broken?fail=100"))["payment_id"]
    info(f"webhook отвечает 503 два раза: {flaky}")
    info(f"webhook отвечает 503 всегда:   {broken}")

    calls = wait_webhooks("flaky", 3, timeout=30)
    statuses = [c["status"] for c in calls]
    r.check(
        "webhook с двумя отказами доставлен с третьей попытки",
        statuses == [503, 503, 200],
        statuses,
    )
    if len(calls) >= 3:
        first_gap = calls[1]["timestamp"] - calls[0]["timestamp"]
        second_gap = calls[2]["timestamp"] - calls[1]["timestamp"]
        r.check(f"пауза перед 2-й попыткой ~2 с (была {first_gap:.2f} с)", 1.9 <= first_gap <= 3.5)
        r.check(
            f"пауза перед 3-й попыткой ~4 с (была {second_gap:.2f} с)", 3.9 <= second_gap <= 5.5
        )
        outcomes = {(c["body"]["status"], c["body"]["processed_at"]) for c in calls}
        r.check(
            "во всех попытках один и тот же результат: повторного списания нет", len(outcomes) == 1
        )

    dead = wait_for(lambda: find_dead_letter(broken), timeout=30)
    r.check("после 3 неудачных попыток сообщение в payments.new.dlq", dead)
    if dead:
        headers = dead["properties"].get("headers", {})
        reasons = {d.get("reason") for d in headers.get("x-death", [])}
        r.check(
            "в сообщении заголовок x-attempt = 3",
            headers.get("x-attempt") == 3,
            headers.get("x-attempt"),
        )
        r.check("RabbitMQ пометил его как rejected (x-death)", "rejected" in reasons, reasons)
    time.sleep(3)
    attempts = len(webhooks("broken"))
    r.check("четвёртой попытки не было", attempts == 3, f"попыток: {attempts}")
    r.check(
        "сам платёж проведён, застряло только уведомление",
        get_payment(broken)["status"] in FINAL_STATUSES,
    )
    r.check("успешно доставленный платёж в DLQ не попал", find_dead_letter(flaky) is None)


def poison_messages(r: Report) -> None:
    def publish(payload: str) -> bool:
        message = {
            "properties": {"content_type": "application/json", "delivery_mode": 2},
            "routing_key": "payment.created",
            "payload": payload,
            "payload_encoding": "string",
        }
        response = rabbit("POST", "/exchanges/%2F/payments/publish", message)
        return bool(response.body and response.body.get("routed"))

    marker = uuid.uuid4()
    cases = {
        "не JSON": f"not json {marker}",
        "JSON без payment_id": json.dumps({"something": str(marker)}),
        "несуществующий платёж": json.dumps({"payment_id": str(marker)}),
    }
    for name, payload in cases.items():
        r.check(f"сообщение '{name}' опубликовано в payments.new", publish(payload))
        landed = wait_for(lambda: find_dead_letter(payload), timeout=10)  # noqa: B023
        r.check(f"'{name}' сразу ушло в DLQ, без ретраев", landed)


def concurrency(r: Report) -> None:
    count = 20
    started = time.monotonic()
    with ThreadPoolExecutor(count) as pool:
        created = list(pool.map(lambda i: create(f"bulk/{i}"), range(count)))
        results = list(pool.map(lambda c: wait_processed(c["payment_id"], timeout=40), created))
    elapsed = time.monotonic() - started

    statuses = [p["status"] for p in results if p]
    r.check(f"{count} платежей обработаны за {elapsed:.1f} с", len(statuses) == count)
    succeeded, failed = statuses.count("succeeded"), statuses.count("failed")
    info(f"succeeded: {succeeded}, failed: {failed} (в среднем 10% failed)")

    time.sleep(1)
    received = webhooks("bulk/")
    per_client = [sum(w["path"].endswith(f"/bulk/{i}") for w in received) for i in range(count)]
    r.check("каждый клиент получил ровно один webhook", per_client == [1] * count, per_client)


def broker_outage(r: Report) -> None:
    info("останавливаем RabbitMQ...")
    compose("stop", "rabbitmq")
    try:
        response = api("POST", PAYMENTS, payment_body("outage"), idem=str(uuid.uuid4()))
        r.check("при лежащем брокере API принимает платёж (202)", response.status == 202, response)
        payment_id = response.body["payment_id"]
        time.sleep(3)
        r.check("событие ждёт в outbox неопубликованным", not outbox_published(payment_id))
        r.check("платёж пока pending", get_payment(payment_id)["status"] == "pending")
    finally:
        info("запускаем RabbitMQ обратно...")
        compose("start", "rabbitmq")

    payment = wait_processed(payment_id, timeout=90)
    r.check("relay и consumer переподключились, платёж обработан", payment is not None)
    r.check("событие помечено опубликованным", outbox_published(payment_id))
    r.check("webhook доставлен", wait_webhooks("outage", 1, timeout=10))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--skip-outage", action="store_true", help="не останавливать RabbitMQ")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):  # Windows pipes default to the ANSI code page
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print(f"Прогон {RUN_ID}: API {API_URL}, RabbitMQ {RABBITMQ_API}, приёмник {RECEIVER_URL}")
    report = Report()
    report.section("1. Окружение", environment)
    report.section("2. Авторизация (X-API-Key)", auth)
    report.section("3. Валидация", validation)
    report.section("4. Создание платежа и идемпотентность", creation_and_idempotency)
    report.section("5. Топология RabbitMQ", topology)
    report.section("6. Outbox -> consumer -> webhook", processing)
    report.section("7. Ретраи с экспоненциальной задержкой и DLQ", retries_and_dlq)
    report.section("8. Битые сообщения", poison_messages)
    report.section("9. Параллельная обработка", concurrency)
    if args.skip_outage:
        print("\n== 10. Падение брокера: пропущено (--skip-outage) ==")
    else:
        report.section("10. Падение брокера: гарантия outbox", broker_outage)

    print(f"\nWebhook'и этого прогона: {RECEIVER_URL}/e2e/{RUN_ID}")
    total = report.passed + len(report.failed)
    if report.failed:
        print(f"ИТОГ: пройдено {report.passed} из {total}. Не прошли:")
        for name in report.failed:
            print(f"  - {name}")
        sys.exit(1)
    print(f"ИТОГ: все {total} проверок пройдены")


if __name__ == "__main__":
    main()
