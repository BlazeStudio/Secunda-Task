import asyncio
import json
import random
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import ConsumerSettings
from app.consumer.app import ATTEMPT_HEADER, RetryPolicy, create_app
from app.consumer.gateway import EmulatedGateway
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhooks import WebhookDeliveryError, WebhookSender
from app.messaging import PAYMENT_CREATED, PAYMENTS_DLQ, PAYMENTS_EXCHANGE
from app.models import Payment, PaymentStatus
from app.relay import OutboxRelay
from tests.utils import wait_until

MakePayment = Callable[..., Awaitable[Payment]]


class StubGateway:
    """Returns the given outcomes in order, repeating the last one."""

    def __init__(self, *outcomes: PaymentStatus, delay: float = 0.0) -> None:
        self._outcomes = list(outcomes) or [PaymentStatus.SUCCEEDED]
        self._delay = delay
        self.charged: list[uuid.UUID] = []

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.charged.append(payment.id)
        outcome = self._outcomes.pop(0) if len(self._outcomes) > 1 else self._outcomes[0]
        await asyncio.sleep(self._delay)
        return outcome


class WebhookReceiver:
    """Records webhook calls and answers with the given status codes, then with 200."""

    def __init__(self, *statuses: int) -> None:
        self._statuses = list(statuses)
        self.requests: list[httpx.Request] = []
        self.received_at: list[float] = []

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.received_at.append(time.monotonic())
        status = self._statuses.pop(0) if self._statuses else 200
        if status == 0:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(status)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._handle))

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(r.content) for r in self.requests]


async def load(factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


class TestPaymentProcessor:
    @pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
    async def test_charges_pending_payment_and_sends_webhook(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        make_payment: MakePayment,
        outcome: PaymentStatus,
    ) -> None:
        payment = await make_payment()
        gateway, webhooks = StubGateway(outcome), WebhookReceiver()
        processor = PaymentProcessor(session_factory, gateway, WebhookSender(webhooks.client()))

        await processor.process(payment.id)

        stored = await load(session_factory, payment.id)
        assert stored.status is outcome
        assert stored.processed_at is not None
        assert gateway.charged == [payment.id]

        [request] = webhooks.requests
        assert request.method == "POST"
        assert str(request.url) == payment.webhook_url
        assert request.headers["content-type"] == "application/json"
        body = json.loads(request.content)
        assert body["payment_id"] == str(payment.id)
        assert body["status"] == outcome.value
        assert body["amount"] == "100.50"
        assert body["processed_at"] is not None

    async def test_repeated_message_does_not_charge_again(
        self, session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
    ) -> None:
        payment = await make_payment()
        gateway, webhooks = StubGateway(), WebhookReceiver()
        processor = PaymentProcessor(session_factory, gateway, WebhookSender(webhooks.client()))

        await processor.process(payment.id)
        await processor.process(payment.id)

        assert gateway.charged == [payment.id]
        assert [b["status"] for b in webhooks.bodies] == ["succeeded", "succeeded"]

    @pytest.mark.parametrize("status", [500, 404, 301, 0], ids=["5xx", "4xx", "redirect", "conn"])
    async def test_failed_webhook_keeps_payment_result(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        make_payment: MakePayment,
        status: int,
    ) -> None:
        payment = await make_payment()
        gateway, webhooks = StubGateway(PaymentStatus.FAILED), WebhookReceiver(status)
        processor = PaymentProcessor(session_factory, gateway, WebhookSender(webhooks.client()))

        with pytest.raises(WebhookDeliveryError):
            await processor.process(payment.id)
        assert (await load(session_factory, payment.id)).status is PaymentStatus.FAILED

        # the retry only re-sends the webhook
        await processor.process(payment.id)
        assert gateway.charged == [payment.id]
        assert len(webhooks.requests) == 2

    async def test_concurrent_processing_reports_a_single_outcome(
        self, session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
    ) -> None:
        payment = await make_payment()
        gateway = StubGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED, delay=0.1)
        webhooks = WebhookReceiver()
        processor = PaymentProcessor(session_factory, gateway, WebhookSender(webhooks.client()))

        await asyncio.gather(processor.process(payment.id), processor.process(payment.id))

        final = (await load(session_factory, payment.id)).status
        assert [b["status"] for b in webhooks.bodies] == [final.value, final.value]

    async def test_unknown_payment(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        webhooks = WebhookReceiver()
        processor = PaymentProcessor(
            session_factory, StubGateway(), WebhookSender(webhooks.client())
        )

        with pytest.raises(PaymentNotFoundError):
            await processor.process(uuid.uuid4())
        assert webhooks.requests == []


class TestRetryPolicy:
    def test_delays_grow_exponentially(self) -> None:
        policy = RetryPolicy(max_attempts=4, base_delay=2.0)
        assert policy.delays() == [2.0, 4.0, 8.0]

    def test_single_attempt_has_no_delays(self) -> None:
        assert RetryPolicy(max_attempts=1, base_delay=2.0).delays() == []


class TestEmulatedGateway:
    @pytest.mark.parametrize(
        ("success_rate", "expected"),
        [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)],
    )
    async def test_outcome_follows_success_rate(
        self, success_rate: float, expected: PaymentStatus
    ) -> None:
        gateway = EmulatedGateway(min_delay=0, max_delay=0, success_rate=success_rate)
        assert await gateway.charge(Payment()) is expected

    async def test_success_rate_is_roughly_respected(self) -> None:
        gateway = EmulatedGateway(min_delay=0, max_delay=0, rng=random.Random(42))
        outcomes = [await gateway.charge(Payment()) for _ in range(1000)]
        assert 850 < outcomes.count(PaymentStatus.SUCCEEDED) < 950


# --- the consumer wired to a real RabbitMQ -------------------------------------------

StartConsumer = Callable[..., Awaitable[FastStream]]


@pytest.fixture
async def start_consumer(
    database_url: str, rabbitmq_url: str, broker: RabbitBroker
) -> AsyncIterator[StartConsumer]:
    started: list[FastStream] = []

    async def start(gateway: StubGateway, webhooks: WebhookReceiver) -> FastStream:
        settings = ConsumerSettings(
            _env_file=None,
            database_url=database_url,
            rabbitmq_url=rabbitmq_url,
            retry_base_delay=0.2,
            log_level="WARNING",
        )
        app = create_app(settings, gateway=gateway, http_client=webhooks.client())
        await app.start()
        started.append(app)
        return app

    yield start
    for app in started:
        await app.stop()


async def publish_outbox(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    assert await OutboxRelay(session_factory, broker).publish_batch() > 0


async def next_dead_letter(broker: RabbitBroker, timeout: float = 10) -> AbstractIncomingMessage:
    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    message: AbstractIncomingMessage | None = None

    async def arrived() -> bool:
        nonlocal message
        message = await dlq.get(fail=False, no_ack=True)
        return message is not None

    await wait_until(arrived, timeout=timeout)
    assert message is not None
    return message


async def is_empty(broker: RabbitBroker, queue: RabbitQueue) -> bool:
    declared = await broker.declare_queue(queue)
    return await declared.get(fail=False, no_ack=True) is None


class TestConsumer:
    async def test_processes_payment_end_to_end(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        start_consumer: StartConsumer,
        make_payment: MakePayment,
    ) -> None:
        payment = await make_payment()
        gateway, webhooks = StubGateway(), WebhookReceiver()
        await start_consumer(gateway, webhooks)

        await publish_outbox(session_factory, broker)
        await wait_until(lambda: bool(webhooks.requests))

        assert (await load(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED
        assert webhooks.bodies[0]["status"] == "succeeded"
        assert await is_empty(broker, PAYMENTS_DLQ)

    async def test_retries_webhook_with_backoff(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        start_consumer: StartConsumer,
        make_payment: MakePayment,
    ) -> None:
        await make_payment()
        gateway, webhooks = StubGateway(), WebhookReceiver(503, 0)
        await start_consumer(gateway, webhooks)

        await publish_outbox(session_factory, broker)
        await wait_until(lambda: len(webhooks.requests) == 3)

        first, second, third = webhooks.received_at
        assert second - first >= 0.2
        assert third - second >= 0.4
        assert len(gateway.charged) == 1
        assert await is_empty(broker, PAYMENTS_DLQ)

    async def test_moves_message_to_dlq_after_three_attempts(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        start_consumer: StartConsumer,
        make_payment: MakePayment,
    ) -> None:
        payment = await make_payment()
        gateway, webhooks = StubGateway(), WebhookReceiver(*[500] * 10)
        await start_consumer(gateway, webhooks)

        await publish_outbox(session_factory, broker)
        dead = await next_dead_letter(broker)

        assert json.loads(dead.body) == {"payment_id": str(payment.id)}
        assert dead.headers[ATTEMPT_HEADER] == 3
        assert len(webhooks.requests) == 3
        assert len(gateway.charged) == 1
        # the charge itself went through, only the notification is stuck
        assert (await load(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED

    async def test_unknown_payment_goes_straight_to_dlq(
        self, broker: RabbitBroker, start_consumer: StartConsumer
    ) -> None:
        webhooks = WebhookReceiver()
        await start_consumer(StubGateway(), webhooks)

        payment_id = str(uuid.uuid4())
        await broker.publish(
            {"payment_id": payment_id}, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENT_CREATED
        )
        dead = await next_dead_letter(broker)

        assert json.loads(dead.body) == {"payment_id": payment_id}
        assert ATTEMPT_HEADER not in (dead.headers or {})
        assert webhooks.requests == []

    @pytest.mark.parametrize(
        "body", [b"not json", b'{"payment_id": "nope"}', b"{}"], ids=["garbage", "bad id", "empty"]
    )
    async def test_malformed_message_goes_straight_to_dlq(
        self, broker: RabbitBroker, start_consumer: StartConsumer, body: bytes
    ) -> None:
        await start_consumer(StubGateway(), WebhookReceiver())

        await broker.publish(
            body,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENT_CREATED,
            content_type="application/json",
        )
        dead = await next_dead_letter(broker)

        assert dead.body == body
