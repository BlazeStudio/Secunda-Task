import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import PAYMENT_CREATED, PAYMENTS_QUEUE
from app.models import OutboxEvent, Payment
from app.relay import OutboxRelay

MakePayment = Callable[..., Awaitable[Payment]]


async def outbox_events(factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with factory() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


class RecordingBroker:
    """Pretends to be a broker; can be told to fail on a given publish."""

    def __init__(self, fail_on: int | None = None, latency: float = 0.0) -> None:
        self.published: list[dict[str, Any]] = []
        self._fail_on = fail_on
        self._latency = latency

    async def publish(self, message: dict[str, Any], **kwargs: Any) -> None:
        await asyncio.sleep(self._latency)
        if self._fail_on is not None and len(self.published) + 1 == self._fail_on:
            raise ConnectionError("broker is gone")
        self.published.append(message)


async def test_publishes_event_to_payments_queue(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    make_payment: MakePayment,
) -> None:
    payment = await make_payment()
    relay = OutboxRelay(session_factory, broker)

    assert await relay.publish_batch() == 1

    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    message = await queue.get(fail=False, timeout=5)
    assert message is not None
    assert json.loads(message.body) == {"payment_id": str(payment.id)}
    assert message.content_type == "application/json"
    assert message.delivery_mode == 2  # persistent
    assert message.type == PAYMENT_CREATED

    [event] = await outbox_events(session_factory)
    assert message.message_id == str(event.id)
    assert event.published_at is not None


async def test_published_events_are_not_sent_again(
    session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
) -> None:
    await make_payment()
    broker = RecordingBroker()
    relay = OutboxRelay(session_factory, broker)  # type: ignore[arg-type]

    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0
    assert len(broker.published) == 1


async def test_respects_batch_size(
    session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
) -> None:
    for _ in range(5):
        await make_payment()
    relay = OutboxRelay(session_factory, RecordingBroker(), batch_size=2)  # type: ignore[arg-type]

    assert [await relay.publish_batch() for _ in range(4)] == [2, 2, 1, 0]


async def test_keeps_confirmed_events_when_broker_fails_mid_batch(
    session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
) -> None:
    for _ in range(3):
        await make_payment()
    relay = OutboxRelay(session_factory, RecordingBroker(fail_on=2))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="Failed to publish outbox event"):
        await relay.publish_batch()

    events = await outbox_events(session_factory)
    assert sum(e.published_at is not None for e in events) == 1

    # the rest goes out once the broker is back
    relay = OutboxRelay(session_factory, RecordingBroker())  # type: ignore[arg-type]
    assert await relay.publish_batch() == 2


async def test_parallel_relays_do_not_publish_the_same_event(
    session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
) -> None:
    for _ in range(20):
        await make_payment()
    brokers = [RecordingBroker(latency=0.01) for _ in range(3)]
    relays = [
        OutboxRelay(session_factory, b, batch_size=5)  # type: ignore[arg-type]
        for b in brokers
    ]

    async def drain(relay: OutboxRelay) -> None:
        while await relay.publish_batch():
            pass

    await asyncio.gather(*(drain(r) for r in relays))

    published = [m["payment_id"] for b in brokers for m in b.published]
    assert len(published) == 20
    assert len(set(published)) == 20
    assert all(b.published for b in brokers)


async def test_run_stops_on_request(
    session_factory: async_sessionmaker[AsyncSession], make_payment: MakePayment
) -> None:
    await make_payment()
    broker = RecordingBroker()
    relay = OutboxRelay(session_factory, broker, poll_interval=0.05)  # type: ignore[arg-type]
    stop = asyncio.Event()

    task = asyncio.create_task(relay.run(stop))
    async with asyncio.timeout(5):
        while not broker.published:
            await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(broker.published) == 1
