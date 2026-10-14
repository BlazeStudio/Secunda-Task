"""Outbox relay: moves events from the outbox table to RabbitMQ.

Delivery is at-least-once. An event is marked as published only after the broker
confirms it, so a crash between the confirm and the commit leads to a duplicate,
never to a loss. The consumer is idempotent and tolerates duplicates.
"""

import asyncio
import contextlib
import logging
import signal
from uuid import UUID

from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import RelaySettings
from app.db import create_engine, create_session_factory
from app.logs import setup_logging
from app.messaging import PAYMENTS_EXCHANGE, declare_topology
from app.models import OutboxEvent
from app.repositories import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        error_backoff: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._error_backoff = error_backoff

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("Outbox relay started")
        while not stop.is_set():
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                await _wait(stop, self._error_backoff)
                continue
            # a full batch means there is probably more waiting, so don't sleep
            if published < self._batch_size:
                await _wait(stop, self._poll_interval)
        logger.info("Outbox relay stopped")

    async def publish_batch(self) -> int:
        failed: OutboxEvent | None = None
        error: Exception | None = None

        async with self._session_factory.begin() as session:
            outbox = OutboxRepository(session)
            published: list[UUID] = []
            for event in await outbox.lock_unpublished(self._batch_size):
                try:
                    await self._publish(event)
                except Exception as exc:
                    failed, error = event, exc
                    break
                published.append(event.id)
            # commit whatever the broker has confirmed, even if a later publish failed
            await outbox.mark_published(published)

        if published:
            logger.info("Published %d outbox event(s)", len(published))
        if failed is not None:
            raise RuntimeError(f"Failed to publish outbox event {failed.id}") from error
        return len(published)

    async def _publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.event_type,
            message_id=str(event.id),
            message_type=event.event_type,
            timestamp=event.created_at,
            persist=True,
        )


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for the given time, waking up early if a stop was requested."""
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(seconds):
            await stop.wait()


async def main() -> None:
    settings = RelaySettings()
    setup_logging(settings.log_level)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            loop.add_signal_handler(sig, stop.set)

    engine = create_engine(settings.database_url)
    # on_return_raises: an unroutable message must not be treated as delivered
    broker = RabbitBroker(settings.rabbitmq_url, default_channel=Channel(on_return_raises=True))
    try:
        await broker.connect()
        await declare_topology(broker)
        relay = OutboxRelay(
            create_session_factory(engine),
            broker,
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
            error_backoff=settings.outbox_error_backoff,
        )
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
