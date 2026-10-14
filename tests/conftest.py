from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from app.db import create_engine, create_session_factory
from app.messaging import PAYMENTS_DLQ, PAYMENTS_QUEUE, declare_topology
from app.models import Payment
from app.services import PaymentService
from tests.utils import payment_request


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:18-alpine", driver="asyncpg") as postgres:
        url = postgres.get_connection_url()
        # the schema is built by the real migrations, so they get tested too
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        command.upgrade(config, "head")
        yield url


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    container = (
        DockerContainer("rabbitmq:4.3-alpine")
        .with_exposed_ports(5672)
        .waiting_for(LogMessageWaitStrategy("Server startup complete"))
    )
    with container as rabbitmq:
        host = rabbitmq.get_container_host_ip()
        port = rabbitmq.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture(scope="session")
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    factory = create_session_factory(engine)
    async with factory.begin() as session:
        await session.execute(text("TRUNCATE payments, outbox"))
    return factory


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    """A connected broker with the topology declared and the main queues empty."""
    broker = RabbitBroker(rabbitmq_url, logger=None)
    await broker.connect()
    await declare_topology(broker)
    for queue in (PAYMENTS_QUEUE, PAYMENTS_DLQ):
        await (await broker.declare_queue(queue)).purge()
    yield broker
    await broker.stop()


@pytest.fixture
def make_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[..., Awaitable[Payment]]:
    counter = 0

    async def make(**overrides: Any) -> Payment:
        nonlocal counter
        counter += 1
        async with session_factory() as session:
            result = await PaymentService(session).create(
                payment_request(**overrides), idempotency_key=f"key-{counter}"
            )
        return result.payment

    return make
