import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.main import create_app
from app.config import ApiSettings
from app.messaging import PAYMENT_CREATED
from app.models import OutboxEvent, Payment
from tests.utils import API_KEY

PAYMENTS_URL = "/api/v1/payments"


@pytest.fixture
async def client(
    database_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    settings = ApiSettings(_env_file=None, api_key=API_KEY, database_url=database_url)
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client


def body(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042, "customer": {"email": "user@example.com"}},
        "webhook_url": "https://example.com/webhooks/payments",
    }
    data.update(overrides)
    return data


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(session_factory: async_sessionmaker[AsyncSession], model: type[Any]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


async def test_create_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(PAYMENTS_URL, json=body(), headers=key())

    assert response.status_code == 202
    data = response.json()
    assert data.keys() == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"{PAYMENTS_URL}/{data['payment_id']}"
    assert "Idempotent-Replayed" not in response.headers

    async with session_factory() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1
    assert events[0].event_type == PAYMENT_CREATED
    assert events[0].payload == {"payment_id": data["payment_id"]}
    assert events[0].published_at is None


async def test_get_payment(client: httpx.AsyncClient) -> None:
    headers = key()
    created = (await client.post(PAYMENTS_URL, json=body(), headers=headers)).json()

    response = await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042, "customer": {"email": "user@example.com"}},
        "status": "pending",
        "idempotency_key": headers["Idempotency-Key"],
        "webhook_url": "https://example.com/webhooks/payments",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_amount_is_stored_with_two_decimal_places(client: httpx.AsyncClient) -> None:
    created = (await client.post(PAYMENTS_URL, json=body(amount=10.5), headers=key())).json()
    payment = (await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")).json()
    assert payment["amount"] == "10.50"


async def test_get_unknown_payment(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_get_payment_with_malformed_id(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/not-a-uuid")
    assert response.status_code == 422


class TestIdempotency:
    async def test_retry_returns_the_same_payment(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        headers = key()
        first = await client.post(PAYMENTS_URL, json=body(), headers=headers)
        second = await client.post(PAYMENTS_URL, json=body(), headers=headers)

        assert second.status_code == 202
        assert second.json() == first.json()
        assert second.headers["Idempotent-Replayed"] == "true"
        assert await count(session_factory, Payment) == 1
        assert await count(session_factory, OutboxEvent) == 1

    async def test_equivalent_body_is_treated_as_the_same_request(
        self, client: httpx.AsyncClient
    ) -> None:
        headers = key()
        first = await client.post(PAYMENTS_URL, json=body(amount="1500.00"), headers=headers)
        second = await client.post(PAYMENTS_URL, json=body(amount=1500), headers=headers)

        assert second.status_code == 202
        assert second.json()["payment_id"] == first.json()["payment_id"]

    @pytest.mark.parametrize(
        "changes",
        [
            {"amount": "1500.01"},
            {"currency": "USD"},
            {"description": "Another order"},
            {"metadata": {"order_id": 1043}},
            {"webhook_url": "https://example.com/other"},
        ],
    )
    async def test_reused_key_with_different_body_is_rejected(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        changes: dict[str, Any],
    ) -> None:
        headers = key()
        await client.post(PAYMENTS_URL, json=body(), headers=headers)

        response = await client.post(PAYMENTS_URL, json=body(**changes), headers=headers)

        assert response.status_code == 409
        assert await count(session_factory, Payment) == 1

    async def test_concurrent_requests_create_one_payment(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        headers = key()
        responses = await asyncio.gather(
            *(client.post(PAYMENTS_URL, json=body(), headers=headers) for _ in range(10))
        )

        assert {r.status_code for r in responses} == {202}
        assert len({r.json()["payment_id"] for r in responses}) == 1
        assert sum("Idempotent-Replayed" not in r.headers for r in responses) == 1
        assert await count(session_factory, Payment) == 1
        assert await count(session_factory, OutboxEvent) == 1

    @pytest.mark.parametrize(
        "headers",
        [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "k" * 256}],
        ids=["missing", "empty", "too long"],
    )
    async def test_key_is_required_and_bounded(
        self, client: httpx.AsyncClient, headers: dict[str, str]
    ) -> None:
        response = await client.post(PAYMENTS_URL, json=body(), headers=headers)
        assert response.status_code == 422


class TestAuth:
    @pytest.mark.parametrize(
        "headers",
        [{}, {"X-API-Key": "wrong-key"}, {"X-API-Key": ""}],
        ids=["missing", "wrong", "empty"],
    )
    @pytest.mark.parametrize("method", ["POST", "GET"])
    async def test_rejects_requests_without_valid_key(
        self, client: httpx.AsyncClient, headers: dict[str, str], method: str
    ) -> None:
        url = PAYMENTS_URL if method == "POST" else f"{PAYMENTS_URL}/{uuid.uuid4()}"
        # drop the key the client sends by default
        client.headers.pop("X-API-Key")

        response = await client.request(method, url, json=body(), headers={**key(), **headers})

        assert response.status_code == 401

    async def test_rejected_before_body_validation(self, client: httpx.AsyncClient) -> None:
        client.headers.pop("X-API-Key")
        response = await client.post(PAYMENTS_URL, json={})
        assert response.status_code == 401


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"amount": "0"}, id="zero amount"),
        pytest.param({"amount": "-10"}, id="negative amount"),
        pytest.param({"amount": "10.001"}, id="too many decimals"),
        pytest.param({"amount": "1e30"}, id="amount too large"),
        pytest.param({"amount": "NaN"}, id="nan amount"),
        pytest.param({"amount": "ten"}, id="non-numeric amount"),
        pytest.param({"currency": "GBP"}, id="unsupported currency"),
        pytest.param({"currency": "rub"}, id="lowercase currency"),
        pytest.param({"description": ""}, id="empty description"),
        pytest.param({"description": "x" * 1025}, id="long description"),
        pytest.param({"webhook_url": "not a url"}, id="malformed webhook url"),
        pytest.param({"webhook_url": "ftp://example.com/hook"}, id="non-http webhook url"),
        pytest.param({"metadata": ["a", "b"]}, id="metadata not an object"),
        pytest.param({"metadata": {"blob": "x" * 17_000}}, id="metadata too large"),
        pytest.param({"status": "succeeded"}, id="unknown field"),
    ],
)
async def test_invalid_body_is_rejected(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    changes: dict[str, Any],
) -> None:
    response = await client.post(PAYMENTS_URL, json=body(**changes), headers=key())

    assert response.status_code == 422
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize("field", ["amount", "currency", "description", "webhook_url"])
async def test_required_fields(client: httpx.AsyncClient, field: str) -> None:
    data = body()
    del data[field]
    response = await client.post(PAYMENTS_URL, json=data, headers=key())
    assert response.status_code == 422


async def test_metadata_is_optional(client: httpx.AsyncClient) -> None:
    data = body()
    del data["metadata"]
    created = (await client.post(PAYMENTS_URL, json=data, headers=key())).json()
    payment = (await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")).json()
    assert payment["metadata"] == {}


async def test_health_does_not_require_key(client: httpx.AsyncClient) -> None:
    client.headers.pop("X-API-Key")
    response = await client.get("/health")
    assert response.status_code == 200
