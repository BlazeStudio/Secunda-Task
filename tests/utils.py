import asyncio
import inspect
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

from app.models import Currency
from app.schemas import PaymentCreate

API_KEY = "test-api-key-0123456789"


def payment_request(**overrides: Any) -> PaymentCreate:
    data: dict[str, Any] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Test order",
        "metadata": {"order_id": 1},
        "webhook_url": "http://webhook.test/payments",
    }
    data.update(overrides)
    return PaymentCreate.model_validate(data)


async def wait_until(
    condition: Callable[[], bool | Awaitable[bool]], timeout: float = 10.0, interval: float = 0.05
) -> None:
    async with asyncio.timeout(timeout):
        while True:
            result = condition()
            if inspect.isawaitable(result):
                result = await result
            if result:
                return
            await asyncio.sleep(interval)
