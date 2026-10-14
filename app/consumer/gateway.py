import asyncio
import random
from typing import Protocol

from app.models import Payment, PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class EmulatedGateway:
    """Stand-in for an external acquirer: random latency, random outcome.

    A real integration would pass payment.id to the provider as an idempotency key,
    so that a redelivered message can't charge the customer twice.
    """

    def __init__(
        self,
        *,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
