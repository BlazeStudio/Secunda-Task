from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import PAYMENT_CREATED
from app.models import Payment
from app.repositories import OutboxRepository, PaymentRepository
from app.schemas import PaymentCreate, PaymentEvent


class IdempotencyKeyReusedError(Exception):
    """The key was already used for a request with a different body."""


@dataclass(frozen=True, slots=True)
class CreateResult:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreateResult:
        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
            )
            if payment is not None:
                # same transaction as the payment itself, so the event can't get lost
                event = PaymentEvent(payment_id=payment.id)
                self._outbox.add(PAYMENT_CREATED, event.model_dump(mode="json"))
                return CreateResult(payment, created=True)

            existing = await self._payments.get_by_idempotency_key(idempotency_key)

        if existing is None:
            # payments are never deleted, so the conflicting row must be there
            raise RuntimeError(f"payment with idempotency key {idempotency_key!r} not found")
        if not _same_request(existing, data):
            raise IdempotencyKeyReusedError(idempotency_key)
        return CreateResult(existing, created=False)

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._payments.get(payment_id)


def _same_request(payment: Payment, data: PaymentCreate) -> bool:
    return (
        payment.amount == data.amount
        and payment.currency == data.currency
        and payment.description == data.description
        and payment.metadata_ == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )
