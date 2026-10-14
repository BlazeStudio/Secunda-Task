from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        payment: Payment | None = await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == key)
        )
        return payment

    async def insert_if_absent(self, **values: Any) -> Payment | None:
        """Insert a payment unless one with the same idempotency key already exists.

        Returns None on conflict. Concurrent inserts with the same key are serialized
        by the unique index, so exactly one of them gets the row back.
        """
        stmt = (
            insert(Payment)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment: Payment | None = await self._session.scalar(stmt)
        return payment

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        """Move a pending payment to a final status. Returns None if it is not pending."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
        )
        payment: Payment | None = await self._session.scalar(stmt)
        return payment


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event_type: str, payload: dict[str, Any]) -> None:
        self._session.add(OutboxEvent(event_type=event_type, payload=payload))

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        # SKIP LOCKED lets several relay instances work in parallel without
        # stepping on each other's batches
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def mark_published(self, event_ids: Sequence[UUID]) -> None:
        if not event_ids:
            return
        await self._session.execute(
            update(OutboxEvent).where(OutboxEvent.id.in_(event_ids)).values(published_at=func.now())
        )
