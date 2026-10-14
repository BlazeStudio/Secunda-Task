import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.consumer.webhooks import WebhookSender
from app.models import Payment, PaymentStatus
from app.repositories import PaymentRepository

logger = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    pass


class PaymentProcessor:
    """Charges a payment and notifies the client. Safe to run more than once.

    A message can arrive again (redelivery, outbox duplicate, retry after a failed
    webhook), so the gateway is only called while the payment is still pending,
    and the status is switched with a conditional UPDATE.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: UUID) -> None:
        payment = await self._load(payment_id)

        if payment.status is PaymentStatus.PENDING:
            # no transaction is held open while waiting for the gateway
            status = await self._gateway.charge(payment)
            async with self._session_factory.begin() as session:
                completed = await PaymentRepository(session).complete(payment_id, status)
            if completed is None:
                # another worker got there first, report whatever it decided
                payment = await self._load(payment_id)
            else:
                payment = completed
                logger.info("Payment %s %s", payment_id, payment.status)
        else:
            logger.info("Payment %s is already %s, skipping charge", payment_id, payment.status)

        await self._webhooks.send(payment)
        logger.info("Webhook for payment %s delivered", payment_id)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
