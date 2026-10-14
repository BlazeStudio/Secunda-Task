import logging
from dataclasses import dataclass

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.config import ConsumerSettings
from app.consumer.gateway import EmulatedGateway, PaymentGateway
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhooks import WebhookDeliveryError, WebhookSender
from app.db import create_engine, create_session_factory
from app.logs import setup_logging
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology, retry_queue
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_delay: float

    def delay_after(self, attempt: int) -> float:
        return self.base_delay * 2.0 ** (attempt - 1)

    def delays(self) -> list[float]:
        return [self.delay_after(attempt) for attempt in range(1, self.max_attempts)]


def get_attempt(message: RabbitMessage) -> int:
    try:
        return max(int(message.headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


def create_app(
    settings: ConsumerSettings | None = None,
    *,
    gateway: PaymentGateway | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastStream:
    settings = settings or ConsumerSettings()
    setup_logging(settings.log_level)

    broker = RabbitBroker(settings.rabbitmq_url)
    engine = create_engine(settings.database_url)
    http_client = http_client or httpx.AsyncClient(timeout=settings.webhook_timeout)
    gateway = gateway or EmulatedGateway(
        min_delay=settings.gateway_min_delay,
        max_delay=settings.gateway_max_delay,
        success_rate=settings.gateway_success_rate,
    )
    processor = PaymentProcessor(
        create_session_factory(engine), gateway, WebhookSender(http_client)
    )
    retry = RetryPolicy(settings.max_attempts, settings.retry_base_delay)

    async def schedule_retry(message: RabbitMessage, attempt: int) -> None:
        # The copy goes to a delay queue and comes back to payments.new once its
        # TTL expires. The original is acked after the handler returns, i.e. only
        # when the broker has already confirmed the copy.
        await broker.publish(
            message.body,
            queue=retry_queue(retry.delay_after(attempt)),
            headers={ATTEMPT_HEADER: attempt + 1},
            content_type=message.content_type,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            persist=True,
        )

    # REJECT_ON_ERROR: a normal return acks the message, an exception escaping
    # the handler (including a body that fails to decode) rejects it into the DLQ.
    @broker.subscriber(
        PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        channel=Channel(prefetch_count=settings.consumer_prefetch),
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
        attempt = get_attempt(message)
        try:
            await processor.process(event.payment_id)
        except PaymentNotFoundError:
            logger.error("Payment %s does not exist, moving message to DLQ", event.payment_id)
            await message.reject()
        except Exception as exc:
            # an unreachable webhook is routine and the message says it all,
            # anything else is worth a traceback
            exc_info = not isinstance(exc, WebhookDeliveryError)
            if attempt >= retry.max_attempts:
                logger.error(
                    "Payment %s: attempt %d/%d failed (%s), moving message to DLQ",
                    event.payment_id,
                    attempt,
                    retry.max_attempts,
                    exc,
                    exc_info=exc_info,
                )
                await message.reject()
            else:
                logger.warning(
                    "Payment %s: attempt %d/%d failed (%s), retrying in %.1fs",
                    event.payment_id,
                    attempt,
                    retry.max_attempts,
                    exc,
                    retry.delay_after(attempt),
                    exc_info=exc_info,
                )
                await schedule_retry(message, attempt)

    async def declare_queues() -> None:
        await broker.connect()
        await declare_topology(broker, retry.delays())

    async def close_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[declare_queues], after_shutdown=[close_resources])
