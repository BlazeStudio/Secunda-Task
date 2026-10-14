"""RabbitMQ topology shared by the outbox relay and the consumer (diagram in README)."""

from collections.abc import Iterable

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENT_CREATED = "payment.created"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENT_CREATED,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": "payments.new",
    },
)
PAYMENTS_DLQ = RabbitQueue("payments.new.dlq", durable=True, routing_key="payments.new")


def retry_queue(delay: float) -> RabbitQueue:
    """Parking queue that returns messages to payments.new after `delay` seconds.

    Every message in a given queue has the same TTL, so there is no head-of-line
    blocking. The delay is part of the name: changing the retry settings creates new
    queues instead of failing on redeclaration with different arguments.
    """
    delay_ms = round(delay * 1000)
    return RabbitQueue(
        f"payments.new.retry.{delay_ms}ms",
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": PAYMENTS_QUEUE.name,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_delays: Iterable[float] = ()) -> None:
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dead_letter_exchange = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.bind(payments_exchange, routing_key=PAYMENTS_QUEUE.routing_key)

    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    await dlq.bind(dead_letter_exchange, routing_key=PAYMENTS_DLQ.routing_key)

    for delay in retry_delays:
        await broker.declare_queue(retry_queue(delay))
