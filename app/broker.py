from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from app.config import Settings

broker = RabbitBroker(
    Settings().rabbitmq_url,
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    graceful_timeout=60,
)
payments_queue = RabbitQueue("payments.new", durable=True)
dead_letter_queue = RabbitQueue("payments.dlq", durable=True)
