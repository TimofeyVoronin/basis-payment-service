import asyncio
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitQueue
from pamqp.commands import Basic


async def publish_confirmed(
    broker: RabbitBroker,
    queue: RabbitQueue,
    payload: dict[str, Any] | bytes,
    *,
    message_id: str,
    headers: dict[str, Any] | None = None,
    content_type: str | None = None,
) -> None:
    """Отправить постоянное сообщение и дождаться подтверждения RabbitMQ."""
    async with asyncio.timeout(10):
        confirmation = await broker.publish(
            payload,
            queue=queue,
            persist=True,
            mandatory=True,
            message_id=message_id,
            headers=headers,
            content_type=content_type,
            timeout=10,
        )

    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not confirm publication")
