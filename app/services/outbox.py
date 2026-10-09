from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker, RabbitQueue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox
from app.services.messaging import publish_confirmed


async def publish_next_event(
    session: AsyncSession,
    broker: RabbitBroker,
    queue: RabbitQueue,
) -> bool:
    """Опубликовать доступное событие и сохранить отметку после подтверждения."""
    statement = (
        select(Outbox)
        .where(Outbox.published_at.is_(None))
        .order_by(Outbox.created_at, Outbox.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )

    async with session.begin():
        event = await session.scalar(statement)
        if event is None:
            return False

        await publish_confirmed(broker, queue, event.payload, message_id=str(event.id))

        event.published_at = datetime.now(UTC)

    return True
