from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.schemas import PaymentCreate


async def create_payment(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    """Сохранить платёж и событие вместе или вернуть платёж по известному ключу."""
    statement = (
        insert(Payment)
        .values(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_json=data.metadata,
            webhook_url=str(data.webhook_url),
            idempotency_key=idempotency_key,
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )

    async with session.begin():
        result = await session.execute(statement)
        payment = result.scalar_one_or_none()

        if payment is None:
            result = await session.execute(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            return result.scalar_one()

        session.add(
            Outbox(
                payment_id=payment.id,
                payload={"payment_id": str(payment.id)},
            )
        )

    return payment


async def get_payment(
    session: AsyncSession,
    payment_id: UUID,
) -> Payment | None:
    """Получить платёж по ID."""
    return await session.get(Payment, payment_id)
