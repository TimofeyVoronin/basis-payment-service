import asyncio
import random
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import PaymentStatus
from app.models import Payment


async def emulate_payment() -> PaymentStatus:
    """Эмулировать шлюз: задержка 2–5 секунд и 90% успешных платежей."""
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED


async def process_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    """Сохранить результат шлюза, пропуская уже обработанный платёж."""
    statement = select(Payment).where(Payment.id == payment_id).with_for_update()

    async with session.begin():
        payment = await session.scalar(statement)
        if payment is None:
            return None
        if payment.status != PaymentStatus.PENDING:
            return payment

        payment.status = await emulate_payment()
        payment.processed_at = datetime.now(UTC)

    return payment
