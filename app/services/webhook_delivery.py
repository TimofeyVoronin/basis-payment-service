from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment

MAX_ATTEMPTS = 3
WEBHOOK_TIMEOUT = 5


def utc_now() -> datetime:
    return datetime.now(UTC)


def remaining_delay(payment: Payment) -> float:
    """Вычислить оставшееся ожидание без удержания блокировки БД."""
    if payment.webhook_next_attempt_at is None:
        return 0
    return max(0, (payment.webhook_next_attempt_at - utc_now()).total_seconds())


async def _lock_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    statement = select(Payment).where(Payment.id == payment_id).with_for_update()
    payment = await session.scalar(statement)
    if payment is None:
        raise LookupError("Payment not found")
    return payment


async def reserve_webhook_attempt(session: AsyncSession, payment_id: UUID) -> tuple[Payment, bool]:
    """Зафиксировать попытку до HTTP, сохранив общий лимит при перезапуске."""
    async with session.begin():
        payment = await _lock_payment(session, payment_id)
        if (
            payment.webhook_delivered_at is not None
            or remaining_delay(payment) > 0
            or payment.webhook_attempts >= MAX_ATTEMPTS
        ):
            return payment, False

        payment.webhook_attempts += 1
        attempt = payment.webhook_attempts
        backoff = 2**attempt if attempt < MAX_ATTEMPTS else 0
        # При остановке во время HTTP новая доставка дождётся таймаута и backoff.
        payment.webhook_next_attempt_at = utc_now() + timedelta(seconds=WEBHOOK_TIMEOUT + backoff)
        payment.webhook_last_error = "DeliveryInterrupted"

    return payment, True


async def mark_webhook_failed(
    session: AsyncSession, payment_id: UUID, attempt: int, error_type: str
) -> None:
    """Сохранить ошибку текущей попытки и задержку перед следующей."""
    async with session.begin():
        payment = await _lock_payment(session, payment_id)
        if payment.webhook_attempts != attempt or payment.webhook_delivered_at is not None:
            return
        payment.webhook_last_error = error_type
        backoff = 2**attempt if attempt < MAX_ATTEMPTS else 0
        payment.webhook_next_attempt_at = utc_now() + timedelta(seconds=backoff)


async def mark_webhook_delivered(session: AsyncSession, payment_id: UUID) -> None:
    """Запомнить успешную доставку, чтобы не повторять её при дубликате события."""
    async with session.begin():
        payment = await _lock_payment(session, payment_id)
        if payment.webhook_delivered_at is None:
            payment.webhook_delivered_at = utc_now()
        payment.webhook_next_attempt_at = None
        payment.webhook_last_error = None
