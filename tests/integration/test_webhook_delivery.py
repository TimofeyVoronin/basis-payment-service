import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.enums import Currency, PaymentStatus
from app.models import Payment
from app.services import webhook_delivery

pytestmark = pytest.mark.integration


class FrozenClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FrozenClock:
    frozen = FrozenClock()
    monkeypatch.setattr(webhook_delivery, "utc_now", frozen.now)
    return frozen


async def save_payment(
    db_sessions: async_sessionmaker[AsyncSession],
    *,
    attempts: int = 0,
    next_attempt_at: datetime | None = None,
    last_error: str | None = None,
) -> UUID:
    payment_id = uuid4()
    payment = Payment(
        id=payment_id,
        amount=Decimal("1500.25"),
        currency=Currency.RUB,
        description="Тест доставки webhook",
        metadata_json={"order_id": str(payment_id)},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key=str(payment_id),
        webhook_url="https://example.org/webhook",
        processed_at=datetime(2026, 10, 9, 12, 0, tzinfo=UTC),
        webhook_attempts=attempts,
        webhook_next_attempt_at=next_attempt_at,
        webhook_delivered_at=None,
        webhook_last_error=last_error,
    )
    async with db_sessions() as session:
        session.add(payment)
        await session.commit()
    return payment_id


async def stored_payment(
    db_sessions: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> Payment:
    async with db_sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def test_three_attempt_budget_survives_new_sessions(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(db_sessions)

    for attempt in range(1, 4):
        async with db_sessions() as session:
            payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
        assert reserved is True
        assert payment.webhook_attempts == attempt

        async with db_sessions() as session:
            await webhook_delivery.mark_webhook_failed(
                session, payment_id, attempt, "HTTPStatusError"
            )
        if attempt < 3:
            clock.advance(2**attempt)

    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)

    assert reserved is False
    assert payment.webhook_attempts == 3
    persisted = await stored_payment(db_sessions, payment_id)
    assert persisted.webhook_attempts == 3
    assert persisted.webhook_last_error == "HTTPStatusError"
    assert persisted.webhook_delivered_at is None


async def test_backoff_preserves_remaining_two_and_four_second_delays(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(db_sessions)
    async with db_sessions() as session:
        _, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True
    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_failed(session, payment_id, 1, "ReadTimeout")

    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_next_attempt_at == clock.now() + timedelta(seconds=2)
    assert webhook_delivery.remaining_delay(payment) == 2

    clock.advance(1)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is False
    assert payment.webhook_attempts == 1
    assert webhook_delivery.remaining_delay(payment) == 1

    clock.advance(1)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True
    assert payment.webhook_attempts == 2
    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_failed(session, payment_id, 2, "ReadTimeout")

    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_next_attempt_at == clock.now() + timedelta(seconds=4)
    assert webhook_delivery.remaining_delay(payment) == 4

    clock.advance(3)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is False
    assert payment.webhook_attempts == 2
    assert webhook_delivery.remaining_delay(payment) == 1

    clock.advance(1)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True
    assert payment.webhook_attempts == 3


async def test_interrupted_reservation_keeps_attempt_and_deadline(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(db_sessions)
    async with db_sessions() as session:
        _, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True

    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_attempts == 1
    assert payment.webhook_last_error == "DeliveryInterrupted"
    assert payment.webhook_next_attempt_at == clock.now() + timedelta(
        seconds=webhook_delivery.WEBHOOK_TIMEOUT + 2
    )

    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is False
    assert payment.webhook_attempts == 1

    clock.advance(webhook_delivery.WEBHOOK_TIMEOUT + 2)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True
    assert payment.webhook_attempts == 2
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_last_error == "DeliveryInterrupted"


async def test_delivered_marker_suppresses_duplicates_and_keeps_timestamp(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(db_sessions)
    async with db_sessions() as session:
        _, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is True
    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_delivered(session, payment_id)

    delivered_at = clock.now()
    clock.advance(60)
    async with db_sessions() as session:
        payment, reserved = await webhook_delivery.reserve_webhook_attempt(session, payment_id)
    assert reserved is False
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at == delivered_at
    assert payment.webhook_next_attempt_at is None
    assert payment.webhook_last_error is None
    assert webhook_delivery.remaining_delay(payment) == 0

    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_delivered(session, payment_id)
    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_delivered_at == delivered_at
    assert payment.webhook_delivered_at.utcoffset() == timedelta(0)


async def test_concurrent_calls_reserve_last_attempt_only_once(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(
        db_sessions, attempts=2, next_attempt_at=clock.now(), last_error="ReadTimeout"
    )

    async def reserve_once() -> tuple[Payment, bool]:
        async with db_sessions() as session:
            return await webhook_delivery.reserve_webhook_attempt(session, payment_id)

    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(reserve_once()) for _ in range(2)]

    results = [task.result() for task in tasks]
    assert sorted(reserved for _, reserved in results) == [False, True]
    assert all(payment.webhook_attempts == 3 for payment, _ in results)
    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_attempts == 3
    assert payment.webhook_next_attempt_at == clock.now() + timedelta(
        seconds=webhook_delivery.WEBHOOK_TIMEOUT
    )


async def test_failed_reservation_transaction_rolls_back_attempt(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    payment_id = await save_payment(db_sessions)
    async with db_sessions() as session:
        async with session.begin():
            await session.execute(
                text(
                    "ALTER TABLE payments ADD CONSTRAINT test_webhook_reservation_failure "
                    "CHECK (webhook_attempts = 0)"
                )
            )

    try:
        async with db_sessions() as session:
            with pytest.raises(IntegrityError):
                await webhook_delivery.reserve_webhook_attempt(session, payment_id)

        payment = await stored_payment(db_sessions, payment_id)
        assert payment.webhook_attempts == 0
        assert payment.webhook_next_attempt_at is None
        assert payment.webhook_last_error is None
        assert payment.webhook_delivered_at is None
    finally:
        async with db_sessions() as session:
            async with session.begin():
                await session.execute(
                    text("ALTER TABLE payments DROP CONSTRAINT test_webhook_reservation_failure")
                )


async def test_stale_failure_cannot_overwrite_new_attempt_or_success(
    db_sessions: async_sessionmaker[AsyncSession],
    clock: FrozenClock,
) -> None:
    deadline = clock.now() + timedelta(seconds=4)
    payment_id = await save_payment(
        db_sessions, attempts=2, next_attempt_at=deadline, last_error="ReadTimeout"
    )
    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_failed(session, payment_id, 1, "HTTPStatusError")

    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_attempts == 2
    assert payment.webhook_last_error == "ReadTimeout"
    assert payment.webhook_next_attempt_at == deadline

    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_delivered(session, payment_id)
    delivered_at = clock.now()
    clock.advance(1)
    async with db_sessions() as session:
        await webhook_delivery.mark_webhook_failed(session, payment_id, 2, "HTTPStatusError")

    payment = await stored_payment(db_sessions, payment_id)
    assert payment.webhook_delivered_at == delivered_at
    assert payment.webhook_next_attempt_at is None
    assert payment.webhook_last_error is None
    assert payment.webhook_attempts == 2
