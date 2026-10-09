import asyncio

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.enums import PaymentStatus
from app.models import Outbox, Payment
from app.schemas import PaymentCreate
from app.services.payments import create_payment

pytestmark = pytest.mark.integration


@pytest.fixture
def payment_data() -> PaymentCreate:
    return PaymentCreate(
        amount="1500.25",
        currency="RUB",
        description="Тестовый платёж",
        metadata={"order_id": "test-order"},
        webhook_url="https://example.org/webhook",
    )


async def test_create_payment_saves_payment_and_outbox(
    db_sessions: async_sessionmaker[AsyncSession],
    payment_data: PaymentCreate,
) -> None:
    async with db_sessions() as session:
        payment = await create_payment(session, payment_data, "new-payment")
        payment_id = payment.id

    async with db_sessions() as session:
        stored_payment = await session.get(Payment, payment_id)
        events = (await session.scalars(select(Outbox))).all()

        assert stored_payment is not None
        assert stored_payment.amount == payment_data.amount
        assert stored_payment.currency == payment_data.currency
        assert stored_payment.description == payment_data.description
        assert stored_payment.metadata_json == payment_data.metadata
        assert stored_payment.webhook_url == str(payment_data.webhook_url)
        assert stored_payment.idempotency_key == "new-payment"
        assert stored_payment.status == PaymentStatus.PENDING
        assert stored_payment.created_at is not None
        assert stored_payment.processed_at is None
        assert len(events) == 1
        assert events[0].payment_id == payment_id
        assert events[0].payload == {"payment_id": str(payment_id)}
        assert events[0].published_at is None


async def test_concurrent_requests_create_one_payment_and_outbox(
    db_sessions: async_sessionmaker[AsyncSession],
    payment_data: PaymentCreate,
) -> None:
    async def create_once() -> Payment:
        async with db_sessions() as session:
            return await create_payment(session, payment_data, "concurrent-key")

    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(create_once()) for _ in range(5)]

    payments = [task.result() for task in tasks]
    assert len({payment.id for payment in payments}) == 1

    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        event = (await session.scalars(select(Outbox))).one()
        assert event.payment_id == payments[0].id
        assert event.payload == {"payment_id": str(payments[0].id)}


async def test_create_payment_returns_existing_for_same_key(
    db_sessions: async_sessionmaker[AsyncSession],
    payment_data: PaymentCreate,
) -> None:
    async with db_sessions() as session:
        original = await create_payment(session, payment_data, "same-key")

    async with db_sessions() as session:
        repeated = await create_payment(session, payment_data, "same-key")

    assert repeated.id == original.id
    assert repeated.amount == original.amount

    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_outbox_failure_rolls_back_payment(
    db_sessions: async_sessionmaker[AsyncSession],
    payment_data: PaymentCreate,
) -> None:
    async with db_sessions() as session:
        async with session.begin():
            await session.execute(
                text("ALTER TABLE outbox ADD CONSTRAINT test_outbox_reject CHECK (false)")
            )

    async with db_sessions() as session:
        with pytest.raises(IntegrityError):
            await create_payment(session, payment_data, "failed-outbox")

    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
