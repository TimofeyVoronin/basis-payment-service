import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitQueue
from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Outbox
from app.schemas import PaymentCreate
from app.services.outbox import publish_next_event
from app.services.payments import create_payment

pytestmark = pytest.mark.integration


@pytest.fixture
def broker() -> SimpleNamespace:
    return SimpleNamespace(publish=AsyncMock(return_value=Basic.Ack()))


@pytest.fixture
def queue() -> RabbitQueue:
    return RabbitQueue("payments.new", durable=True)


@pytest_asyncio.fixture
async def event(db_sessions: async_sessionmaker[AsyncSession]) -> Outbox:
    data = PaymentCreate(
        amount="1500.25",
        currency="RUB",
        description="Проверка публикации события",
        metadata={"order_id": "outbox-test"},
        webhook_url="https://example.org/webhook",
    )
    async with db_sessions() as session:
        await create_payment(session, data, "outbox-test")

    async with db_sessions() as session:
        return (await session.scalars(select(Outbox))).one()


async def _stored_event(db_sessions: async_sessionmaker[AsyncSession], event_id: UUID) -> Outbox:
    async with db_sessions() as session:
        event = await session.get(Outbox, event_id)
        assert event is not None
        return event


async def test_empty_outbox_does_not_publish(
    db_sessions: async_sessionmaker[AsyncSession],
    broker: SimpleNamespace,
    queue: RabbitQueue,
) -> None:
    async with db_sessions() as session:
        assert await publish_next_event(session, broker, queue) is False

    broker.publish.assert_not_awaited()


async def test_confirmed_event_is_saved_and_skipped_on_next_call(
    db_sessions: async_sessionmaker[AsyncSession],
    event: Outbox,
    broker: SimpleNamespace,
    queue: RabbitQueue,
) -> None:
    async with db_sessions() as session:
        assert await publish_next_event(session, broker, queue) is True

    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is not None
    assert stored.published_at.tzinfo is not None

    async with db_sessions() as session:
        assert await publish_next_event(session, broker, queue) is False

    broker.publish.assert_awaited_once()
    publication = broker.publish.await_args
    assert publication.args == (event.payload,)
    assert publication.kwargs["message_id"] == str(event.id)
    assert publication.kwargs["persist"] is True
    assert publication.kwargs["mandatory"] is True


@pytest.mark.parametrize(
    ("confirmation", "error_type"),
    [
        (Basic.Nack(), RuntimeError),
        (TimeoutError("Publisher confirmation timed out"), TimeoutError),
    ],
    ids=["nack", "timeout"],
)
async def test_unconfirmed_event_can_be_published_again_with_same_message_id(
    db_sessions: async_sessionmaker[AsyncSession],
    event: Outbox,
    broker: SimpleNamespace,
    queue: RabbitQueue,
    confirmation: Basic.Nack | TimeoutError,
    error_type: type[Exception],
) -> None:
    broker.publish.side_effect = [confirmation, Basic.Ack()]

    async with db_sessions() as session:
        with pytest.raises(error_type):
            await publish_next_event(session, broker, queue)

    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is None

    async with db_sessions() as session:
        assert await publish_next_event(session, broker, queue) is True

    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is not None
    assert broker.publish.await_count == 2
    assert [call.kwargs["message_id"] for call in broker.publish.await_args_list] == [
        str(event.id),
        str(event.id),
    ]


async def test_database_failure_after_ack_leaves_event_for_republication(
    db_sessions: async_sessionmaker[AsyncSession],
    event: Outbox,
    broker: SimpleNamespace,
    queue: RabbitQueue,
) -> None:
    async with db_sessions() as session:

        async def acknowledge_before_constraint_failure(
            *args: object, **kwargs: object
        ) -> Basic.Ack:
            # ACK получен, но последующий commit нарушит настоящий внешний ключ.
            locked_event = await session.get(Outbox, event.id)
            assert locked_event is not None
            locked_event.payment_id = uuid4()
            return Basic.Ack()

        broker.publish.side_effect = acknowledge_before_constraint_failure
        with pytest.raises(IntegrityError):
            await publish_next_event(session, broker, queue)

    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is None
    assert stored.payment_id == event.payment_id

    broker.publish.side_effect = None
    async with db_sessions() as session:
        assert await publish_next_event(session, broker, queue) is True

    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is not None
    assert broker.publish.await_count == 2
    assert [call.kwargs["message_id"] for call in broker.publish.await_args_list] == [
        str(event.id),
        str(event.id),
    ]


async def test_concurrent_relays_skip_event_locked_during_publication(
    db_sessions: async_sessionmaker[AsyncSession],
    event: Outbox,
    broker: SimpleNamespace,
    queue: RabbitQueue,
) -> None:
    publication_started = asyncio.Event()
    release_publication = asyncio.Event()

    async def acknowledge_after_release(*args: object, **kwargs: object) -> Basic.Ack:
        publication_started.set()
        await release_publication.wait()
        return Basic.Ack()

    async def relay_once() -> bool:
        async with db_sessions() as session:
            return await publish_next_event(session, broker, queue)

    broker.publish.side_effect = acknowledge_after_release
    async with asyncio.TaskGroup() as group:
        first = group.create_task(relay_once())
        try:
            async with asyncio.timeout(5):
                await publication_started.wait()
                second = group.create_task(relay_once())
                # Второй relay должен завершиться, пока первый ещё держит блокировку.
                assert await second is False
        finally:
            release_publication.set()

    assert first.result() is True
    broker.publish.assert_awaited_once()
    stored = await _stored_event(db_sessions, event.id)
    assert stored.published_at is not None
