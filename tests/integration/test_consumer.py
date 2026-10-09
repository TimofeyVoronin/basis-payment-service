import asyncio
import importlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.enums import Currency, PaymentStatus
from app.models import Payment
from app.services import processing, webhook_delivery

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def consumer_runtime(
    db_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[SimpleNamespace]:
    """Вызвать настоящий handler без подключения к RabbitMQ и реального ожидания."""
    monkeypatch.setenv(
        "DATABASE_URL", db_sessions.kw["bind"].url.render_as_string(hide_password=False)
    )
    monkeypatch.setenv("RABBITMQ_URL", "amqp://payments:payments@localhost:5672/")
    monkeypatch.setenv("API_KEY", "test-key")
    consumer = importlib.import_module("app.consumer")
    original_engine = consumer.engine

    clock = SimpleNamespace(now=datetime(2026, 10, 9, 12, tzinfo=UTC), sleeps=[])

    async def advance_clock(delay: float) -> None:
        clock.sleeps.append(delay)
        clock.now += timedelta(seconds=delay)

    publisher = AsyncMock()
    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr(consumer, "session_factory", db_sessions)
    monkeypatch.setattr(consumer, "sleep", advance_clock)
    monkeypatch.setattr(consumer, "publish_confirmed", publisher)
    monkeypatch.setattr(webhook_delivery, "utc_now", lambda: clock.now)
    monkeypatch.setattr(processing, "emulate_payment", gateway)

    try:
        yield SimpleNamespace(consumer=consumer, clock=clock, publisher=publisher, gateway=gateway)
    finally:
        await original_engine.dispose()


@pytest_asyncio.fixture
async def payment(db_sessions: async_sessionmaker[AsyncSession]) -> Payment:
    async with db_sessions() as session:
        async with session.begin():
            payment = Payment(
                amount=Decimal("1500.25"),
                currency=Currency.RUB,
                description="Проверка доставки webhook",
                metadata_json={"order_id": "consumer-test"},
                idempotency_key="consumer-test",
                webhook_url="https://example.org/webhook",
            )
            session.add(payment)
    return payment


def _message() -> SimpleNamespace:
    return SimpleNamespace(
        message_id=str(uuid4()),
        headers={"source": "integration-test"},
        content_type="application/json",
    )


def _body(payment: Payment) -> bytes:
    return json.dumps({"payment_id": str(payment.id)}).encode()


async def _stored_payment(
    db_sessions: async_sessionmaker[AsyncSession], payment_id: UUID
) -> Payment:
    async with db_sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def test_successful_webhook_is_skipped_on_duplicate_delivery(
    consumer_runtime: SimpleNamespace,
    db_sessions: async_sessionmaker[AsyncSession],
    payment: Payment,
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    body, message = _body(payment), _message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await consumer_runtime.consumer.handle_payment(body, message, client)
        await consumer_runtime.consumer.handle_payment(body, message, client)

    stored = await _stored_payment(db_sessions, payment.id)
    assert len(requests) == 1
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_attempts == 1
    assert stored.webhook_delivered_at == consumer_runtime.clock.now
    assert stored.webhook_next_attempt_at is None
    assert stored.webhook_last_error is None
    consumer_runtime.gateway.assert_awaited_once()
    consumer_runtime.publisher.assert_not_awaited()


async def test_three_http_failures_publish_original_message_to_dlq(
    consumer_runtime: SimpleNamespace,
    db_sessions: async_sessionmaker[AsyncSession],
    payment: Payment,
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    body, message = _body(payment), _message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await consumer_runtime.consumer.handle_payment(body, message, client)

    stored = await _stored_payment(db_sessions, payment.id)
    assert len(requests) == 3
    assert consumer_runtime.clock.sleeps == [2, 4]
    assert stored.webhook_attempts == 3
    assert stored.webhook_delivered_at is None
    assert stored.webhook_last_error == "HTTPStatusError"
    consumer_runtime.gateway.assert_awaited_once()
    consumer_runtime.publisher.assert_awaited_once_with(
        consumer_runtime.consumer.broker,
        consumer_runtime.consumer.dead_letter_queue,
        body,
        message_id=message.message_id,
        content_type=message.content_type,
        headers={**message.headers, "attempts": 3, "error_type": "HTTPStatusError"},
    )


async def test_failed_dlq_publish_redelivery_does_not_make_fourth_http_attempt(
    consumer_runtime: SimpleNamespace,
    db_sessions: async_sessionmaker[AsyncSession],
    payment: Payment,
) -> None:
    requests: list[httpx.Request] = []
    consumer_runtime.publisher.side_effect = [RuntimeError("DLQ unavailable"), None]

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    body, message = _body(payment), _message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(RuntimeError, match="DLQ unavailable"):
            await consumer_runtime.consumer.handle_payment(body, message, client)

        stored = await _stored_payment(db_sessions, payment.id)
        assert stored.webhook_attempts == 3
        assert len(requests) == 3

        await consumer_runtime.consumer.handle_payment(body, message, client)

    assert len(requests) == 3
    assert consumer_runtime.publisher.await_count == 2
    for call in consumer_runtime.publisher.await_args_list:
        assert call.args[2] == body
        assert call.kwargs["message_id"] == message.message_id
        assert call.kwargs["headers"]["attempts"] == 3
    stored = await _stored_payment(db_sessions, payment.id)
    assert stored.webhook_attempts == 3
    assert stored.webhook_delivered_at is None
    consumer_runtime.gateway.assert_awaited_once()


async def test_cancelled_http_preserves_reservation_for_fresh_delivery(
    consumer_runtime: SimpleNamespace,
    db_sessions: async_sessionmaker[AsyncSession],
    payment: Payment,
) -> None:
    requests: list[httpx.Request] = []
    initial_time = consumer_runtime.clock.now

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise asyncio.CancelledError
        return httpx.Response(200)

    body, message = _body(payment), _message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(asyncio.CancelledError):
            await consumer_runtime.consumer.handle_payment(body, message, client)

        stored = await _stored_payment(db_sessions, payment.id)
        assert stored.webhook_attempts == 1
        assert stored.webhook_next_attempt_at == initial_time + timedelta(seconds=7)
        assert stored.webhook_delivered_at is None
        assert stored.webhook_last_error == "DeliveryInterrupted"
        consumer_runtime.publisher.assert_not_awaited()

        await consumer_runtime.consumer.handle_payment(body, message, client)

    stored = await _stored_payment(db_sessions, payment.id)
    assert len(requests) == 2
    assert consumer_runtime.clock.sleeps == [7]
    assert stored.webhook_attempts == 2
    assert stored.webhook_delivered_at == initial_time + timedelta(seconds=7)
    assert stored.webhook_next_attempt_at is None
    assert stored.webhook_last_error is None
    consumer_runtime.gateway.assert_awaited_once()
    consumer_runtime.publisher.assert_not_awaited()


async def test_database_error_during_reservation_prevents_http(
    consumer_runtime: SimpleNamespace,
    db_sessions: async_sessionmaker[AsyncSession],
    payment: Payment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    original_lock = webhook_delivery._lock_payment

    async def fail_lock(session: AsyncSession, payment_id: UUID) -> Payment:
        # Реальная ошибка PostgreSQL внутри транзакции reserve должна вызвать rollback.
        await session.execute(text("SELECT 1 / 0"))
        return await original_lock(session, payment_id)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    monkeypatch.setattr(webhook_delivery, "_lock_payment", fail_lock)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(DBAPIError):
            await consumer_runtime.consumer.handle_payment(_body(payment), _message(), client)

    stored = await _stored_payment(db_sessions, payment.id)
    assert requests == []
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_attempts == 0
    assert stored.webhook_next_attempt_at is None
    assert stored.webhook_delivered_at is None
    consumer_runtime.gateway.assert_awaited_once()
    consumer_runtime.publisher.assert_not_awaited()


@pytest.mark.parametrize(
    ("body", "error_type"),
    [
        (b"{invalid-json", "ValidationError"),
        (b'{"payment_id":"11111111-1111-4111-8111-111111111111"}', "PaymentNotFound"),
    ],
)
async def test_invalid_event_goes_to_dlq_after_three_processing_attempts(
    consumer_runtime: SimpleNamespace,
    body: bytes,
    error_type: str,
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    message = _message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await consumer_runtime.consumer.handle_payment(body, message, client)

    assert requests == []
    assert consumer_runtime.clock.sleeps == [2, 4]
    consumer_runtime.gateway.assert_not_awaited()
    consumer_runtime.publisher.assert_awaited_once_with(
        consumer_runtime.consumer.broker,
        consumer_runtime.consumer.dead_letter_queue,
        body,
        message_id=message.message_id,
        content_type=message.content_type,
        headers={**message.headers, "attempts": 3, "error_type": error_type},
    )
