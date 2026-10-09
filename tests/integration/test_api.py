from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Outbox, Payment

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def api_client(
    db_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[httpx.AsyncClient]:
    """Направить запросы в FastAPI с настоящей тестовой БД и тестовым API-ключом."""
    monkeypatch.setitem(Settings.model_config, "env_file", None)
    database_url = db_sessions.kw["bind"].url.render_as_string(hide_password=False)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("RABBITMQ_URL", "amqp://unused:unused@localhost:5672/")
    monkeypatch.setenv("API_KEY", "test-api-key")

    # Настройки заданы до импорта приложения: наличие .env не требуется.
    from app.api import dependencies
    from app.db import engine, get_session
    from app.main import app

    monkeypatch.setattr(
        dependencies,
        "settings",
        Settings(
            _env_file=None,
            database_url=database_url,
            rabbitmq_url="amqp://unused:unused@localhost:5672/",
            api_key="test-api-key",
        ),
    )

    async def test_session() -> AsyncIterator[AsyncSession]:
        async with db_sessions() as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key"},
        ) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_session, None)
        await engine.dispose()


@pytest.fixture
def payment_body() -> dict[str, Any]:
    return {
        "amount": "1500.25",
        "currency": "RUB",
        "description": "Тест API",
        "metadata": {"order_id": "api-test"},
        "webhook_url": "https://example.org/webhook",
    }


async def test_post_and_get_return_payment_contract(
    api_client: httpx.AsyncClient,
    payment_body: dict[str, Any],
) -> None:
    response = await api_client.post(
        "/api/v1/payments", headers={"Idempotency-Key": "api-create"}, json=payment_body
    )
    assert response.status_code == 202
    created = response.json()
    assert set(created) == {"payment_id", "status", "created_at"}
    UUID(created["payment_id"])
    assert created["status"] == "pending"
    assert datetime.fromisoformat(created["created_at"]).utcoffset() is not None

    response = await api_client.get(f"/api/v1/payments/{created['payment_id']}")
    assert response.status_code == 200
    assert response.json() == {
        **created,
        **payment_body,
        "idempotency_key": "api-create",
        "processed_at": None,
    }


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_payment_routes_reject_missing_or_wrong_api_key(
    api_client: httpx.AsyncClient,
    db_sessions: async_sessionmaker[AsyncSession],
    payment_body: dict[str, Any],
    method: str,
    api_key: str | None,
) -> None:
    api_client.headers.pop("X-API-Key")
    if api_key is not None:
        api_client.headers["X-API-Key"] = api_key

    if method == "POST":
        response = await api_client.post(
            "/api/v1/payments", headers={"Idempotency-Key": "unauthorized"}, json=payment_body
        )
    else:
        response = await api_client.get("/api/v1/payments/11111111-1111-4111-8111-111111111111")

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "APIKey"
    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


@pytest.mark.parametrize("idempotency_key", [None, "", "x" * 256])
async def test_post_requires_valid_idempotency_key(
    api_client: httpx.AsyncClient,
    db_sessions: async_sessionmaker[AsyncSession],
    payment_body: dict[str, Any],
    idempotency_key: str | None,
) -> None:
    headers = {} if idempotency_key is None else {"Idempotency-Key": idempotency_key}
    response = await api_client.post("/api/v1/payments", headers=headers, json=payment_body)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]
    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_repeated_post_keeps_original_payment_and_single_event(
    api_client: httpx.AsyncClient,
    db_sessions: async_sessionmaker[AsyncSession],
    payment_body: dict[str, Any],
) -> None:
    headers = {"Idempotency-Key": "api-repeat"}
    original = await api_client.post("/api/v1/payments", headers=headers, json=payment_body)
    assert original.status_code == 202

    repeated = await api_client.post(
        "/api/v1/payments",
        headers=headers,
        json={**payment_body, "amount": "999.99", "metadata": {"order_id": "changed"}},
    )
    assert repeated.status_code == 202
    assert repeated.json() == original.json()

    response = await api_client.get(f"/api/v1/payments/{original.json()['payment_id']}")
    assert response.status_code == 200
    assert response.json()["amount"] == payment_body["amount"]
    assert response.json()["metadata"] == payment_body["metadata"]
    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0.00"),
        ("currency", "BTC"),
        ("webhook_url", "ftp://example.org/webhook"),
        ("description", "invalid\x00text"),
        ("metadata", {"nested": ["invalid\x00text"]}),
        ("metadata", {"invalid\x00key": "value"}),
    ],
)
async def test_invalid_body_returns_422_without_creating_payment(
    api_client: httpx.AsyncClient,
    db_sessions: async_sessionmaker[AsyncSession],
    payment_body: dict[str, Any],
    field: str,
    value: Any,
) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "invalid-body"},
        json={**payment_body, field: value},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]
    async with db_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_get_unknown_payment_returns_404(
    api_client: httpx.AsyncClient,
) -> None:
    response = await api_client.get("/api/v1/payments/11111111-1111-4111-8111-111111111111")

    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}
