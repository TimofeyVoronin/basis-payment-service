import json
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest

from app.enums import PaymentStatus
from app.models import Payment
from app.services.webhooks import send_webhook


@pytest.fixture
def payment() -> Payment:
    """Подготовить данные обработанного платежа для отправки webhook."""
    return Payment(
        id=UUID("11111111-1111-4111-8111-111111111111"),
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime(2026, 10, 9, 12, 0, tzinfo=UTC),
        webhook_url="https://example.org/webhook",
    )


@pytest.mark.parametrize("status_code", [200, 204])
@pytest.mark.parametrize("payment_status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_send_webhook_sends_json_object(
    payment: Payment,
    status_code: int,
    payment_status: PaymentStatus,
) -> None:
    payment.status = payment_status
    requests: list[httpx.Request] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code)

    transport = httpx.MockTransport(handle_request)
    async with httpx.AsyncClient(transport=transport) as client:
        await send_webhook(client, payment)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == payment.webhook_url
    assert request.headers["content-type"] == "application/json"
    assert json.loads(request.content) == {
        "payment_id": "11111111-1111-4111-8111-111111111111",
        "status": payment_status.value,
        "processed_at": "2026-10-09T12:00:00Z",
    }


@pytest.mark.parametrize("status_code", [400, 500])
async def test_send_webhook_propagates_http_error(payment: Payment, status_code: int) -> None:
    def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code)

    transport = httpx.MockTransport(handle_request)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError) as error_info:
            await send_webhook(client, payment)

    assert error_info.value.response.status_code == status_code


async def test_send_webhook_propagates_timeout(payment: Payment) -> None:
    def handle_request(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Test timeout", request=request)

    transport = httpx.MockTransport(handle_request)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.ReadTimeout) as error_info:
            await send_webhook(client, payment)

    assert str(error_info.value.request.url) == payment.webhook_url
