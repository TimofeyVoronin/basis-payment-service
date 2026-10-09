import httpx

from app.models import Payment
from app.schemas import PaymentWebhook


async def send_webhook(
    client: httpx.AsyncClient,
    payment: Payment,
) -> None:
    """Отправить клиенту результат обработки платежа."""
    payload = PaymentWebhook.model_validate(payment).model_dump(mode="json")

    response = await client.post(
        payment.webhook_url,
        json=payload,
    )
    response.raise_for_status()
