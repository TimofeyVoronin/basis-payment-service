from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from app.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Проверить тело запроса на создание платежа."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: AnyHttpUrl


class PaymentCreated(BaseModel):
    """Сформировать ответ после принятия платежа."""

    model_config = ConfigDict(from_attributes=True, validate_by_name=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(PaymentCreated):
    """Сформировать ответ при запросе информации о платеже."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_json")
    idempotency_key: str
    webhook_url: AnyHttpUrl
    processed_at: datetime | None


class PaymentWebhook(BaseModel):
    """Сформировать уведомление о результате обработки платежа."""

    model_config = ConfigDict(from_attributes=True, validate_by_name=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    processed_at: datetime


class PaymentEvent(BaseModel):
    """Проверить событие создания платежа из очереди."""

    payment_id: UUID
