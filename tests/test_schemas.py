import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate


@pytest.mark.parametrize("amount", ["0.00", "-0.01"])
def test_payment_create_rejects_non_positive_amount(amount: str) -> None:
    with pytest.raises(ValidationError) as error_info:
        PaymentCreate(
            amount=amount,
            currency="RUB",
            description="Тестовый платёж",
            metadata={},
            webhook_url="https://example.org/webhook",
        )

    errors = error_info.value.errors()

    assert len(errors) == 1
    assert errors[0]["loc"] == ("amount",)
    assert errors[0]["type"] == "greater_than"


@pytest.mark.parametrize("currency", ["BTC", "rub"])
def test_payment_create_rejects_invalid_currency(currency: str) -> None:
    with pytest.raises(ValidationError) as error_info:
        PaymentCreate(
            amount="1500.25",
            currency=currency,
            description="Тестовый платёж",
            metadata={},
            webhook_url="https://example.org/webhook",
        )

    errors = error_info.value.errors()

    assert len(errors) == 1
    assert errors[0]["loc"] == ("currency",)
    assert errors[0]["type"] == "enum"
