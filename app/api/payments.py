from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import PaymentCreate, PaymentCreated, PaymentRead
from app.services.payments import create_payment, get_payment

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreated,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_payment(
    data: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255),
    ],
) -> PaymentCreated:
    """Принять платёж для дальнейшей асинхронной обработки."""
    payment = await create_payment(session, data, idempotency_key)
    return PaymentCreated.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentRead)
async def read_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentRead:
    """Вернуть информацию о платеже по ID."""
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return PaymentRead.model_validate(payment)
