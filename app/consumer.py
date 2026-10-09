import asyncio
import logging
from asyncio import sleep
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated

import httpx
from faststream import Context, ContextRepo, FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.broker import broker, dead_letter_queue, payments_queue
from app.db import engine, session_factory
from app.outbox import run_outbox
from app.schemas import PaymentEvent
from app.services.messaging import publish_confirmed
from app.services.processing import process_payment
from app.services.webhook_delivery import (
    MAX_ATTEMPTS,
    WEBHOOK_TIMEOUT,
    mark_webhook_delivered,
    mark_webhook_failed,
    remaining_delay,
    reserve_webhook_attempt,
)
from app.services.webhooks import send_webhook

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(context: ContextRepo) -> AsyncIterator[None]:
    """Открыть общий HTTP-клиент и освободить ресурсы после остановки."""
    try:
        async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
            context.set_global("http_client", client)
            try:
                yield
            finally:
                try:
                    await stop_outbox(context)
                finally:
                    await broker.stop()
    finally:
        await engine.dispose()


app = FastStream(broker, lifespan=lifespan)


@app.on_startup
async def declare_queues() -> None:
    """Подготовить очереди до начала приёма сообщений."""
    await broker.connect()
    await broker.declare_queue(payments_queue)
    await broker.declare_queue(dead_letter_queue)


@app.after_startup
async def start_outbox(context: ContextRepo) -> None:
    """Запустить relay в том же процессе, что и consumer."""
    context.set_global("outbox_task", asyncio.create_task(run_outbox()))


@app.on_shutdown
async def stop_outbox(context: ContextRepo) -> None:
    """Остановить relay перед закрытием соединения с RabbitMQ."""
    task = context.get("outbox_task")
    if task is not None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def decode_body(message: RabbitMessage) -> bytes:
    """Передать JSON в обработчик, чтобы ошибки формата тоже прошли через DLQ."""
    return message.body


@broker.subscriber(
    payments_queue,
    ack_policy=AckPolicy.NACK_ON_ERROR,
    decoder=decode_body,
)
async def handle_payment(
    body: bytes,
    message: RabbitMessage,
    client: Annotated[httpx.AsyncClient, Context("http_client")],
) -> None:
    """Подтвердить сообщение только после доставки webhook или публикации в DLQ."""
    try:
        await _handle_payment(body, message, client)
    except Exception:
        logger.exception("Сообщение %s будет доставлено повторно", message.message_id)
        await sleep(1)
        raise


async def send_to_dlq(
    body: bytes, message: RabbitMessage, *, attempts: int, error_type: str
) -> None:
    """Дождаться подтверждения DLQ перед ACK исходного сообщения."""
    await publish_confirmed(
        broker,
        dead_letter_queue,
        body,
        message_id=message.message_id,
        content_type=message.content_type,
        headers={
            **message.headers,
            "attempts": attempts,
            "error_type": error_type,
        },
    )
    logger.error("Сообщение %s отправлено в payments.dlq", message.message_id)


async def _handle_payment(body: bytes, message: RabbitMessage, client: httpx.AsyncClient) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            event = PaymentEvent.model_validate_json(body)
        except ValidationError:
            error_type = "ValidationError"
        else:
            async with session_factory() as session:
                payment = await process_payment(session, event.payment_id)
            if payment is not None:
                break
            error_type = "PaymentNotFound"

        if attempt == MAX_ATTEMPTS:
            await send_to_dlq(body, message, attempts=attempt, error_type=error_type)
            return
        await sleep(2**attempt)

    while True:
        async with session_factory() as session:
            payment, reserved = await reserve_webhook_attempt(session, event.payment_id)
        if payment.webhook_delivered_at is not None:
            return
        if not reserved:
            delay = remaining_delay(payment)
            if delay > 0:
                await sleep(delay)
                continue
            if payment.webhook_attempts >= MAX_ATTEMPTS:
                await send_to_dlq(
                    body,
                    message,
                    attempts=payment.webhook_attempts,
                    error_type=payment.webhook_last_error or "DeliveryInterrupted",
                )
                return
            continue

        attempt = payment.webhook_attempts
        try:
            async with asyncio.timeout(WEBHOOK_TIMEOUT):
                await send_webhook(client, payment)
        except Exception as error:
            async with session_factory() as session:
                await mark_webhook_failed(session, payment.id, attempt, type(error).__name__)
            logger.warning(
                "Сообщение %s: попытка %s/%s, ошибка %s",
                message.message_id,
                attempt,
                MAX_ATTEMPTS,
                type(error).__name__,
            )
            continue

        async with session_factory() as session:
            await mark_webhook_delivered(session, payment.id)
        logger.info("Уведомление для платежа %s доставлено", payment.id)
        return
