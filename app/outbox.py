import asyncio
import logging

from app.broker import broker, payments_queue
from app.db import engine, session_factory
from app.services.outbox import publish_next_event

logger = logging.getLogger(__name__)


async def run_outbox() -> None:
    """Публиковать события outbox, повторяя попытку после ошибки."""
    while True:
        try:
            async with session_factory() as session:
                published = await publish_next_event(session, broker, payments_queue)
        except Exception as error:
            logger.warning("Ошибка публикации: %s; повтор через секунду", type(error).__name__)
            await asyncio.sleep(1)
        else:
            if not published:
                await asyncio.sleep(1)


async def main() -> None:
    """Подключиться к RabbitMQ и запустить публикацию outbox."""
    try:
        async with broker:
            await broker.declare_queue(payments_queue)
            await run_outbox()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
