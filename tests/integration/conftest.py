import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base


@pytest_asyncio.fixture
async def db_sessions() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Создать отдельную БД для теста и удалить её после закрытия сессий."""
    server_url = make_url(
        os.environ.get(
            "TEST_DATABASE_URL",
            "postgresql+asyncpg://payments:payments@127.0.0.1:55433/postgres",
        )
    )
    database_name = f"test_payments_{uuid4().hex}"
    admin_engine = create_async_engine(server_url, isolation_level="AUTOCOMMIT")

    try:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))

        try:
            test_engine = create_async_engine(server_url.set(database=database_name))
            try:
                async with test_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)

                yield async_sessionmaker(test_engine, expire_on_commit=False)
            finally:
                await test_engine.dispose()
        finally:
            async with admin_engine.connect() as connection:
                await connection.execute(text(f'DROP DATABASE "{database_name}"'))
    finally:
        await admin_engine.dispose()
