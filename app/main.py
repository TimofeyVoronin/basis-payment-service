from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.dependencies import require_api_key
from app.api.payments import router as payments_router
from app.db import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Освободить подключения к базе при остановке приложения."""
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="Payment service",
    lifespan=lifespan,
    dependencies=[Depends(require_api_key)],
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.include_router(payments_router, prefix="/api/v1")
