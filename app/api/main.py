from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import payments
from app.config import ApiSettings
from app.db import create_engine, create_session_factory
from app.logs import setup_logging


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.session_factory = create_session_factory(engine)
        app.state.api_key = settings.api_key.get_secret_value().encode()
        yield
        await engine.dispose()

    app = FastAPI(title="Payments Service", version="0.1.0", lifespan=lifespan)
    app.include_router(payments.router)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
