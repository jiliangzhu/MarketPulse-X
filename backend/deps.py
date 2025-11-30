from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Optional
import secrets

from fastapi import Depends, Header, HTTPException, Request

from .db import Database
from .settings import Settings, get_settings


async def get_db(request: Request) -> Database:
    return request.app.state.db  # type: ignore[attr-defined]


def get_app_settings() -> Settings:
    return get_settings()


def require_admin_token(
    token: Annotated[Optional[str], Header(alias="x-api-key")] = None,
    settings: Settings = Depends(get_app_settings),
):
    expected = settings.admin_api_token
    if not token or not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="invalid token")
    return token


@asynccontextmanager
async def lifespan(app):  # pragma: no cover - exercised via integration tests
    settings = get_settings()
    from .utils.logging import configure_logging

    configure_logging()

    from .db import Database

    db = Database(settings.database_dsn)
    await db.connect()
    app.state.db = db
    app.state.settings = settings

    from .service import bootstrap_services

    background_tasks = await bootstrap_services(app, settings, db)
    try:
        yield
    finally:
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            try:
                await task
            except Exception:  # pragma: no cover - suppression
                pass
        notifier = getattr(app.state, "notifier", None)
        if notifier:
            await notifier.aclose()
        await db.disconnect()
