from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request

from backend.db import Database
from backend.deps import get_db
from backend.schemas import HealthResponse

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/healthz", response_model=HealthResponse)
async def health(request: Request, db: Database = Depends(get_db)) -> HealthResponse:
    await db.fetchrow("SELECT 1")
    rules_heartbeat = "stale"
    last_signal = await db.fetchrow("SELECT created_at FROM signal ORDER BY created_at DESC LIMIT 1")
    if last_signal:
        delta = (datetime.now(timezone.utc) - last_signal["created_at"]).total_seconds()
        rules_heartbeat = "ok" if delta < 30 else "lagging"
    return HealthResponse(
        status="ok",
        time=datetime.now(timezone.utc),
        db="ok",
        rules_heartbeat=rules_heartbeat,
    )
