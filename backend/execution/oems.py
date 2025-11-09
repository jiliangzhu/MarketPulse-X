from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from backend.db import Database
from backend.metrics import order_intent_counter
from backend.repo import execution_repo


async def bootstrap_policy(db: Database, settings) -> int:
    policy = await execution_repo.get_policy(db)
    if policy:
        return int(policy["policy_id"])
    policy_id = await execution_repo.upsert_default_policy(
        db,
        name="default-phase2",
        mode=settings.exec_mode,
        max_order=settings.exec_max_notional_per_order,
        max_concurrent=settings.exec_max_concurrent_orders,
        max_daily=settings.exec_max_daily_notional,
        slippage_bps=settings.exec_slippage_bps,
    )
    return policy_id


async def create_suggested_intent(db: Database, payload: dict[str, Any]) -> dict[str, Any]:
    order = await execution_repo.create_intent(db, payload)
    order_intent_counter.labels(status=payload.get("status", "suggested")).inc()
    return order


async def mark_status(db: Database, intent_id: int, status: str, detail_json: dict[str, Any] | None = None) -> None:
    await execution_repo.update_intent_status(db, intent_id, status, detail_json)
    order_intent_counter.labels(status=status).inc()


async def list_intents(db: Database, status: str | None = None, limit: int = 50):
    return await execution_repo.fetch_intents(db, status=status, limit=limit)
