from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
import json

from backend.db import Database


async def upsert_default_policy(db: Database, *, name: str, mode: str, max_order: float, max_concurrent: int,
                                max_daily: float, slippage_bps: int) -> int:
    query = """
        INSERT INTO execution_policy (name, mode, max_notional_per_order, max_concurrent_orders, max_daily_notional, slippage_bps)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (name)
        DO UPDATE SET mode = EXCLUDED.mode,
                      max_notional_per_order = EXCLUDED.max_notional_per_order,
                      max_concurrent_orders = EXCLUDED.max_concurrent_orders,
                      max_daily_notional = EXCLUDED.max_daily_notional,
                      slippage_bps = EXCLUDED.slippage_bps,
                      updated_at = now()
        RETURNING policy_id
    """
    row = await db.fetchrow(query, name, mode, max_order, max_concurrent, max_daily, slippage_bps)
    return int(row["policy_id"])


async def get_policy(db: Database) -> Optional[dict[str, Any]]:
    row = await db.fetchrow("SELECT policy_id, name, mode, max_notional_per_order, max_concurrent_orders, max_daily_notional, slippage_bps FROM execution_policy WHERE enabled = TRUE ORDER BY policy_id LIMIT 1")
    return dict(row) if row else None


async def create_intent(db: Database, payload: dict[str, Any]) -> dict[str, Any]:
    query = """
        INSERT INTO order_intent (signal_id, market_id, side, qty, limit_price, ttl_secs, status, policy_id, detail_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        RETURNING intent_id, created_at, status
    """
    detail = json.dumps(payload.get("detail_json", {}))
    row = await db.fetchrow(
        query,
        payload.get("signal_id"),
        payload.get("market_id"),
        payload.get("side"),
        payload.get("qty"),
        payload.get("limit_price"),
        payload.get("ttl_secs", 60),
        payload.get("status", "suggested"),
        payload.get("policy_id"),
        detail,
    )
    return {**payload, "intent_id": row["intent_id"], "created_at": row["created_at"], "status": row["status"]}


async def update_intent_status(db: Database, intent_id: int, status: str, detail_json: dict[str, Any] | None = None) -> None:
    detail_payload = json.dumps(detail_json) if detail_json is not None else None
    await db.execute(
        """
        UPDATE order_intent
        SET status = $2, detail_json = COALESCE($3, detail_json), updated_at = now()
        WHERE intent_id = $1
        """,
        intent_id,
        status,
        detail_payload,
    )


async def fetch_intents(db: Database, *, status: Optional[str] = None, limit: int = 50) -> List[dict[str, Any]]:
    query = "SELECT intent_id, signal_id, market_id, side, qty, limit_price, ttl_secs, status, policy_id, detail_json, created_at, updated_at FROM order_intent"
    params: list[Any] = []
    if status:
        query += " WHERE status = $1"
        params.append(status)
    params.append(limit)
    query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
    rows = await db.fetch(query, *params)
    result = []
    for r in rows:
        data = dict(r)
        detail_raw = data.get("detail_json")
        if detail_raw and isinstance(detail_raw, str):
            try:
                data["detail_json"] = json.loads(detail_raw)
            except json.JSONDecodeError:
                data["detail_json"] = {}
        result.append(data)
    return result


async def daily_notional(db: Database, *, day_value: Optional[date] = None) -> float:
    day_value = day_value or datetime.now(timezone.utc).date()
    row = await db.fetchrow(
        """
        SELECT COALESCE(sum(qty * COALESCE(limit_price,0)), 0) AS notional
        FROM order_intent
        WHERE DATE(created_at) = $1 AND status IN ('sent','filled')
        """,
        day_value,
    )
    return float(row["notional"]) if row else 0.0


async def open_intents_count(db: Database) -> int:
    row = await db.fetchrow(
        "SELECT COUNT(1) AS c FROM order_intent WHERE status IN ('suggested','confirmed','sent')"
    )
    return int(row["c"]) if row else 0
