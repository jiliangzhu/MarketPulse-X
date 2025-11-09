from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from backend.db import Database


async def insert_ticks(db: Database, ticks: list[dict[str, Any]]) -> None:
    if not ticks:
        return
    query = """
        INSERT INTO tick (ts, market_id, option_id, price, volume, best_bid, best_ask, liquidity)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (ts, market_id, option_id)
        DO NOTHING
    """
    values = [
        (
            tick.get("ts"),
            tick.get("market_id"),
            tick.get("option_id"),
            float(tick.get("price", 0)),
            float(tick.get("volume", 0) or 0),
            float(tick.get("best_bid", 0) or 0),
            float(tick.get("best_ask", 0) or 0),
            float(tick.get("liquidity", 0) or 0),
        )
        for tick in ticks
    ]
    await db.executemany(query, values)


async def recent_ticks(
    db: Database, market_id: str, *, minutes: int = 5, limit: int = 120
) -> list[dict[str, Any]]:
    window = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    rows = await db.fetch(
        """
        SELECT ts, market_id, option_id, price, volume, best_bid, best_ask, liquidity
        FROM tick
        WHERE market_id = $1 AND ts >= $2
        ORDER BY ts DESC
        LIMIT $3
        """,
        market_id,
        window,
        limit,
    )
    return [dict(r) for r in rows]


async def latest_ticks_by_market(db: Database, market_id: str) -> dict[str, dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT DISTINCT ON (option_id) option_id, ts, price, volume, liquidity, best_bid, best_ask
        FROM tick
        WHERE market_id = $1
        ORDER BY option_id, ts DESC
        """,
        market_id,
    )
    return {row["option_id"]: dict(row) for row in rows}


async def latest_tick_ts(db: Database) -> Optional[datetime]:
    row = await db.fetchrow("SELECT ts FROM tick ORDER BY ts DESC LIMIT 1")
    return row["ts"] if row else None
