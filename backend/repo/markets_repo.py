from __future__ import annotations

from typing import Any, Iterable, List, Optional

from backend.db import Database


async def upsert_market(db: Database, market: dict[str, Any]) -> None:
    query = """
        INSERT INTO market (market_id, title, platform, status, starts_at, ends_at, tags)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (market_id)
        DO UPDATE SET title = EXCLUDED.title,
                      platform = EXCLUDED.platform,
                      status = EXCLUDED.status,
                      starts_at = EXCLUDED.starts_at,
                      ends_at = EXCLUDED.ends_at,
                      tags = EXCLUDED.tags
    """
    await db.execute(
        query,
        market.get("market_id"),
        market.get("title"),
        market.get("platform", "polymarket"),
        market.get("status", "active"),
        market.get("starts_at"),
        market.get("ends_at"),
        market.get("tags", []),
    )


async def upsert_options(db: Database, options: Iterable[dict[str, Any]]) -> None:
    query = """
        INSERT INTO market_option (option_id, market_id, label)
        VALUES ($1, $2, $3)
        ON CONFLICT (option_id)
        DO UPDATE SET label = EXCLUDED.label
    """
    values = [
        (
            opt.get("option_id"),
            opt.get("market_id"),
            opt.get("label"),
        )
        for opt in options
    ]
    if not values:
        return
    await db.executemany(query, values)


async def list_markets(
    db: Database, *, status: Optional[str] = None, limit: int = 50, offset: int = 0
) -> List[dict[str, Any]]:
    base = "SELECT market_id, title, platform, status, starts_at, ends_at, tags FROM market"
    params: list[Any] = []
    if status:
        base += " WHERE status = $1"
        params.append(status)
    params.append(limit)
    base += " ORDER BY ends_at NULLS LAST LIMIT $" + str(len(params))
    if offset:
        params.append(offset)
        base += " OFFSET $" + str(len(params))
    rows = await db.fetch(base, *params)
    return [dict(row) for row in rows]


async def get_market(db: Database, market_id: str) -> Optional[dict[str, Any]]:
    row = await db.fetchrow(
        "SELECT market_id, title, platform, status, starts_at, ends_at, tags FROM market WHERE market_id = $1",
        market_id,
    )
    return dict(row) if row else None


async def list_options(db: Database, market_id: str) -> list[dict[str, Any]]:
    rows = await db.fetch(
        "SELECT option_id, market_id, label FROM market_option WHERE market_id = $1 ORDER BY option_id",
        market_id,
    )
    return [dict(r) for r in rows]


async def synonym_peers(db: Database, market_id: str) -> list[str]:
    rows = await db.fetch(
        """
        SELECT m2.market_id
        FROM synonym_group_member m1
        JOIN synonym_group_member m2 ON m1.group_id = m2.group_id
        WHERE m1.market_id = $1 AND m2.market_id <> $1
        """,
        market_id,
    )
    return [row["market_id"] for row in rows]
