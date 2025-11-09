from __future__ import annotations

from typing import Any, Iterable, List, Optional

from backend.db import Database, VECTOR_SUPPORTED
from backend.processing.embedding import get_embedding_model


async def upsert_market(db: Database, market: dict[str, Any]) -> None:
    embedding = None
    if VECTOR_SUPPORTED:
        title = market.get("title") or ""
        try:
            embedding = get_embedding_model().encode(title)
        except Exception:  # pragma: no cover - embedding failures shouldn't block ingestion
            embedding = None
    query = """
        INSERT INTO market (market_id, title, platform, status, starts_at, ends_at, tags, embedding)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (market_id)
        DO UPDATE SET title = EXCLUDED.title,
                      platform = EXCLUDED.platform,
                      status = EXCLUDED.status,
                      starts_at = EXCLUDED.starts_at,
                      ends_at = EXCLUDED.ends_at,
                      tags = EXCLUDED.tags,
                      embedding = COALESCE(EXCLUDED.embedding, market.embedding)
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
        embedding,
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


async def synonym_peers(db: Database, market_id: str, limit: int = 5) -> list[str]:
    anchor = await db.fetchrow("SELECT embedding FROM market WHERE market_id = $1", market_id)
    if not anchor or anchor["embedding"] is None:
        return []
    rows = await db.fetch(
        """
        SELECT market_id
        FROM market
        WHERE market_id <> $1
          AND embedding IS NOT NULL
        ORDER BY embedding <-> $2
        LIMIT $3
        """,
        market_id,
        anchor["embedding"],
        limit,
    )
    return [row["market_id"] for row in rows]
