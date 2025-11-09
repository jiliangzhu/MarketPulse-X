from __future__ import annotations

import asyncio

from backend.db import Database
from backend.ingestion.source_mock import MockPolymarketSource
from backend.repo import markets_repo, ticks_repo
from backend.settings import get_settings


async def seed() -> None:
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    source = MockPolymarketSource()
    markets = await source.list_markets()
    for market in markets:
        await markets_repo.upsert_market(db, market)
        options = await source.list_options(market["market_id"])
        await markets_repo.upsert_options(db, options)
    ticks = await source.poll_ticks([m["market_id"] for m in markets])
    await ticks_repo.insert_ticks(db, ticks)
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(seed())
