from __future__ import annotations

import asyncio

from backend.db import Database
from backend.ingestion.source_mock import MockPolymarketSource
from backend.repo import ticks_repo
from backend.settings import get_settings


async def main(iterations: int = 5) -> None:
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    source = MockPolymarketSource()
    market_ids = [m["market_id"] for m in await source.list_markets()]
    for _ in range(iterations):
        ticks = await source.poll_ticks(market_ids)
        await ticks_repo.insert_ticks(db, ticks)
        await asyncio.sleep(1)
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
