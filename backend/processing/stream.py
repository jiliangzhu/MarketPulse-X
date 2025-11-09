from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from backend.db import Database
from backend.ingestion.polymarket_client import MarketDataSource
from backend.metrics import ingest_latency_ms, ingest_last_tick_ts
from backend.repo import markets_repo, ticks_repo
from backend.utils.logging import get_logger


class StreamProcessor:
    def __init__(self, db: Database, source: MarketDataSource, interval: float = 1.0, *, parallelism: int = 3) -> None:
        self.db = db
        self.source = source
        self.interval = interval
        self.logger = get_logger("stream")
        self.market_ids: list[str] = []
        self.parallelism = max(1, parallelism)
        self._cache: dict[tuple[str, str], float] = {}
        self.source_label = self.source.__class__.__name__

    async def initialize(self) -> None:
        markets = await self.source.list_markets()
        for market in markets:
            await markets_repo.upsert_market(self.db, market)
            options = await self.source.list_options(market["market_id"])
            await markets_repo.upsert_options(self.db, options)
        self.market_ids = [m["market_id"] for m in markets]
        self.logger.info("stream-initialized", extra={"count": len(self.market_ids)})

    async def run(self, app_state: Optional[Any] = None) -> None:
        if not self.market_ids:
            await self.initialize()
        backoff = 1
        while True:
            try:
                start = time.perf_counter()
                ticks: list[dict[str, Any]] = []
                chunks = [self.market_ids[i :: self.parallelism] for i in range(self.parallelism)]
                tasks = [self.source.poll_ticks(chunk) for chunk in chunks if chunk]
                results = await asyncio.gather(*tasks)
                for bucket in results:
                    ticks.extend(bucket)
                new_ticks = self._filter_ticks(ticks)
                if new_ticks:
                    await ticks_repo.insert_ticks(self.db, new_ticks)
                    last_ts = self._last_ts(new_ticks)
                    if last_ts:
                        ingest_last_tick_ts.labels(source=self.source_label).set(last_ts.timestamp())
                        if app_state is not None:
                            app_state.ingestion_last_run = last_ts
                elif app_state is not None:
                    app_state.ingestion_last_run = getattr(app_state, "ingestion_last_run", None)
                duration = (time.perf_counter() - start) * 1000
                ingest_latency_ms.labels(source=self.source.__class__.__name__).observe(duration)
                backoff = 1
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.error("stream-error", extra={"error": str(exc)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            await asyncio.sleep(self.interval)

    def _filter_ticks(self, ticks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fresh: list[dict[str, Any]] = []
        for tick in ticks:
            key = (tick["market_id"], tick.get("option_id"))
            price = float(tick.get("price") or 0)
            cached = self._cache.get(key)
            if cached is None or abs(cached - price) > 1e-4:
                self._cache[key] = price
                fresh.append(tick)
        return fresh

    def _last_ts(self, ticks: list[dict[str, Any]]) -> Optional[datetime]:
        valid = [tick.get("ts") for tick in ticks if tick.get("ts") is not None]
        if not valid:
            return None
        return max(valid)
