from __future__ import annotations

import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from backend.db import Database
from backend.ingestion.polymarket_client import MarketDataSource
from backend.metrics import ingest_latency_ms, ingest_last_tick_ts
from backend.repo import markets_repo, ticks_repo
from backend.utils.logging import get_logger


class StreamProcessor:
    def __init__(
        self,
        db: Database,
        source: MarketDataSource | None = None,
        interval: float = 1.0,
        *,
        parallelism: int = 3,
    ) -> None:
        self.db = db
        self.source = source
        self.interval = interval
        self.logger = get_logger("stream")
        self.market_ids: list[str] = []
        self.parallelism = max(1, parallelism)
        self._cache: dict[tuple[str, str], float] = {}
        self.source_label = source.__class__.__name__ if source else "WebSocketMarketSource"

    async def initialize(self) -> None:
        if not self.source:
            self.logger.info("stream-initialize-skipped", extra={"reason": "no-http-source"})
            return
        markets = await self.source.list_markets()
        for market in markets:
            await markets_repo.upsert_market(self.db, market)
            options = await self.source.list_options(market["market_id"])
            await markets_repo.upsert_options(self.db, options)
        self.market_ids = [m["market_id"] for m in markets]
        self.logger.info("stream-initialized", extra={"count": len(self.market_ids)})

    async def run_polling(self, app_state: Optional[Any] = None) -> None:
        if not self.source:
            raise RuntimeError("Polling requested without HTTP source")
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
                await self._persist_ticks(ticks, app_state)
                duration = (time.perf_counter() - start) * 1000
                ingest_latency_ms.labels(source=self.source_label).observe(duration)
                backoff = 1
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.error("stream-polling-error", extra={"error": str(exc)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            await asyncio.sleep(self.interval)

    async def run_consumer(self, data_queue: "asyncio.Queue[list[dict[str, Any]]]", app_state: Optional[Any] = None) -> None:
        while True:
            ticks = await data_queue.get()
            start = time.perf_counter()
            try:
                await self._persist_ticks(ticks, app_state)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.error("stream-consumer-error", extra={"error": str(exc)})
            finally:
                data_queue.task_done()
                duration = (time.perf_counter() - start) * 1000
                ingest_latency_ms.labels(source=self.source_label).observe(duration)

    async def _persist_ticks(self, ticks: list[dict[str, Any]], app_state: Optional[Any]) -> None:
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

    def _filter_ticks(self, ticks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fresh: list[dict[str, Any]] = []
        for tick in ticks:
            key = (tick["market_id"], tick.get("option_id"))
            price = self._normalize_price(tick)
            cached = self._cache.get(key)
            if cached is None or abs(cached - price) > 1e-4:
                self._cache[key] = price
                fresh.append(tick)
        return fresh

    def _normalize_price(self, tick: dict[str, Any]) -> float:
        raw_price = tick.get("price")
        try:
            price = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            price = None
        if price is None or price == 0:
            best_bid = tick.get("best_bid")
            best_ask = tick.get("best_ask")
            bid = float(best_bid) if best_bid not in (None, "") else None
            ask = float(best_ask) if best_ask not in (None, "") else None
            if bid is not None and ask is not None:
                price = (bid + ask) / 2
            elif bid is not None:
                price = bid
            elif ask is not None:
                price = ask
            else:
                price = 0.0
            tick["price"] = price
        return float(price)

    def _last_ts(self, ticks: list[dict[str, Any]]) -> Optional[datetime]:
        valid = [tick.get("ts") for tick in ticks if tick.get("ts") is not None]
        if not valid:
            return None
        return max(valid)
