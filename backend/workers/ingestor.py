from __future__ import annotations

import asyncio
import contextlib
from typing import Any

try:  # pragma: no cover - optional
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover - fallback
    pass

from backend.db import Database
from backend.ingestion.polymarket_client import build_data_source
from backend.ingestion.source_real import RealPolymarketSource
from backend.ingestion.source_websocket import WebSocketMarketSource, websocket_available
from backend.processing.stream import StreamProcessor
from backend.repo import markets_repo
from backend.settings import get_settings
from backend.utils.config import load_app_config
from backend.utils.logging import configure_logging, get_logger


async def main() -> None:
    settings = get_settings()
    configure_logging()
    logger = get_logger("ingestor-worker")
    config = load_app_config(settings.config_app_path)
    db = Database(settings.database_dsn)
    await db.connect()
    interval = config.get("app", {}).get("ingestion_interval_secs", 1)
    parallelism = config.get("scheduler", {}).get("max_concurrency", 3)

    if settings.data_source == "mock":
        source = await build_data_source(settings.data_source)
        stream = StreamProcessor(
            db,
            source,
            interval=interval,
            parallelism=parallelism,
        )
        await stream.initialize()
        logger.info("ingestor-started", extra={"mode": "polling"})
        try:
            await stream.run_polling()
        finally:
            close_source = getattr(source, "aclose", None)
            if callable(close_source):
                await close_source()
            await db.disconnect()
        return

    # Real 数据源目前不接受筛选参数，直接初始化
    http_source = RealPolymarketSource()
    logger.info("Bootstrapping asset list via HTTP...")
    markets = await http_source.list_markets()
    # 限制启动时的市场数量，避免全量拉取导致超时
    if settings.market_bootstrap_limit and len(markets) > settings.market_bootstrap_limit:
        markets = markets[: settings.market_bootstrap_limit]
    asset_to_market_map: dict[str, str] = {}
    for market in markets:
        await markets_repo.upsert_market(db, market)
        options = await http_source.list_options(market["market_id"])
        await markets_repo.upsert_options(db, options)
        for opt in options:
            asset_id = opt.get("option_id")
            market_id = opt.get("market_id")
            if asset_id and market_id:
                asset_to_market_map[asset_id] = market_id
    all_asset_ids = list(asset_to_market_map.keys())
    logger.info("asset-bootstrap-complete", extra={"assets": len(all_asset_ids), "markets": len(markets)})

    if not websocket_available:
        logger.warning("websocket-deps-missing", extra={"fallback": "http-polling"})
        stream = StreamProcessor(
            db,
            source=http_source,
            interval=interval,
            parallelism=parallelism,
        )
        await stream.initialize()
        logger.info("ingestor-started", extra={"mode": "polling-fallback"})
        try:
            await stream.run_polling()
        finally:
            await http_source.aclose()
            await db.disconnect()
        return

    await http_source.aclose()
    data_queue: asyncio.Queue[list[dict[str, Any]]] = asyncio.Queue()
    websocket_source = WebSocketMarketSource(asset_to_market_map)
    stream = StreamProcessor(
        db,
        source=None,
        interval=interval,
        parallelism=parallelism,
    )
    logger.info("ingestor-started", extra={"mode": "websocket"})
    consumer_task = asyncio.create_task(stream.run_consumer(data_queue), name="ws-consumer")
    producer_task = asyncio.create_task(websocket_source.run(data_queue, all_asset_ids), name="ws-producer")
    try:
        await asyncio.gather(consumer_task, producer_task)
    finally:
        for task in (consumer_task, producer_task):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
