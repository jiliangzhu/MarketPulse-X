from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI

from backend.alerting.notifier_telegram import TelegramNotifier
from backend.db import Database
from backend.ingestion.polymarket_client import build_data_source
from backend.ingestion.source_real import RealPolymarketSource
from backend.ingestion.source_websocket import WebSocketMarketSource, websocket_available
from backend.processing.rules_engine import RulesEngine
from backend.processing.stream import StreamProcessor
from backend.repo import markets_repo
from backend.settings import Settings
from backend.utils.config import load_app_config
from backend.utils.logging import get_logger


async def bootstrap_services(app: FastAPI, settings: Settings, db: Database):
    logger = get_logger("bootstrap")
    config = load_app_config(settings.config_app_path)
    app.state.config = config
    notifier = TelegramNotifier(settings)
    app.state.notifier = notifier

    tasks: list[asyncio.Task] = []
    if settings.service_role == "all":
        interval = config.get("app", {}).get("ingestion_interval_secs", 1)
        parallelism = config.get("scheduler", {}).get("max_concurrency", 3)

        rules_engine = RulesEngine(
            db,
            notifier,
            settings,
            settings.config_rules_path,
            interval_secs=config.get("app", {}).get("rules_interval_secs", 2),
        )
        await rules_engine.load_rules()
        app.state.rules_engine = rules_engine

        if settings.data_source == "mock":
            data_source = await build_data_source(settings.data_source)
            stream = StreamProcessor(
                db,
                data_source,
                interval=interval,
                parallelism=parallelism,
            )
            await stream.initialize()
            app.state.stream = stream
            tasks = [
                asyncio.create_task(stream.run_polling(app.state), name="stream-loop"),
                asyncio.create_task(rules_engine.run(app.state), name="rules-loop"),
            ]
            logger.info("services-bootstrapped", extra={"mode": "all-polling"})
            return tasks

        http_source = RealPolymarketSource()
        markets = await http_source.list_markets()
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

        if not websocket_available:
            logger.warning("websocket-deps-missing", extra={"fallback": "http-polling"})
            stream = StreamProcessor(
                db,
                source=http_source,
                interval=interval,
                parallelism=parallelism,
            )
            await stream.initialize()
            app.state.stream = stream
            tasks = [
                asyncio.create_task(stream.run_polling(app.state), name="stream-loop"),
                asyncio.create_task(rules_engine.run(app.state), name="rules-loop"),
            ]
            logger.info("services-bootstrapped", extra={"mode": "all-polling-fallback"})
            return tasks

        await http_source.aclose()
        stream = StreamProcessor(
            db,
            source=None,
            interval=interval,
            parallelism=parallelism,
        )
        app.state.stream = stream
        websocket_source = WebSocketMarketSource(asset_to_market_map)
        data_queue: asyncio.Queue[list[dict[str, Any]]] = asyncio.Queue()
        consumer_task = asyncio.create_task(stream.run_consumer(data_queue, app.state), name="stream-loop")
        producer_task = asyncio.create_task(
            websocket_source.run(data_queue, all_asset_ids),
            name="ws-loop",
        )

        tasks = [
            consumer_task,
            producer_task,
            asyncio.create_task(rules_engine.run(app.state), name="rules-loop"),
        ]
        logger.info("services-bootstrapped", extra={"mode": "all-websocket"})
    else:
        logger.info("bootstrap-skip", extra={"role": settings.service_role})
    return tasks
