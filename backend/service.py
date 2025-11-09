from __future__ import annotations

import asyncio
from typing import List

from fastapi import FastAPI

from backend.alerting.notifier_telegram import TelegramNotifier
from backend.db import Database
from backend.ingestion.polymarket_client import build_data_source
from backend.processing.rules_engine import RulesEngine
from backend.processing.stream import StreamProcessor
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
        data_source = await build_data_source(settings.data_source)
        stream = StreamProcessor(
            db,
            data_source,
            interval=config.get("app", {}).get("ingestion_interval_secs", 1),
            parallelism=config.get("scheduler", {}).get("max_concurrency", 3),
        )
        await stream.initialize()
        app.state.stream = stream

        rules_engine = RulesEngine(
            db,
            notifier,
            settings,
            settings.config_rules_path,
            interval_secs=config.get("app", {}).get("rules_interval_secs", 2),
        )
        await rules_engine.load_rules()
        app.state.rules_engine = rules_engine

        tasks = [
            asyncio.create_task(stream.run(app.state), name="stream-loop"),
            asyncio.create_task(rules_engine.run(app.state), name="rules-loop"),
        ]
        logger.info("services-bootstrapped", extra={"mode": "all"})
    else:
        logger.info("bootstrap-skip", extra={"role": settings.service_role})
    return tasks
