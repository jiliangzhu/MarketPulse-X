from __future__ import annotations

import asyncio

try:  # pragma: no cover - optional
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover - fallback
    pass

from backend.alerting.notifier_telegram import TelegramNotifier
from backend.db import Database
from backend.metrics import signals_counter
from backend.processing.rules_engine import RulesEngine
from backend.settings import get_settings
from backend.utils.config import load_app_config
from backend.utils.logging import configure_logging, get_logger


async def main() -> None:
    settings = get_settings()
    configure_logging()
    logger = get_logger("rules-worker")
    config = load_app_config(settings.config_app_path)
    db = Database(settings.database_dsn)
    await db.connect()
    notifier = TelegramNotifier(settings)
    rules_engine = RulesEngine(
        db,
        notifier,
        settings,
        settings.config_rules_path,
        interval_secs=config.get("app", {}).get("rules_interval_secs", 2),
    )
    await rules_engine.load_rules()
    logger.info("rules-engine-started")
    try:
        await rules_engine.run()
    finally:
        await notifier.aclose()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
