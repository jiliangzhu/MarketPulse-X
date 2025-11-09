from __future__ import annotations

import asyncio

try:  # pragma: no cover - optional
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover - fallback
    pass

from backend.db import Database
from backend.ingestion.polymarket_client import build_data_source
from backend.processing.stream import StreamProcessor
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
    source = await build_data_source(settings.data_source)
    stream = StreamProcessor(
        db,
        source,
        interval=config.get("app", {}).get("ingestion_interval_secs", 1),
        parallelism=config.get("scheduler", {}).get("max_concurrency", 3),
    )
    await stream.initialize()
    logger.info("ingestor-started")
    try:
        await stream.run()
    finally:
        close_source = getattr(source, "aclose", None)
        if callable(close_source):
            await close_source()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
