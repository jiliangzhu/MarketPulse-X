from __future__ import annotations

import asyncio
from pathlib import Path

from backend.db import Database
from backend.settings import get_settings
from backend.utils.logging import configure_logging


async def run() -> None:
    configure_logging()
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    migrations_dir = Path("migrations")
    for sql_file in sorted(migrations_dir.glob("*.sql")):
        sql_text = sql_file.read_text(encoding="utf-8").strip()
        if not sql_text:
            continue
        await db.execute(sql_text)
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
