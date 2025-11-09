from __future__ import annotations

import pytest

from backend.alerting.notifier_telegram import TelegramNotifier
from backend.settings import Settings


@pytest.mark.asyncio
async def test_notifier_dry_run(monkeypatch):
    settings = Settings(telegram_enabled=False)
    notifier = TelegramNotifier(settings)
    status = await notifier.send_message("test", dedupe_key="x", cooldown_secs=0)
    assert status == "dry-run"
    await notifier.aclose()
