from __future__ import annotations

import asyncio
import time
from typing import Optional

import httpx
from cachetools import TTLCache

from backend.metrics import telegram_failures
from backend.settings import Settings
from backend.utils.logging import get_logger


class TelegramNotifier:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = get_logger("telegram")
        self.enabled = bool(settings.telegram_enabled and settings.telegram_bot_token and settings.telegram_chat_id)
        self._client = httpx.AsyncClient(timeout=10.0)
        self._dedupe = TTLCache(maxsize=512, ttl=300)

    async def send_message(
        self,
        text: str,
        *,
        dedupe_key: str,
        cooldown_secs: int = 120,
        parse_mode: Optional[str] = "Markdown",
    ) -> str:
        now = time.time()
        last = self._dedupe.get(dedupe_key)
        if last and now - last < cooldown_secs:
            self.logger.info("telegram-skip", extra={"key": dedupe_key, "reason": "cooldown"})
            return "cooldown"
        self._dedupe[dedupe_key] = now
        if not self.enabled:
            self.logger.info(
                "telegram-dry-run",
                extra={"text": text[:200], "dedupe_key": dedupe_key},
            )
            return "dry-run"
        payload = {
            "chat_id": self.settings.telegram_chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        try:
            resp = await self._client.post(
                f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage",
                json=payload,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network
            self.logger.error("telegram-error", extra={"error": str(exc)})
            telegram_failures.inc()
            return "error"
        return "sent"

    async def aclose(self) -> None:
        await self._client.aclose()
