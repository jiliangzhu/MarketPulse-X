from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from backend.risk import guardrails, limits


class DummyDB:
    def __init__(self, liquidity: float = 500.0) -> None:
        self._open = 0
        self._filled_notional = 0
        self._liquidity = liquidity

    async def fetchrow(self, query: str, *args):
        if "COUNT(1)" in query:
            return {"c": self._open}
        if "sum(qty" in query:
            return {"notional": self._filled_notional}
        if "SELECT DISTINCT" in query:
            ts = datetime.now(timezone.utc)
            return {"option_id": "o1", "ts": ts, "price": 0.6, "liquidity": self._liquidity}
        if "SELECT ts" in query:
            return {"ts": datetime.now(timezone.utc)}
        return None

    async def fetch(self, query: str, *args):
        if "SELECT DISTINCT" in query:
            return [
                {"option_id": "o1", "ts": datetime.now(timezone.utc), "price": 0.6, "liquidity": self._liquidity},
            ]
        return []


@pytest.mark.asyncio
async def test_limits_enforced():
    db = DummyDB()
    settings = type("obj", (), {
        "exec_max_notional_per_order": 50,
        "exec_max_concurrent_orders": 1,
        "exec_max_daily_notional": 60,
    })()
    result = await limits.evaluate_limits(db, qty=200, limit_price=1, settings=settings)
    assert not result.ok
    assert "per-order" in result.reasons[0]


@pytest.mark.asyncio
async def test_guardrails_reject_high_slippage():
    db = DummyDB(liquidity=500)
    result = await guardrails.evaluate_guardrails(db, "m1", side="buy", limit_price=1.0, slippage_bps=10)
    assert not result.ok
    assert result.reason == "slippage too high"
