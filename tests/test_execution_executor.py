from __future__ import annotations

import pytest

from backend.execution.executor import Executor


class MockDB:
    async def fetchrow(self, query: str, *args):
        if "COUNT" in query:
            return {"c": 0}
        if "sum(qty" in query:
            return {"notional": 0}
        if "DISTINCT" in query:
            return {"option_id": "o1", "ts": None, "price": 0.5, "liquidity": 600}
        return None

    async def fetch(self, query: str, *args):
        if "DISTINCT" in query:
            return [
                {"option_id": "o1", "ts": None, "price": 0.5, "liquidity": 600},
            ]
        return []


@pytest.mark.asyncio
async def test_executor_confirms_with_mock_source():
    settings = type(
        "S",
        (),
        {
            "exec_max_notional_per_order": 500,
            "exec_max_concurrent_orders": 5,
            "exec_max_daily_notional": 1000,
            "exec_slippage_bps": 100,
            "data_source": "mock",
        },
    )()
    executor = Executor(MockDB(), settings)
    intent = {
        "intent_id": 1,
        "qty": 1,
        "limit_price": 0.5,
        "side": "buy",
        "market_id": "m1",
        "policy_id": 1,
        "detail_json": {},
    }
    result = await executor.confirm_and_execute(intent)
    assert result["status"] == "filled"


@pytest.mark.asyncio
async def test_executor_rejects_on_slippage():
    class TightSettings:
        exec_max_notional_per_order = 500
        exec_max_concurrent_orders = 5
        exec_max_daily_notional = 1000
        exec_slippage_bps = 1
        data_source = "mock"

    executor = Executor(MockDB(), TightSettings())
    intent = {
        "intent_id": 2,
        "qty": 10,
        "limit_price": 1.0,
        "side": "buy",
        "market_id": "m1",
        "policy_id": 1,
        "detail_json": {},
    }
    result = await executor.confirm_and_execute(intent)
    assert result["status"] == "rejected"
    assert "checks" in result["detail_json"]
