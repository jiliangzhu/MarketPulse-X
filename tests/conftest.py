from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api import health, kpi, markets, signals
from backend.deps import get_db


class FakeDB:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.markets = [
            {
                "market_id": "m1",
                "title": "Test Market",
                "platform": "polymarket",
                "status": "active",
                "starts_at": now - timedelta(days=1),
                "ends_at": now + timedelta(hours=1),
                "tags": ["test"],
            }
        ]
        self.options = {
            "m1": [
                {"option_id": "m1-yes", "market_id": "m1", "label": "Yes"},
                {"option_id": "m1-no", "market_id": "m1", "label": "No"},
            ]
        }
        tick = {
            "option_id": "m1-yes",
            "ts": now,
            "price": 0.55,
            "volume": 120,
            "liquidity": 500,
            "best_bid": 0.54,
            "best_ask": 0.56,
        }
        tick_no = tick | {"option_id": "m1-no", "price": 0.48}
        self.latest_ticks = {"m1": {"m1-yes": tick, "m1-no": tick_no}}
        self.recent_ticks = {
            "m1": [
                tick,
                tick | {"ts": now - timedelta(seconds=5), "price": 0.5},
                tick_no,
            ]
        }
        self.signals: list[dict[str, Any]] = [
            {
                "signal_id": 1,
                "market_id": "m1",
                "option_id": "m1-yes",
                "level": "P1",
                "score": 80,
                "payload_json": {"transport": "telegram-dry-run", "rule_type": "SUM_LT_1"},
                 "edge_score": 0.05,
                "created_at": now,
            }
        ]
        self.kpi_rows = [
            {
                "day": now.date(),
                "rule_type": "SUM_LT_1",
                "signals": 1,
                "p1_signals": 1,
                "avg_gap": 0.02,
                "est_edge_bps": 10,
            }
        ]

    async def fetch(self, query: str, *args: Any):
        if "FROM market_option" in query:
            market_id = args[0]
            return self.options.get(market_id, [])
        if "FROM market" in query and "WHERE market_id" not in query:
            limit = args[-1]
            status = args[0] if "WHERE status" in query else None
            rows = [m for m in self.markets if status in (None, m["status"])]
            return rows[:limit]
        if "DISTINCT ON" in query:
            market_id = args[0]
            ticks = self.latest_ticks.get(market_id, {})
            return [tick | {"option_id": oid} for oid, tick in ticks.items()]
        if "SELECT ts, market_id" in query:
            market_id, _, limit = args
            return self.recent_ticks.get(market_id, [])[:limit]
        if query.startswith("SELECT signal_id"):
            return self.signals
        if "FROM synonym_group_member" in query:
            return []
        if "FROM rule_kpi_daily" in query:
            return self.kpi_rows
        return []

    async def fetchrow(self, query: str, *args: Any):
        if query.startswith("SELECT 1"):
            return {"?column?": 1}
        if "FROM market" in query and "market_id" in query:
            market_id = args[0]
            return next((m for m in self.markets if m["market_id"] == market_id), None)
        if query.startswith("INSERT INTO signal"):
            next_id = len(self.signals) + 1
            record = {
                "signal_id": next_id,
                "market_id": args[0],
                "option_id": args[1],
                "rule_id": args[2],
                "level": args[3],
                "score": args[4],
                "payload_json": args[5],
                "edge_score": args[6],
                "created_at": datetime.now(timezone.utc),
            }
            self.signals.append(record)
            return {"signal_id": next_id}
        if query.startswith("SELECT created_at FROM signal"):
            return {"created_at": self.signals[-1]["created_at"]}
        return None

    async def execute(self, *_args: Any, **_kwargs: Any):  # pragma: no cover - simple stub
        return "OK"


@pytest.fixture
def fake_db():
    return FakeDB()


@pytest.fixture
def test_app(fake_db):
    app = FastAPI()
    app.include_router(health.router)
    app.include_router(markets.router)
    app.include_router(signals.router)
    app.include_router(kpi.router)
    app.state.rules_last_run = datetime.now(timezone.utc)

    async def _get_db():
        return fake_db

    app.dependency_overrides[get_db] = _get_db
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(test_app):
    with TestClient(test_app) as client:
        yield client
