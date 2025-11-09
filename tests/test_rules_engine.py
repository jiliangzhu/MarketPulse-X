from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.processing.rules_engine import Rule, RulesEngine
from backend.repo import kpi_repo, signals_repo
from backend.settings import Settings


class DummyDB:
    async def fetch(self, *args, **kwargs):  # pragma: no cover - not used
        return []


class DummyNotifier(SimpleNamespace):
    async def send_message(self, *args, **kwargs):  # pragma: no cover - not used
        return "dry-run"


@pytest.fixture
def engine():
    settings = Settings()
    return RulesEngine(DummyDB(), DummyNotifier(), settings, Path("configs/rules"))


def _sample_ticks():
    now = datetime.now(timezone.utc)
    latest = {
        "o1": {
            "price": 0.4,
            "liquidity": 400,
            "best_bid": 0.39,
            "best_ask": 0.41,
        },
        "o2": {
            "price": 0.55,
            "liquidity": 410,
            "best_bid": 0.54,
            "best_ask": 0.56,
        },
    }
    recent = [
        {"option_id": "o1", "ts": now, "price": 0.55, "volume": 200},
        {"option_id": "o1", "ts": now, "price": 0.5, "volume": 150},
        {"option_id": "o1", "ts": now, "price": 0.45, "volume": 120},
    ]
    return latest, recent


@pytest.mark.asyncio
async def test_sum_rule_triggers(engine: RulesEngine):
    rule = Rule(
        name="sum",
        type="SUM_LT_1",
        config={
            "thresholds": {"sum_price_lt": 1.1, "min_liquidity": 200},
            "outputs": {"score": {"base": 60, "weights": {}}},
        },
        rule_id=1,
    )
    latest, recent = _sample_ticks()
    result = engine._rule_sum_lt_1(rule, {"title": "T", "market_id": "m", "ends_at": datetime.now(timezone.utc)}, latest, recent, [])
    assert result is not None
    assert "score" in result
    assert result["edge_score"] > 0


@pytest.mark.asyncio
async def test_dutch_rule(engine: RulesEngine):
    rule = Rule(
        name="dutch",
        type="DUTCH_BOOK_DETECT",
        config={
            "params": {"sum_price_lt": 0.99, "min_liquidity": 100},
            "outputs": {"score": {"base": 70, "weights": {}}},
        },
        rule_id=8,
    )
    latest, recent = _sample_ticks()
    result = engine._rule_dutch_book(rule, {"title": "T", "market_id": "m"}, latest, recent, [])
    assert result is not None
    assert result["edge_score"] > 0


@pytest.mark.asyncio
async def test_spike_rule(engine: RulesEngine):
    rule = Rule(
        name="spike",
        type="SPIKE_DETECT",
        config={
            "params": {"window_secs": 10, "pct_change_gt": 0.01, "min_liquidity": 100},
            "outputs": {"score": {"base": 50, "weights": {}}},
        },
        rule_id=2,
    )
    latest, recent = _sample_ticks()
    result = engine._rule_spike(rule, {"title": "T", "market_id": "m"}, latest, recent, [])
    assert result is not None
    assert result["option_id"] == "o1"


@pytest.mark.asyncio
async def test_trend_breakout_rule(engine: RulesEngine):
    now = datetime.now(timezone.utc)
    latest = {
        "o1": {"price": 0.55, "liquidity": 500},
    }
    recent = [
        {"option_id": "o1", "ts": now - timedelta(seconds=50), "price": 0.5},
        {"option_id": "o1", "ts": now - timedelta(seconds=40), "price": 0.51},
        {"option_id": "o1", "ts": now - timedelta(seconds=30), "price": 0.5},
        {"option_id": "o1", "ts": now - timedelta(seconds=10), "price": 0.55},
    ]
    rule = Rule(
        name="trend",
        type="TREND_BREAKOUT",
        config={
            "params": {"lookback_secs": 60, "pct_breakout": 0.02, "min_points": 3, "min_liquidity": 100},
            "outputs": {"score": {"base": 55, "weights": {}}},
        },
        rule_id=6,
    )
    result = engine._rule_trend_breakout(
        rule,
        {"title": "T", "market_id": "m", "ends_at": now + timedelta(hours=1)},
        latest,
        recent,
        [{"option_id": "o1", "label": "Yes"}],
    )
    assert result is not None
    assert result["edge_score"] > 0


def test_synonym_rule(engine: RulesEngine):
    rule = Rule(
        name="synonym",
        type="SYNONYM_MISPRICE",
        config={
            "params": {"group_min_size": 2, "price_gap_gt": 0.02, "min_liquidity": 100},
            "outputs": {"score": {"base": 65, "weights": {"gap": 0.5}}},
        },
        rule_id=3,
    )
    snapshots = {
        "m1": {
            "market": {"market_id": "m1", "title": "A", "ends_at": datetime.now(timezone.utc)},
            "ticks": {"o1": {"price": 0.8, "liquidity": 500}},
        },
        "m2": {
            "market": {"market_id": "m2", "title": "B", "ends_at": datetime.now(timezone.utc)},
            "ticks": {"o1": {"price": 0.7, "liquidity": 500}},
        },
    }
    payloads = engine._rule_synonym(rule, [{"name": "grp", "members": ["m1", "m2"]}], snapshots)
    assert payloads
    market_id, payload = payloads[0]
    assert market_id == "m2"
    assert "gap" in payload["payload"]


def test_cross_market_rule(engine: RulesEngine):
    now = datetime.now(timezone.utc)
    snapshots = {
        "m1": {
            "market": {"market_id": "m1", "title": "A", "ends_at": now},
            "ticks": {"opt-yes": {"price": 0.6, "liquidity": 600}},
            "options": [{"option_id": "opt-yes", "label": "Yes"}],
        },
        "m2": {
            "market": {"market_id": "m2", "title": "B", "ends_at": now},
            "ticks": {"opt-yes": {"price": 0.4, "liquidity": 600}},
            "options": [{"option_id": "opt-yes", "label": "Yes"}],
        },
    }
    rule = Rule(
        name="cross",
        type="CROSS_MARKET_MISPRICE",
        config={
            "params": {"group_min_size": 2, "price_diff_threshold": 0.05, "min_liquidity": 100, "target_label": "yes"},
            "outputs": {"score": {"base": 65, "weights": {}}},
        },
        rule_id=5,
    )
    payloads = engine._rule_cross_market(rule, [{"name": "grp", "members": ["m1", "m2"]}], snapshots)
    assert payloads
    assert payloads[0][1]["edge_score"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_emit_signal(monkeypatch, engine: RulesEngine):
    rule = Rule(
        name="sum",
        type="SUM_LT_1",
        config={"outputs": {"level": "P1", "score": {"base": 60, "weights": {}}}},
        rule_id=7,
    )
    payload = {"message": "test", "score": 70, "payload": {}}
    inserted = {}

    async def fake_insert_signal(db, data):
        inserted["data"] = data
        return 42

    async def fake_audit(*_args, **_kwargs):
        return None

    async def fake_kpi(*_args, **_kwargs):
        return None

    async def fake_send(*_args, **_kwargs):
        return "sent"

    monkeypatch.setattr(signals_repo, "insert_signal", fake_insert_signal)
    monkeypatch.setattr(signals_repo, "insert_audit", fake_audit)
    monkeypatch.setattr(kpi_repo, "record_kpi", fake_kpi)
    engine.notifier.send_message = fake_send  # type: ignore[attr-defined]
    await engine._emit_signal(rule, "m1", payload)
    assert inserted["data"]["market_id"] == "m1"
