from __future__ import annotations

import random
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from backend.settings import get_settings

from .polymarket_client import MarketDataSource, MarketPayload, OptionPayload, TickPayload


@dataclass
class MockMarket:
    market_id: str
    title: str
    status: str = "active"
    ends_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc) + timedelta(hours=2))
    starts_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc) - timedelta(days=1))
    tags: list[str] = field(default_factory=list)
    platform: str = "mock"


class MockPolymarketSource(MarketDataSource):
    def __init__(self, *, platform_label: str | None = None) -> None:
        random.seed(42)
        if platform_label is None:
            try:
                settings = get_settings()
                platform_label = "polymarket" if settings.data_source == "mock" else "mock"
            except Exception:  # pragma: no cover - settings may not be initialized
                platform_label = "mock"
        self.platform_label = platform_label
        now = datetime.now(timezone.utc)
        self.markets: list[MockMarket] = [
            MockMarket(
                market_id="mock-election",
                title="Will candidate A win the election?",
                ends_at=now + timedelta(hours=5),
                tags=["politics"],
                platform=self.platform_label,
            ),
            MockMarket(
                market_id="mock-fed",
                title="Will the Fed raise rates in December?",
                ends_at=now + timedelta(days=2),
                tags=["rates"],
                platform=self.platform_label,
            ),
            MockMarket(
                market_id="mock-endgame",
                title="Will Team X sweep the finals?",
                ends_at=now + timedelta(minutes=25),
                tags=["sports"],
                platform=self.platform_label,
            ),
        ]
        self.options: Dict[str, list[OptionPayload]] = {
            "mock-election": [
                {"option_id": "mock-election-yes", "market_id": "mock-election", "label": "Yes"},
                {"option_id": "mock-election-no", "market_id": "mock-election", "label": "No"},
            ],
            "mock-fed": [
                {"option_id": "mock-fed-up", "market_id": "mock-fed", "label": "Hike"},
                {"option_id": "mock-fed-hold", "market_id": "mock-fed", "label": "Hold"},
                {"option_id": "mock-fed-cut", "market_id": "mock-fed", "label": "Cut"},
            ],
            "mock-endgame": [
                {"option_id": "mock-endgame-yes", "market_id": "mock-endgame", "label": "Sweep"},
                {"option_id": "mock-endgame-no", "market_id": "mock-endgame", "label": "No sweep"},
            ],
        }
        self.state: Dict[str, Dict[str, float]] = {}
        for market in self.markets:
            for option in self.options[market.market_id]:
                base_price = random.uniform(0.3, 0.7)
                self.state[option["option_id"]] = {
                    "price": base_price,
                    "liquidity": random.uniform(200, 800),
                }

    async def list_markets(self) -> List[MarketPayload]:
        return [self._serialize_market(m) for m in self.markets]

    async def list_options(self, market_id: str) -> List[OptionPayload]:
        return deepcopy(self.options.get(market_id, []))

    async def poll_ticks(self, market_ids: list[str]) -> List[TickPayload]:
        ticks: list[TickPayload] = []
        now = datetime.now(timezone.utc)
        for market_id in market_ids:
            options = self.options.get(market_id, [])
            if not options:
                continue
            total_price = 0.0
            local_prices: dict[str, float] = {}
            for option in options:
                state = self.state[option["option_id"]]
                drift = random.uniform(-0.02, 0.02)
                if random.random() < 0.07:
                    drift += random.choice([-0.08, 0.09])
                price = min(0.99, max(0.01, state["price"] + drift))
                state["price"] = price
                state["liquidity"] = max(150.0, min(1200.0, state["liquidity"] + random.uniform(-50, 60)))
                volume = random.uniform(50, 300) * (1 + random.random())
                best_bid = round(max(0.0, price - random.uniform(0.005, 0.02)), 4)
                best_ask = round(min(1.0, price + random.uniform(0.005, 0.02)), 4)
                tick = {
                    "ts": now,
                    "market_id": market_id,
                    "option_id": option["option_id"],
                    "price": round(price, 4),
                    "volume": round(volume, 4),
                    "liquidity": round(state["liquidity"], 2),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                }
                ticks.append(tick)
                total_price += price
                local_prices[option["option_id"]] = price

            if len(options) > 2 and random.random() < 0.35:
                scale = random.uniform(0.7, 0.95)
                for option in options:
                    option_id = option["option_id"]
                    new_price = max(0.01, min(0.99, local_prices[option_id] * scale))
                    self.state[option_id]["price"] = new_price

            if market_id == "mock-endgame" and random.random() < 0.5:
                self.state["mock-endgame-yes"]["price"] = max(0.92, self.state["mock-endgame-yes"]["price"] + 0.05)
                self.state["mock-endgame-yes"]["liquidity"] = 650

        return ticks

    def _serialize_market(self, market: MockMarket) -> MarketPayload:
        return {
            "market_id": market.market_id,
            "title": market.title,
            "status": market.status,
            "starts_at": market.starts_at,
            "ends_at": market.ends_at,
            "platform": market.platform,
            "tags": market.tags,
        }
