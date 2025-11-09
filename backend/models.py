from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel


class MarketOption(BaseModel):
    option_id: str
    market_id: str
    label: str


class Market(BaseModel):
    market_id: str
    title: str
    platform: str = "polymarket"
    status: str
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    tags: list[str] = []


class Tick(BaseModel):
    ts: datetime
    market_id: str
    option_id: str
    price: float
    volume: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    liquidity: Optional[float] = None


class Signal(BaseModel):
    signal_id: int
    market_id: str
    option_id: Optional[str] = None
    level: Literal["P1", "P2", "P3"]
    score: Optional[float] = None
    payload_json: Optional[dict] = None
    created_at: datetime


class RuleDefinition(BaseModel):
    rule_id: Optional[int] = None
    name: str
    type: str
    enabled: bool = True
    cooldown_secs: Optional[int] = None
    raw_yaml: Optional[str] = None
