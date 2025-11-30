from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    time: datetime
    db: str
    rules_heartbeat: str


class MarketOptionSchema(BaseModel):
    option_id: str
    label: str
    last_price: Optional[float] = None
    last_ts: Optional[datetime] = None


class MarketSummarySchema(BaseModel):
    market_id: str
    title: str
    status: str
    ends_at: Optional[datetime] = None
    tags: list[str] = Field(default_factory=list)
    last_updated: Optional[datetime] = None
    options: list[MarketOptionSchema] = Field(default_factory=list)


class MarketDetailSchema(MarketSummarySchema):
    sparkline: list[dict] = Field(default_factory=list)
    synonyms: list[str] = Field(default_factory=list)


class SignalSchema(BaseModel):
    signal_id: int
    market_id: str
    option_id: Optional[str] = None
    level: str
    score: Optional[float] = None
    edge_score: Optional[float] = None
    payload_json: Optional[dict] = None
    created_at: datetime
    source: str = "rule"
    confidence: Optional[float] = None
    ml_features: Optional[dict] = None
    reason: Optional[str] = None


class RuleUploadSchema(BaseModel):
    name: str = Field(max_length=128)
    dsl: str = Field(max_length=8000)
