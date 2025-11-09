from __future__ import annotations

from typing import Any, Dict, List, Protocol


MarketPayload = Dict[str, Any]
OptionPayload = Dict[str, Any]
TickPayload = Dict[str, Any]


class MarketDataSource(Protocol):
    async def list_markets(self) -> List[MarketPayload]:
        ...

    async def list_options(self, market_id: str) -> List[OptionPayload]:
        ...

    async def poll_ticks(self, market_ids: list[str]) -> List[TickPayload]:
        ...


async def build_data_source(source: str) -> MarketDataSource:
    if source == "real":
        from .source_real import RealPolymarketSource

        return RealPolymarketSource()
    from .source_mock import MockPolymarketSource

    return MockPolymarketSource()
