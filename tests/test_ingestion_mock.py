from __future__ import annotations

import pytest

from backend.ingestion.source_mock import MockPolymarketSource


@pytest.mark.asyncio
async def test_mock_source_generates_ticks():
    source = MockPolymarketSource()
    markets = await source.list_markets()
    assert len(markets) >= 1
    market_ids = [market["market_id"] for market in markets]
    ticks = await source.poll_ticks(market_ids)
    assert ticks
    sample = ticks[0]
    assert sample["price"] > 0
    assert sample["market_id"] in market_ids
