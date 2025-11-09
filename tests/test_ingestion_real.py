from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from backend.ingestion.source_real import RealPolymarketSource


@pytest.mark.asyncio
async def test_real_source_uses_orderbooks_and_cache():
    book_calls: dict[str, int] = {"token-yes": 0, "token-no": 0}

    list_payload = [
        {
            "id": "m1",
            "question": "Test market?",
            "startDate": "2024-01-01T00:00:00Z",
            "endDate": "2025-01-01T00:00:00Z",
            "closed": False,
            "categories": ["test"],
        }
    ]
    detail_payload = {
        "id": "m1",
        "question": "Test market?",
        "startDate": "2024-01-01T00:00:00Z",
        "endDate": "2025-01-01T00:00:00Z",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["token-yes","token-no"]',
        "outcomePrices": '["0.12","0.88"]',
        "liquidityClob": "5000",
        "volume24hrClob": "1200",
    }
    orderbooks = {
        "token-yes": {
            "timestamp": int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
            "bids": [{"price": "0.1"}],
            "asks": [{"price": "0.14"}],
        },
        "token-no": {
            "timestamp": int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
            "bids": [{"price": "0.82"}],
            "asks": [{"price": "0.9"}],
        },
    }

    def responder(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        path = request.url.path
        if host == "gamma-api.polymarket.com":
            if path == "/markets":
                return httpx.Response(200, json=list_payload)
            if path == "/markets/m1":
                return httpx.Response(200, json=detail_payload)
        if host == "clob.polymarket.com" and path == "/book":
            token_id = request.url.params.get("token_id")
            book_calls[token_id] += 1
            return httpx.Response(200, json=orderbooks[token_id])
        return httpx.Response(404, json={"error": "not found"})

    transport = httpx.MockTransport(responder)
    gamma_client = httpx.AsyncClient(transport=transport)
    clob_client = httpx.AsyncClient(transport=transport)
    source = RealPolymarketSource(gamma_client=gamma_client, clob_client=clob_client)

    markets = await source.list_markets()
    assert markets[0]["market_id"] == "m1"
    options = await source.list_options("m1")
    assert options[0]["option_id"] == "token-yes"

    first = await source.poll_ticks(["m1"])
    assert first and first[0]["price"] > 0

    second = await source.poll_ticks(["m1"])
    assert second
    assert book_calls["token-yes"] == 1
    assert book_calls["token-no"] == 1

    await source.aclose()
