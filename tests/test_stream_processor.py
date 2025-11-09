from __future__ import annotations

import asyncio

import pytest

from backend.processing.stream import StreamProcessor


class DummySource:
    async def list_markets(self):
        return []

    async def list_options(self, *_args, **_kwargs):  # pragma: no cover - unused
        return []

    async def poll_ticks(self, market_ids):  # pragma: no cover - unused
        return []


class DummyDB:
    async def connect(self):  # pragma: no cover - unused
        return None


@pytest.mark.asyncio
async def test_filter_ticks_deduplicates():
    sp = StreamProcessor(DummyDB(), DummySource(), interval=1)
    first = sp._filter_ticks([
        {"market_id": "m1", "option_id": "o1", "price": 0.5},
        {"market_id": "m1", "option_id": "o1", "price": 0.5},
    ])
    assert len(first) == 1
    second = sp._filter_ticks([
        {"market_id": "m1", "option_id": "o1", "price": 0.51},
    ])
    assert len(second) == 1
