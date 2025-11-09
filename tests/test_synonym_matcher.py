from __future__ import annotations

import asyncio

import pytest

from backend.processing.synonym_matcher import SynonymMatcher
from backend.repo import markets_repo


class DummyDB:
    def __init__(self):
        self.inserted = []

    async def fetchrow(self, query: str, *args):
        if "SELECT group_id" in query:
            return None
        if query.startswith("INSERT INTO synonym_group"):
            return {"group_id": 1}
        return None

    async def execute(self, query: str, *args):
        return None

    async def executemany(self, query: str, payload):
        self.inserted.extend(payload)


@pytest.mark.asyncio
async def test_synonym_matcher_builds_groups(monkeypatch, tmp_path):
    cfg = tmp_path / "syn.yml"
    cfg.write_text("""
    groups:
      - name: test-group
        method: keyword
        keywords: [alpha]
    """, encoding="utf-8")
    matcher = SynonymMatcher(cfg)

    async def fake_markets(db, status=None, limit=200):
        return [
            {"market_id": "m-alpha", "title": "Alpha Market"},
            {"market_id": "m-beta", "title": "Beta"},
        ]

    monkeypatch.setattr(markets_repo, "list_markets", fake_markets)
    db = DummyDB()
    groups = await matcher.build_groups(db)
    assert groups
    assert db.inserted  # ensures members inserted
