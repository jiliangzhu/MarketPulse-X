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
        if "SELECT embedding" in query:
            return {"embedding": [0.0, 0.0, 0.0]}
        return None

    async def fetch(self, query: str, *args):
        if "SELECT market_id, embedding" in query:
            return [
                {"market_id": "m-alpha", "embedding": [0.1, 0.2, 0.3]},
                {"market_id": "m-beta", "embedding": [0.11, 0.19, 0.33]},
            ]
        return []

    async def execute(self, query: str, *args):
        return None

    async def executemany(self, query: str, payload):
        self.inserted.extend(payload)


@pytest.mark.asyncio
async def test_synonym_matcher_builds_groups(monkeypatch, tmp_path):
    cfg = tmp_path / "syn.yml"
    cfg.write_text("""
    groups:
      - name: manual-group
        explicit: ["m-alpha", "m-beta"]
    """, encoding="utf-8")
    matcher = SynonymMatcher(cfg)

    from backend.processing import synonym_matcher as sm

    if sm.util is not None:
        monkeypatch.setattr(
            "backend.processing.synonym_matcher.util.community_detection",
            lambda embeddings, threshold, min_community_size: [[0, 1]],
        )
    db = DummyDB()
    groups = await matcher.build_groups(db)
    assert groups
    assert db.inserted  # ensures members inserted
