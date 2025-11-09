from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import yaml

from backend.db import Database
from backend.repo import markets_repo
from backend.utils.logging import get_logger


def normalize_tags(tags: Iterable[str]) -> set[str]:
    return {tag.lower() for tag in tags}


class SynonymMatcher:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.logger = get_logger("synonym-matcher")
        self.config = self._load_config()

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            return {"groups": []}
        with self.config_path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {"groups": []}

    async def build_groups(self, db: Database) -> List[dict]:
        markets = await markets_repo.list_markets(db, status=None, limit=200)
        groups: list[dict] = []
        for entry in self.config.get("groups", []):
            keywords = [kw.lower() for kw in entry.get("keywords", [])]
            members: list[str] = []
            for market in markets:
                title = market["title"].lower()
                if any(kw in title for kw in keywords) or market["market_id"] in entry.get("explicit", []):
                    members.append(market["market_id"])
            members = sorted(set(members))
            if len(members) < entry.get("group_min_size", 1):
                continue
            groups.append(
                {
                    "name": entry.get("name"),
                    "method": entry.get("method", "keyword"),
                    "members": members,
                }
            )
        await self._sync_db(db, groups)
        return groups

    async def _sync_db(self, db: Database, groups: List[dict]) -> None:
        for group in groups:
            row = await db.fetchrow("SELECT group_id FROM synonym_group WHERE title = $1", group["name"])
            if row:
                group_id = row["group_id"]
                await db.execute("UPDATE synonym_group SET updated_at = now() WHERE group_id = $1", group_id)
            else:
                new_row = await db.fetchrow(
                    "INSERT INTO synonym_group (method, title) VALUES ($1,$2) RETURNING group_id",
                    group["method"],
                    group["name"],
                )
                group_id = new_row["group_id"]
            await db.execute("DELETE FROM synonym_group_member WHERE group_id = $1", group_id)
            insert_payload = [(group_id, market_id) for market_id in group["members"]]
            if insert_payload:
                await db.executemany(
                    "INSERT INTO synonym_group_member (group_id, market_id) VALUES ($1,$2)",
                    insert_payload,
                )
            self.logger.info("synonym-group-updated", extra={"group_name": group["name"], "size": len(group["members"])})
