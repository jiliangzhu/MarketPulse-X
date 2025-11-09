from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

try:  # pragma: no cover - optional vector math
    import numpy as np
except Exception:  # pragma: no cover - fallback
    np = None  # type: ignore
import yaml

try:  # pragma: no cover - optional dependency
    from sentence_transformers import util
except Exception:  # pragma: no cover - fallback
    util = None  # type: ignore

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
        rows = await db.fetch("SELECT market_id, embedding FROM market WHERE embedding IS NOT NULL")
        vectors = []
        market_ids = []
        for row in rows:
            embedding = row["embedding"]
            if embedding is None:
                continue
            if np is not None:
                vectors.append(np.array(embedding))
            else:
                vectors.append(embedding)
            market_ids.append(row["market_id"])

        groups: list[dict] = []
        member_to_group: dict[str, int] = {}
        threshold = float(self.config.get("threshold", 0.75))
        min_size = int(self.config.get("min_size", 2))
        if vectors and np is not None and util is not None:
            communities = util.community_detection(vectors, threshold=threshold, min_community_size=min_size)
            for idx, community in enumerate(communities, start=1):
                members = sorted({market_ids[i] for i in community})
                if len(members) < 2:
                    continue
                groups.append({"name": f"Auto Cluster {idx}", "method": "embedding", "members": members})
                for member in members:
                    member_to_group[member] = len(groups) - 1
        elif np is None or util is None:
            self.logger.warning(
                "community-detection-disabled",
                extra={"reason": "missing-numpy-or-sentence-transformers"},
            )

        for entry in self.config.get("groups", []):
            explicit = entry.get("explicit", []) or []
            if not explicit:
                continue
            target_idx = None
            for market_id in explicit:
                if market_id in member_to_group:
                    target_idx = member_to_group[market_id]
                    break
            if target_idx is None:
                target_idx = len(groups)
                groups.append(
                    {
                        "name": entry.get("name", f"Manual Cluster {target_idx+1}"),
                        "method": entry.get("method", "manual"),
                        "members": [],
                    }
                )
            else:
                groups[target_idx]["name"] = entry.get("name", groups[target_idx]["name"])
            for market_id in explicit:
                current_idx = member_to_group.get(market_id)
                if current_idx is None:
                    groups[target_idx]["members"].append(market_id)
                    member_to_group[market_id] = target_idx
                elif current_idx != target_idx:
                    groups[target_idx]["members"].extend(groups[current_idx]["members"])
                    for mid in groups[current_idx]["members"]:
                        member_to_group[mid] = target_idx
                    groups[current_idx]["members"] = []

        normalized_groups: list[dict] = []
        for group in groups:
            members = sorted({m for m in group["members"] if m})
            if len(members) < 2:
                continue
            normalized_groups.append({**group, "members": members})

        await self._sync_db(db, normalized_groups)
        return normalized_groups

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
