from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from decimal import Decimal
import json

from backend.db import Database


async def upsert_rule_def(db: Database, rule: dict[str, Any]) -> int:
    query = """
        INSERT INTO rule_def (name, type, dsl_yaml, enabled, version)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (name)
        DO UPDATE SET type = EXCLUDED.type,
                      dsl_yaml = EXCLUDED.dsl_yaml,
                      enabled = EXCLUDED.enabled,
                      version = rule_def.version + 1
        RETURNING rule_id
    """
    row = await db.fetchrow(
        query,
        rule.get("name"),
        rule.get("type"),
        rule.get("raw_yaml", ""),
        rule.get("enabled", True),
        rule.get("version", 1),
    )
    return int(row["rule_id"])


async def insert_signal(db: Database, signal: dict[str, Any]) -> int:
    query = """
        INSERT INTO signal (market_id, option_id, rule_id, level, score, payload_json, edge_score, source, confidence, ml_features, reason)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        RETURNING signal_id
    """
    payload = _json_dump(signal.get("payload_json"))
    features_json = _json_dump(signal.get("ml_features"))
    row = await db.fetchrow(
        query,
        signal.get("market_id"),
        signal.get("option_id"),
        signal.get("rule_id"),
        signal.get("level"),
        signal.get("score"),
        payload,
        signal.get("edge_score"),
        signal.get("source", "rule"),
        signal.get("confidence"),
        features_json,
        signal.get("reason"),
    )
    return int(row["signal_id"])


async def fetch_signals(
    db: Database,
    *,
    level: Optional[str] = None,
    since: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[dict[str, Any]]:
    query = "SELECT signal_id, market_id, option_id, level, score, payload_json, edge_score, created_at, source, confidence, ml_features, reason FROM signal"
    clauses: list[str] = []
    params: list[Any] = []
    if level:
        params.append(level)
        clauses.append(f"level = ${len(params)}")
    if since:
        params.append(since)
        clauses.append(f"created_at >= ${len(params)}")
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    params.append(limit)
    query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
    if offset:
        params.append(offset)
        query += f" OFFSET ${len(params)}"
    rows = await db.fetch(query, *params)
    result: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        data["payload_json"] = _json_load(data.get("payload_json"))
        data["ml_features"] = _json_load(data.get("ml_features"))
        result.append(data)
    return result


async def insert_audit(
    db: Database,
    *,
    actor: str,
    action: str,
    target_id: Optional[str] = None,
    meta_json: Optional[dict[str, Any]] = None,
) -> None:
    query = """
        INSERT INTO audit_log (actor, action, target_id, meta_json, ts)
        VALUES ($1,$2,$3,$4,$5)
    """
    meta_payload = _json_dump(meta_json)
    await db.execute(query, actor, action, target_id, meta_payload, datetime.now(timezone.utc))


async def get_signal(db: Database, signal_id: int) -> dict[str, Any] | None:
    row = await db.fetchrow(
        """
        SELECT signal_id, market_id, option_id, level, score, payload_json, edge_score, created_at, source, confidence, ml_features, reason
        FROM signal
        WHERE signal_id = $1
        """,
        signal_id,
    )
    if not row:
        return None
    data = dict(row)
    data["payload_json"] = _json_load(data.get("payload_json"))
    data["ml_features"] = _json_load(data.get("ml_features"))
    return data

def _json_dump(value: Any) -> str:
    return json.dumps(value or {}, default=_json_default)


def _json_default(obj: Any):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Unserializable type: {type(obj)!r}")


def _json_load(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
