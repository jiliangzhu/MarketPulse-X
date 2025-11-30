from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query

from backend.db import Database
from backend.deps import get_db, get_app_settings, require_admin_token
from backend.repo import signals_repo
from backend.schemas import RuleUploadSchema, SignalSchema
from backend.settings import Settings
from backend.utils.rules import validate_rule_payload

router = APIRouter(prefix="/api", tags=["signals"])


@router.get("/signals", response_model=list[SignalSchema])
async def list_signals(
    level: Optional[str] = Query(default=None),
    since: Optional[datetime] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
) -> list[SignalSchema]:
    rows = await signals_repo.fetch_signals(db, level=level, since=since, limit=limit, offset=offset)
    return [SignalSchema(**row) for row in rows]


@router.post("/rules", dependencies=[Depends(require_admin_token)])
async def upload_rule(
    payload: RuleUploadSchema,
    db: Database = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
):
    rule_yaml = payload.dsl
    rule_dict = validate_rule_payload(rule_yaml, settings.rule_payload_max_bytes)
    rule_dict["raw_yaml"] = rule_yaml
    rule_id = await signals_repo.upsert_rule_def(db, rule_dict)
    await signals_repo.insert_audit(
        db,
        actor="api",
        action="upload_rule",
        target_id=str(rule_id),
        meta_json={"name": payload.name},
    )
    return {"rule_id": rule_id}
