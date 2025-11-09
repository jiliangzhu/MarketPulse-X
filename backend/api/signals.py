from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Query

from backend.db import Database
from backend.deps import get_db, get_app_settings
from backend.repo import signals_repo
from backend.schemas import RuleUploadSchema, SignalSchema
from backend.settings import Settings
import yaml

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


@router.post("/rules")
async def upload_rule(
    payload: RuleUploadSchema,
    request: Request,
    db: Database = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
):
    token = request.headers.get("x-api-key")
    if settings.admin_api_token and token != settings.admin_api_token:
        raise HTTPException(status_code=401, detail="invalid token")
    rule_yaml = payload.dsl
    rule_dict = yaml.safe_load(rule_yaml)
    rule_dict = rule_dict or {}
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
