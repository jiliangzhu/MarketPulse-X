from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pydantic import BaseModel

from backend.db import Database
from backend.deps import get_app_settings, get_db
from backend.execution import oems
from backend.execution.executor import Executor
from backend.repo import markets_repo, signals_repo, ticks_repo
from backend.settings import Settings
from backend.utils.logging import get_logger

router = APIRouter(prefix="/api/execution", tags=["execution"])
logger = get_logger("execution_router")


class IntentRequest(BaseModel):
    signal_id: int
    side: Optional[str] = None
    qty_override: Optional[float] = None
    limit_price_override: Optional[float] = None
    ttl_secs: int = 60


class IntentConfirmResponse(BaseModel):
    intent_id: int
    status: str
    detail_json: dict | None = None


async def _signal_payload(db: Database, signal_id: int) -> dict:
    row = await signals_repo.get_signal(db, signal_id)
    if not row:
        raise HTTPException(status_code=404, detail="signal not found")
    return row


@router.post("/intent", response_model=IntentConfirmResponse)
async def create_intent(
    payload: IntentRequest,
    db: Database = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
    token: Optional[str] = Header(default=None, alias="x-api-key"),
):
    # 简单的 admin token 校验
    if settings.admin_api_token and token != settings.admin_api_token:
        raise HTTPException(status_code=401, detail="invalid token")
    request_payload = payload
    signal = await _signal_payload(db, request_payload.signal_id)
    # 信号时效校验，默认 60s 内有效
    created_at = signal.get("created_at")
    if created_at:
        age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
        if age_seconds > 60:
            raise HTTPException(status_code=400, detail="signal expired")
    if signal.get("level") not in {"P1", "P2"}:
        raise HTTPException(status_code=400, detail="signal level too low")
    market_id = signal["market_id"]
    latest = await ticks_repo.latest_ticks_by_market(db, market_id)
    if not latest:
        raise HTTPException(status_code=400, detail="market has no liquidity")
    top_tick = max(latest.values(), key=lambda x: x.get("price") or 0)
    rule_type = (signal.get("payload_json") or {}).get("rule_type")
    signal_payload = signal.get("payload_json") or {}
    trade_plan_hint = signal_payload.get("suggested_trade") or {}
    legs_hint = trade_plan_hint.get("legs") or []
    primary_leg = legs_hint[0] if legs_hint else None
    qty = request_payload.qty_override or (float(primary_leg.get("qty") or 1) if primary_leg else 1)
    ref_price = float(top_tick.get("price") or 0.5)
    inferred_leg_price = None
    if primary_leg:
        inferred_leg_price = float(primary_leg.get("limit_price") or primary_leg.get("reference_price") or ref_price)
    limit_price = request_payload.limit_price_override or inferred_leg_price or ref_price
    side = request_payload.side or (primary_leg.get("side") if primary_leg else ("buy" if signal.get("level") == "P1" else "sell"))
    # 根据风控滑点对价格做预夹
    allowed_slip = ref_price * (settings.exec_slippage_bps / 10000)
    if rule_type == "ENDGAME_SWEEP":
        qty = request_payload.qty_override or 1
        limit_price = request_payload.limit_price_override or min(0.99, ref_price)
        side = "buy"
    # 最终根据滑点做钳制，避免确认阶段被 guardrail 拒绝
    if side == "buy":
        limit_price = min(limit_price, ref_price + allowed_slip)
    else:
        limit_price = max(limit_price, ref_price - allowed_slip)
    policy_id = await oems.bootstrap_policy(db, settings)
    signal_payload = signal.get("payload_json") or {}
    detail_json = {
        "signal_level": signal.get("level"),
        "rule": signal_payload.get("rule_name"),
        "rule_type": signal_payload.get("rule_type"),
        "transport": signal_payload.get("transport"),
        "edge_score": signal_payload.get("edge_score"),
        "estimated_edge_bps": signal_payload.get("estimated_edge_bps"),
        "payload": signal_payload,
        "trade_plan_hint": trade_plan_hint or None,
        "primary_option_id": primary_leg.get("option_id") if primary_leg else None,
    }
    intent = await oems.create_suggested_intent(
        db,
        {
            "signal_id": request_payload.signal_id,
            "market_id": market_id,
            "side": side,
            "qty": qty,
            "limit_price": limit_price,
            "option_id": primary_leg.get("option_id") if primary_leg else None,
            "ttl_secs": request_payload.ttl_secs,
            "status": "suggested",
            "policy_id": policy_id,
            "detail_json": detail_json,
        },
    )
    return IntentConfirmResponse(intent_id=intent["intent_id"], status=intent["status"], detail_json=detail_json)


@router.post("/confirm/{intent_id}", response_model=IntentConfirmResponse)
async def confirm_intent(
    intent_id: int,
    db: Database = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
):
    intents = await oems.list_intents(db)
    target = next((intent for intent in intents if intent["intent_id"] == intent_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="intent not found")
    executor = Executor(db, settings)
    result = await executor.confirm_and_execute(target)
    await oems.mark_status(db, intent_id, result["status"], result.get("detail_json"))
    return IntentConfirmResponse(intent_id=intent_id, status=result["status"], detail_json=result.get("detail_json"))


class IntentListResponse(BaseModel):
    items: list[dict]


@router.get("/intents", response_model=IntentListResponse)
async def list_intents(
    status: Optional[str] = Query(default=None),
    db: Database = Depends(get_db),
):
    intents = await oems.list_intents(db, status=status)
    return IntentListResponse(items=intents)
