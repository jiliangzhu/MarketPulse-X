from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict

from backend.repo import ticks_repo
from backend.processing.rules_engine import _to_float


@dataclass
class GuardrailResult:
    ok: bool
    reason: str | None = None


async def evaluate_guardrails(
    db,
    market_id: str,
    *,
    option_id: str,
    side: str,
    limit_price: float,
    slippage_bps: int,
) -> GuardrailResult:
    """针对具体 option 做滑点校验，避免使用市场其他腿的高价/低价作为参考。"""
    getcontext().prec = 18
    latest = await ticks_repo.latest_ticks_by_market(db, market_id)
    if not latest:
        return GuardrailResult(ok=False, reason="no market depth")
    tick = latest.get(option_id)
    if not tick:
        return GuardrailResult(ok=False, reason="option depth missing")
    ref_price = Decimal(str(_to_float(tick.get("price"))))
    best_bid = Decimal(str(_to_float(tick.get("best_bid"))))
    best_ask = Decimal(str(_to_float(tick.get("best_ask"))))
    if ref_price <= 0 and best_bid > 0 and best_ask > 0:
        ref_price = (best_bid + best_ask) / Decimal("2")
    if ref_price <= 0:
        return GuardrailResult(ok=False, reason="invalid reference price")
    allowed_slip = ref_price * Decimal(str(slippage_bps)) / Decimal("10000")
    limit = Decimal(str(limit_price))
    if side == "buy" and limit > ref_price + allowed_slip:
        return GuardrailResult(ok=False, reason="slippage too high")
    if side == "sell" and limit < ref_price - allowed_slip:
        return GuardrailResult(ok=False, reason="slippage too high")
    return GuardrailResult(ok=True)
