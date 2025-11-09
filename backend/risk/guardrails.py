from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from backend.repo import ticks_repo
from backend.processing.rules_engine import _to_float


@dataclass
class GuardrailResult:
    ok: bool
    reason: str | None = None


async def evaluate_guardrails(db, market_id: str, *, side: str, limit_price: float, slippage_bps: int) -> GuardrailResult:
    latest = await ticks_repo.latest_ticks_by_market(db, market_id)
    if not latest:
        return GuardrailResult(ok=False, reason="no market depth")
    ref_price = max((_to_float(tick.get("price")) for tick in latest.values()))
    if ref_price <= 0:
        return GuardrailResult(ok=False, reason="invalid reference price")
    allowed_slip = ref_price * (slippage_bps / 10000)
    if side == "buy" and limit_price > ref_price + allowed_slip:
        return GuardrailResult(ok=False, reason="slippage too high")
    if side == "sell" and limit_price < ref_price - allowed_slip:
        return GuardrailResult(ok=False, reason="slippage too high")
    return GuardrailResult(ok=True)
