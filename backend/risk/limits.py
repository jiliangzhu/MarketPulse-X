from __future__ import annotations

from dataclasses import dataclass
from typing import List

from backend.repo import execution_repo


@dataclass
class LimitResult:
    ok: bool
    reasons: List[str]


async def evaluate_limits(
    db,
    *,
    qty: float,
    limit_price: float,
    settings,
) -> LimitResult:
    reasons: list[str] = []
    notional = qty * limit_price
    if notional > settings.exec_max_notional_per_order:
        reasons.append("per-order notional exceeded")
    open_orders = await execution_repo.open_intents_count(db)
    if open_orders >= settings.exec_max_concurrent_orders:
        reasons.append("max concurrent intents reached")
    day_notional = await execution_repo.daily_notional(db)
    if day_notional + notional > settings.exec_max_daily_notional:
        reasons.append("daily notional cap reached")
    return LimitResult(ok=not reasons, reasons=reasons)
