from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, getcontext
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
    conn=None,
) -> LimitResult:
    """在可选的事务连接内做额度校验，防止并发穿透。"""
    getcontext().prec = 18
    reasons: list[str] = []
    notional = float(Decimal(str(qty)) * Decimal(str(limit_price)))
    if notional > settings.exec_max_notional_per_order:
        reasons.append("per-order notional exceeded")
    try:
        if conn:
            await conn.execute("SELECT pg_advisory_xact_lock(42);")
    except Exception:
        # 锁失败时仍继续检查，但会失去并发保护
        pass
    open_orders = await execution_repo.open_intents_count(db, conn=conn)
    if open_orders >= settings.exec_max_concurrent_orders:
        reasons.append("max concurrent intents reached")
    day_notional = await execution_repo.daily_notional(db, conn=conn)
    if day_notional + notional > settings.exec_max_daily_notional:
        reasons.append("daily notional cap reached")
    return LimitResult(ok=not reasons, reasons=reasons)
