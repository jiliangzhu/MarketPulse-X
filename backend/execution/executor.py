from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.risk import guardrails, limits
from backend.utils.logging import get_logger


@dataclass
class ExecutionContext:
    qty: float
    limit_price: float
    side: str
    market_id: str
    option_id: str | None
    policy_id: int


class Executor:
    def __init__(self, db, settings) -> None:
        self.db = db
        self.settings = settings
        self.logger = get_logger("executor")

    async def validate(self, ctx: ExecutionContext) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        limit_result = await limits.evaluate_limits(
            self.db,
            qty=ctx.qty,
            limit_price=ctx.limit_price,
            settings=self.settings,
        )
        if not limit_result.ok:
            reasons.extend(limit_result.reasons)
        option_id = ctx.option_id
        if not option_id:
            reasons.append("missing option for guardrail")
            return False, reasons
        guardrail_result = await guardrails.evaluate_guardrails(
            self.db,
            ctx.market_id,
            option_id=option_id,
            side=ctx.side,
            limit_price=ctx.limit_price,
            slippage_bps=self.settings.exec_slippage_bps,
        )
        if not guardrail_result.ok and guardrail_result.reason:
            reasons.append(guardrail_result.reason)
        return not reasons, reasons

    async def confirm_and_execute(self, intent: dict[str, Any]) -> dict[str, Any]:
        ctx = ExecutionContext(
            qty=float(intent["qty"]),
            limit_price=float(intent.get("limit_price") or 0),
            side=intent["side"],
            market_id=intent["market_id"],
            option_id=intent.get("option_id") or (intent.get("detail_json") or {}).get("primary_option_id"),
            policy_id=intent.get("policy_id"),
        )
        ok, reasons = await self.validate(ctx)
        detail = intent.get("detail_json", {})
        detail.setdefault("checks", {})
        detail["checks"].update({"reasons": reasons, "approved": ok})
        status = "sent" if ok else "rejected"
        if ok and self.settings.data_source == "mock":
            status = "filled"
        self.logger.info(
            "intent-confirm",
            extra={"intent_id": intent.get("intent_id"), "status": status, "market_id": ctx.market_id},
        )
        return {**intent, "status": status, "detail_json": detail}
