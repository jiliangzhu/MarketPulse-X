from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.db import Database


async def record_kpi(
    db: Database,
    *,
    rule_type: str,
    level: str,
    gap: float | None = None,
    est_edge_bps: float | None = None,
) -> None:
    today = datetime.utcnow().date()
    await db.execute(
        """
        INSERT INTO rule_kpi_daily (day, rule_type, signals, p1_signals, avg_gap, est_edge_bps)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (day, rule_type)
        DO UPDATE SET
            signals = rule_kpi_daily.signals + EXCLUDED.signals,
            p1_signals = rule_kpi_daily.p1_signals + EXCLUDED.p1_signals,
            avg_gap = COALESCE((rule_kpi_daily.avg_gap + EXCLUDED.avg_gap)/2, rule_kpi_daily.avg_gap, EXCLUDED.avg_gap),
            est_edge_bps = COALESCE((rule_kpi_daily.est_edge_bps + EXCLUDED.est_edge_bps)/2, rule_kpi_daily.est_edge_bps, EXCLUDED.est_edge_bps)
        """,
        today,
        rule_type,
        1,
        1 if level == "P1" else 0,
        gap,
        est_edge_bps,
    )
