from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends

from backend.db import Database
from backend.deps import get_db

router = APIRouter(prefix="/api/kpi", tags=["kpi"])


@router.get("/daily")
async def daily_kpi(db: Database = Depends(get_db)):
    start = date.today() - timedelta(days=7)
    rows = await db.fetch(
        """
        SELECT day, rule_type, signals, p1_signals, avg_gap, est_edge_bps
        FROM rule_kpi_daily
        WHERE day >= $1
        ORDER BY day DESC, rule_type
        """,
        start,
    )
    return [dict(r) for r in rows]
