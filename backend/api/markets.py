from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.deps import get_db
from backend.repo import markets_repo, ticks_repo
from backend.schemas import MarketDetailSchema, MarketOptionSchema, MarketSummarySchema
from backend.db import Database

router = APIRouter(prefix="/api/markets", tags=["markets"])


@router.get("", response_model=list[MarketSummarySchema])
async def list_markets(
    request: Request,
    status: Optional[str] = Query(default="active"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
) -> list[MarketSummarySchema]:
    markets = await markets_repo.list_markets(db, status=status, limit=limit, offset=offset)
    summaries: list[MarketSummarySchema] = []
    for market in markets:
        latest = await ticks_repo.latest_ticks_by_market(db, market["market_id"])
        options_meta = await markets_repo.list_options(db, market["market_id"])
        options = [
            MarketOptionSchema(
                option_id=opt_meta["option_id"],
                label=opt_meta.get("label", opt_meta["option_id"]),
                last_price=latest.get(opt_meta["option_id"], {}).get("price"),
                last_ts=latest.get(opt_meta["option_id"], {}).get("ts"),
            )
            for opt_meta in options_meta
        ]
        last_updated = None
        if latest:
            last_updated = max((row.get("ts") for row in latest.values() if row.get("ts")), default=None)
        summaries.append(
            MarketSummarySchema(
                market_id=market["market_id"],
                title=market["title"],
                status=market["status"],
                ends_at=market.get("ends_at"),
                tags=market.get("tags") or [],
                options=options,
                last_updated=last_updated,
            )
        )
    return summaries


@router.get("/{market_id}", response_model=MarketDetailSchema)
async def get_market_detail(
    market_id: str,
    request: Request,
    db: Database = Depends(get_db),
) -> MarketDetailSchema:
    market = await markets_repo.get_market(db, market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    options_meta = await markets_repo.list_options(db, market_id)
    latest = await ticks_repo.latest_ticks_by_market(db, market_id)
    sparkline = await ticks_repo.recent_ticks(db, market_id, minutes=5, limit=200)
    synonyms = await markets_repo.synonym_peers(db, market_id)
    option_map = {
        opt["option_id"]: MarketOptionSchema(
            option_id=opt["option_id"],
            label=opt["label"],
            last_price=latest.get(opt["option_id"], {}).get("price"),
            last_ts=latest.get(opt["option_id"], {}).get("ts"),
        )
        for opt in options_meta
    }
    return MarketDetailSchema(
        market_id=market_id,
        title=market["title"],
        status=market["status"],
        ends_at=market.get("ends_at"),
        tags=market.get("tags") or [],
        options=list(option_map.values()),
        sparkline=sparkline,
        synonyms=synonyms,
    )
