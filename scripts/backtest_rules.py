from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List

from backend.db import Database
from backend.processing.rules_engine import RulesEngine, Rule
from backend.repo import markets_repo
from backend.settings import get_settings
from backend.utils.logging import configure_logging, get_logger


class DummyNotifier:
    async def send_message(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - simple stub
        return "dry-run"

    async def aclose(self) -> None:  # pragma: no cover - simple stub
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest MarketPulse-X rules")
    parser.add_argument("--rule", required=True, help="Rule type, e.g. SUM_LT_1")
    parser.add_argument("--start", required=True, help="Start timestamp, e.g. 2024-01-01T00:00:00Z")
    parser.add_argument("--end", required=True, help="End timestamp")
    parser.add_argument("--speed", type=float, default=0, help="Replay speed multiplier (0 disables sleep)")
    parser.add_argument("--csv-out", type=Path, default=None, help="Optional CSV output path")
    return parser.parse_args()


async def run_backtest(args: argparse.Namespace) -> None:
    configure_logging()
    logger = get_logger("backtest")
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    notifier = DummyNotifier()
    engine = RulesEngine(db, notifier, settings, settings.config_rules_path)
    await engine.load_rules()
    engine.rules = [rule for rule in engine.rules if rule.type == args.rule]
    if not engine.rules:
        raise ValueError(f"Rule {args.rule} not found")
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
    rows = await db.fetch(
        """
        SELECT ts, market_id, option_id, price, volume, best_bid, best_ask, liquidity
        FROM tick
        WHERE ts BETWEEN $1 AND $2
        ORDER BY ts
        """,
        start,
        end,
    )
    markets = await markets_repo.list_markets(db, status=None, limit=500)
    market_map = {m["market_id"]: m for m in markets}
    option_cache: Dict[str, List[dict]] = {}
    state: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"latest": {}, "recent": deque(maxlen=300)})
    hits: list[dict[str, Any]] = []
    last_ts: datetime | None = None
    for row in rows:
        market_id = row["market_id"]
        if market_id not in option_cache:
            option_cache[market_id] = await markets_repo.list_options(db, market_id)
        market_state = state[market_id]
        market_state["latest"][row["option_id"]] = dict(row)
        recent: Deque[dict[str, Any]] = market_state["recent"]
        recent.appendleft(dict(row))
        for rule in engine.rules:
            payload = await engine._evaluate_rule(
                rule,
                market_map.get(market_id, {"market_id": market_id, "title": market_id, "status": "active"}),
                market_state["latest"],
                list(recent),
                option_cache[market_id],
            )
            if payload:
                hits.append(
                    {
                        "ts": row["ts"],
                        "market_id": market_id,
                        "message": payload["message"],
                        "score": payload["score"],
                    }
                )
                logger.info("backtest-hit", extra={"market_id": market_id, "score": payload["score"]})
        if args.speed and last_ts is not None:
            delta = (row["ts"] - last_ts).total_seconds() / max(args.speed, 0.1)
            if delta > 0:
                await asyncio.sleep(min(delta, 2))
        last_ts = row["ts"]
    if args.csv_out:
        import csv

        with args.csv_out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["ts", "market_id", "message", "score"])
            writer.writeheader()
            writer.writerows(hits)
    logger.info("backtest-complete", extra={"hits": len(hits)})
    await notifier.aclose()
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(run_backtest(parse_args()))
