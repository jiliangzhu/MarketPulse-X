from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
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
    parser.add_argument("--initial-cash", type=float, default=10_000.0, help="Starting portfolio cash")
    parser.add_argument("--csv-out", type=Path, default=Path("results.csv"), help="Results CSV output path")
    return parser.parse_args()


class Portfolio:
    def __init__(self, initial_cash: float) -> None:
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[Tuple[str, str], Dict[str, float]] = {}
        self.history: List[dict[str, Any]] = []
        self.equity_curve: List[dict[str, Any]] = []

    def execute_signal(self, signal: dict[str, Any]) -> None:
        option_id = signal.get("option_id")
        if not option_id:
            return
        market_id = signal["market_id"]
        tick = signal.get("tick") or {}
        price = self._execution_price(tick, signal.get("side", "buy"))
        if price is None:
            return
        qty = float(signal.get("qty", 1.0))
        side = signal.get("side", "buy")
        key = (market_id, option_id)
        qty_change = qty if side == "buy" else -qty
        realized = self._apply_trade(key, qty_change, price)
        if side == "buy":
            self.cash -= price * qty
        else:
            self.cash += price * qty
        current_qty = self.positions.get(key, {}).get("qty", 0.0)
        self.history.append(
            {
                "ts": signal["ts"],
                "market_id": market_id,
                "option_id": option_id,
                "side": side,
                "qty": qty,
                "price": price,
                "cash": self.cash,
                "realized_pnl": realized,
                "position_qty": current_qty,
            }
        )

    def mark_to_market(self, ts: datetime, quotes: Dict[Tuple[str, str], dict]) -> None:
        total = self.cash
        for key, pos in list(self.positions.items()):
            row = quotes.get(key)
            if not row:
                mark_price = pos["avg_price"]
            else:
                if pos["qty"] >= 0:
                    mark_price = row.get("best_bid") or row.get("price")
                else:
                    mark_price = row.get("best_ask") or row.get("price")
                if mark_price is None:
                    mark_price = pos["avg_price"]
            total += pos["qty"] * float(mark_price or 0)
        self.equity_curve.append({"ts": ts, "equity": total})

    def _execution_price(self, tick: dict, side: str) -> Optional[float]:
        if side == "buy":
            return self._to_float(tick.get("best_ask")) or self._to_float(tick.get("price"))
        return self._to_float(tick.get("best_bid")) or self._to_float(tick.get("price"))

    def _apply_trade(self, key: Tuple[str, str], qty_change: float, price: float) -> float:
        pos = self.positions.get(key, {"qty": 0.0, "avg_price": 0.0})
        realized = 0.0
        if pos["qty"] > 0 and qty_change < 0:
            closing = min(pos["qty"], -qty_change)
            realized += (price - pos["avg_price"]) * closing
            pos["qty"] -= closing
            qty_change += closing
        elif pos["qty"] < 0 and qty_change > 0:
            closing = min(-pos["qty"], qty_change)
            realized += (pos["avg_price"] - price) * closing
            pos["qty"] += closing
            qty_change -= closing
        if pos["qty"] == 0 and qty_change == 0:
            self.positions.pop(key, None)
            return realized
        if qty_change != 0:
            new_qty = pos["qty"] + qty_change
            if pos["qty"] == 0:
                pos = {"qty": qty_change, "avg_price": price}
            else:
                pos["avg_price"] = (pos["avg_price"] * pos["qty"] + price * qty_change) / new_qty
                pos["qty"] = new_qty
            self.positions[key] = pos
        else:
            if pos["qty"] == 0:
                self.positions.pop(key, None)
            else:
                self.positions[key] = pos
        return realized

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


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
    portfolio = Portfolio(args.initial_cash)
    quotes: Dict[Tuple[str, str], dict[str, Any]] = {}
    last_ts: datetime | None = None
    for row in rows:
        market_id = row["market_id"]
        if market_id not in option_cache:
            option_cache[market_id] = await markets_repo.list_options(db, market_id)
        market_state = state[market_id]
        market_state["latest"][row["option_id"]] = dict(row)
        recent: Deque[dict[str, Any]] = market_state["recent"]
        recent.appendleft(dict(row))
        quotes[(market_id, row["option_id"])] = dict(row)
        for rule in engine.rules:
            payload = await engine._evaluate_rule(
                rule,
                market_map.get(market_id, {"market_id": market_id, "title": market_id, "status": "active"}),
                market_state["latest"],
                list(recent),
                option_cache[market_id],
            )
            if payload:
                signal = {
                    "ts": row["ts"],
                    "market_id": market_id,
                    "option_id": payload.get("option_id"),
                    "payload": payload,
                    "rule": rule,
                    "side": infer_side(rule, payload),
                    "qty": payload.get("qty", 1.0),
                    "tick": dict(row),
                }
                portfolio.execute_signal(signal)
                logger.info(
                    "backtest-signal",
                    extra={"market_id": market_id, "rule": rule.type, "side": signal["side"]},
                )
        if args.speed and last_ts is not None:
            delta = (row["ts"] - last_ts).total_seconds() / max(args.speed, 0.1)
            if delta > 0:
                await asyncio.sleep(min(delta, 2))
        last_ts = row["ts"]
        portfolio.mark_to_market(row["ts"], quotes)
    report = build_report(portfolio)
    print_report(report)
    if args.csv_out:
        export_results(args.csv_out, portfolio)
    logger.info("backtest-complete", extra={"trades": len(portfolio.history)})
    await notifier.aclose()
    await db.disconnect()


def infer_side(rule: Rule, payload: dict[str, Any]) -> str:
    if payload.get("direction"):
        return payload["direction"].lower()
    rule_type = rule.type
    if "pct_change" in payload:
        return "buy" if payload["pct_change"] >= 0 else "sell"
    if "delta" in payload:
        return "buy" if payload["delta"] >= 0 else "sell"
    if rule_type in {"SPIKE_DETECT"} and payload.get("window_secs"):
        return "buy" if payload.get("pct_change", 0) >= 0 else "sell"
    if rule_type in {"TREND_BREAKOUT"}:
        return "buy" if payload.get("delta", 0) >= 0 else "sell"
    return "buy"


def build_report(portfolio: Portfolio) -> dict[str, Any]:
    equity_df = pd.DataFrame(portfolio.equity_curve)
    equity_df.set_index("ts", inplace=True)
    total_equity = equity_df["equity"].iloc[-1] if not equity_df.empty else portfolio.initial_cash
    pnl = total_equity - portfolio.initial_cash
    total_return = pnl / portfolio.initial_cash if portfolio.initial_cash else 0
    equity_df["rolling_max"] = equity_df["equity"].cummax()
    equity_df["drawdown"] = equity_df["equity"] / equity_df["rolling_max"] - 1
    max_drawdown = equity_df["drawdown"].min() if not equity_df.empty else 0
    returns = equity_df["equity"].pct_change().dropna()
    if not returns.empty and returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * np.sqrt(max(len(returns), 1))
    else:
        sharpe = 0.0

    trades_df = pd.DataFrame(portfolio.history)
    closed = trades_df[trades_df["realized_pnl"] != 0] if not trades_df.empty else pd.DataFrame()
    wins = closed[closed["realized_pnl"] > 0]
    losses = closed[closed["realized_pnl"] < 0]
    win_rate = len(wins) / len(closed) if len(closed) else 0
    profit_loss_ratio = (
        wins["realized_pnl"].mean() / abs(losses["realized_pnl"].mean())
        if len(wins) and len(losses)
        else float("inf") if len(wins) else 0
    )
    return {
        "total_pnl": pnl,
        "total_return_pct": total_return * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe_ratio": sharpe,
        "win_rate_pct": win_rate * 100,
        "profit_loss_ratio": profit_loss_ratio,
        "total_trades": len(trades_df),
    }


def print_report(report: dict[str, Any]) -> None:
    print("=== Backtest Report ===")
    print(f"Total PnL: {report['total_pnl']:.2f}")
    print(f"Total Return: {report['total_return_pct']:.2f}%")
    print(f"Max Drawdown: {report['max_drawdown_pct']:.2f}%")
    print(f"Sharpe Ratio: {report['sharpe_ratio']:.2f}")
    print(f"Win Rate: {report['win_rate_pct']:.2f}%")
    print(f"Profit/Loss Ratio: {report['profit_loss_ratio']:.2f}")
    print(f"Total Trades: {report['total_trades']}")


def export_results(path: Path, portfolio: Portfolio) -> None:
    trades_df = pd.DataFrame(portfolio.history)
    equity_df = pd.DataFrame(portfolio.equity_curve)
    with path.open("w", encoding="utf-8") as fh:
        fh.write("# Trades\n")
        trades_df.to_csv(fh, index=False)
        fh.write("\n# EquityCurve\n")
        equity_df.to_csv(fh, index=False)


if __name__ == "__main__":
    asyncio.run(run_backtest(parse_args()))
