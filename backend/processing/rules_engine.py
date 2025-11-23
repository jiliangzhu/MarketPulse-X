from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from backend.alerting.notifier_telegram import TelegramNotifier
from backend.db import Database
from backend.metrics import ml_inference_ms, rule_eval_ms, signals_counter
from backend.ingestion.source_binance import BinancePriceCache
from backend.ml.features import extract_features_realtime
from backend.ml.inference import MLModel
from backend.processing import scoring
from backend.processing.synonym_matcher import SynonymMatcher
from backend.repo import kpi_repo, markets_repo, signals_repo, ticks_repo
from backend.risk.circuit_breaker import CircuitBreaker
from backend.settings import Settings
from backend.utils.logging import get_logger


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class Rule:
    name: str
    type: str
    config: dict[str, Any]
    rule_id: int


class RulesEngine:
    def __init__(
        self,
        db: Database,
        notifier: TelegramNotifier,
        settings: Settings,
        rules_dir: Path,
        interval_secs: float = 2.0,
    ) -> None:
        self.db = db
        self.notifier = notifier
        self.interval_secs = interval_secs
        self.rules_dir = rules_dir
        self.settings = settings
        self.rules: list[Rule] = []
        self.logger = get_logger("rules")
        self.last_run: Optional[datetime] = None
        self._cooldowns: dict[tuple[int, str], datetime] = {}
        self.synonym_matcher = SynonymMatcher(settings.config_synonyms_path)
        self.circuit_breaker = CircuitBreaker()
        self.ml_model: Optional[MLModel] = None
        self.ml_interval = settings.ml_inference_interval_secs
        self.last_ml_run = 0.0
        self._latest_snapshots: dict[str, dict[str, Any]] = {}
        self.binance_cache = BinancePriceCache.get_instance()
        try:
            self.binance_cache.ensure_running()
        except RuntimeError:
            self.logger.warning("binance-feed-skip", extra={"reason": "no-running-loop"})
        if self.settings.ml_enabled:
            try:
                self.ml_model = MLModel(self.settings.ml_model_path)
            except Exception as exc:  # pragma: no cover - fallback logging
                self.logger.error("ml-model-load-failed", extra={"error": str(exc)})
                self.ml_model = None

    async def load_rules(self) -> None:
        self.rules.clear()
        for path in sorted(self.rules_dir.glob("*.yaml")):
            raw_content = path.read_text(encoding="utf-8")
            rule_conf = yaml.safe_load(raw_content) or {}
            rule_conf["raw_yaml"] = raw_content
            rule_id = await signals_repo.upsert_rule_def(self.db, rule_conf)
            if not rule_conf.get("enabled", True):
                continue
            rule = Rule(
                name=rule_conf.get("name", path.stem),
                type=rule_conf.get("type", "UNKNOWN"),
                config=rule_conf,
                rule_id=rule_id,
            )
            self.rules.append(rule)
        await signals_repo.insert_audit(
            self.db,
            actor="rules_engine",
            action="rules_loaded",
            meta_json={"count": len(self.rules)},
        )
        self.logger.info("rules-loaded", extra={"count": len(self.rules)})

    async def run(self, app_state: Optional[Any] = None) -> None:
        if not self.rules:
            await self.load_rules()
        while True:
            await self.evaluate_once(app_state)
            await asyncio.sleep(self.interval_secs)

    async def evaluate_once(self, app_state: Optional[Any] = None) -> None:
        start = time.perf_counter()
        markets = await markets_repo.list_markets(self.db, status="active", limit=100)
        snapshots: dict[str, dict[str, Any]] = {}
        group_rules = [rule for rule in self.rules if rule.type == "CROSS_MARKET_MISPRICE"]
        rule_signals: list[tuple[Optional[Rule], str, dict[str, Any]]] = []
        for market in markets:
            if not self._is_market_enabled(market):
                continue
            market_id = market["market_id"]
            ticks = await ticks_repo.latest_ticks_by_market(self.db, market_id)
            recent = await ticks_repo.recent_ticks(self.db, market_id, minutes=5, limit=250)
            options = await markets_repo.list_options(self.db, market_id)
            synonym_ids = await markets_repo.synonym_peers(self.db, market_id)
            peer_entries: list[dict[str, Any]] = []
            for peer_id in synonym_ids:
                peer_ticks = snapshots.get(peer_id, {}).get("ticks")
                if not peer_ticks:
                    peer_ticks = await ticks_repo.latest_ticks_by_market(self.db, peer_id)
                if peer_ticks:
                    top_peer = max(peer_ticks.values(), key=lambda t: _to_float(t.get("price")))
                    peer_entries.append({"market_id": peer_id, "price": _to_float(top_peer.get("price"))})
            snapshots[market_id] = {
                "market": market,
                "ticks": ticks,
                "recent": recent,
                "options": options,
                "synonym_peers": peer_entries,
                "synonym_ids": synonym_ids,
            }
            for rule in self.rules:
                if rule.type == "CROSS_MARKET_MISPRICE":
                    continue
                if not self._market_in_scope(rule, market):
                    continue
                signal_payload = await self._evaluate_rule(rule, market, ticks, recent, options)
                if signal_payload:
                    rule_signals.append((rule, market_id, signal_payload))
        if group_rules:
            groups = await self.synonym_matcher.build_groups(self.db)
            for rule in group_rules:
                payloads = self._rule_cross_market(rule, groups, snapshots)
                for market_id, payload in payloads:
                    rule_signals.append((rule, market_id, payload))
        self._latest_snapshots = snapshots

        ml_signals: list[dict[str, Any]] = []
        if self.ml_model and (time.time() - self.last_ml_run >= self.ml_interval):
            self.last_ml_run = time.time()
            feature_rows: list[dict[str, Any]] = []
            market_refs: list[str] = []
            for market_id, snapshot in snapshots.items():
                features = extract_features_realtime(
                    snapshot["market"],
                    snapshot["ticks"],
                    snapshot["recent"],
                    snapshot.get("synonym_peers"),
                )
                if features:
                    feature_rows.append(features)
                    market_refs.append(market_id)
            if feature_rows:
                features_df = pd.DataFrame(feature_rows).fillna(0)
                infer_start = time.perf_counter()
                probabilities = self.ml_model.predict_proba_batch(features_df)
                ml_inference_ms.observe((time.perf_counter() - infer_start) * 1000)
                for market_id, features, probability in zip(market_refs, feature_rows, probabilities):
                    if probability >= self.settings.ml_confidence_threshold:
                        ml_signals.append(
                            {
                                "market_id": market_id,
                                "confidence": probability,
                                "ml_features": features,
                                "reason": f"ML confidence {probability*100:.1f}%",
                            }
                        )

        fused_signals = self._fuse_signals(rule_signals, ml_signals)
        for fused_payload in fused_signals:
            rule = fused_payload.pop("rule", None)
            market_id = fused_payload.pop("market_id")
            await self._emit_signal(rule, market_id, fused_payload)

        self.last_run = datetime.now(timezone.utc)
        if app_state is not None:
            app_state.rules_last_run = self.last_run
        rule_eval_ms.observe((time.perf_counter() - start) * 1000)

    async def _emit_signal(self, rule: Optional[Rule], market_id: str, signal_payload: dict[str, Any]) -> None:
        rule_name = rule.name if rule else signal_payload.get("source", "ML")
        if self.circuit_breaker.is_open(rule_name, market_id):
            return
        cooldown_conf = rule.config.get("dedupe", {}) if rule else {}
        cooldown_secs = cooldown_conf.get("cooldown_secs", 300)
        dedupe_key = (rule.rule_id if rule else -1, market_id)
        now = datetime.now(timezone.utc)
        last_fire = self._cooldowns.get(dedupe_key)
        if last_fire and (now - last_fire).total_seconds() < cooldown_secs:
            return
        self._cooldowns[dedupe_key] = now
        payload_json = signal_payload.get("payload") or {}
        snapshot_ctx = self._latest_snapshots.get(market_id) or {}
        market_title = (snapshot_ctx.get("market") or {}).get("title")
        if market_title and "market_title" not in payload_json:
            payload_json["market_title"] = market_title
        if rule:
            payload_json["rule_name"] = rule.name
            payload_json["rule_id"] = rule.rule_id
            payload_json["rule_type"] = rule.type
        else:
            payload_json["rule_name"] = signal_payload.get("source", "ML")
            payload_json["rule_type"] = "ML"
        extra_lines: list[str] = []
        trade_hint = payload_json.get("suggested_trade")
        if isinstance(trade_hint, dict):
            legs = trade_hint.get("legs") or []
            if legs:
                leg_bits = []
                for leg in legs[:3]:
                    leg_bits.append(
                        f"{leg.get('side', '?').upper()} "
                        f"{(leg.get('label') or leg.get('option_id'))}:{_to_float(leg.get('reference_price') or leg.get('limit_price')):.3f}"
                    )
                extra_lines.append(f"Trade {trade_hint.get('action', '')}: {' | '.join(leg_bits)}")
            rationale = trade_hint.get("rationale")
            if rationale:
                extra_lines.append(f"Plan: {rationale}")
        book_snapshot = payload_json.get("book_snapshot")
        if isinstance(book_snapshot, list) and book_snapshot:
            book_bits = [
                f"{entry.get('label') or entry.get('option_id')}:{_to_float(entry.get('price')):.3f}"
                for entry in book_snapshot[:3]
            ]
            extra_lines.append("Book: " + ", ".join(book_bits))
        if extra_lines:
            signal_payload["message"] = f"{signal_payload['message']}\n" + "\n".join(extra_lines)
        edge_score = signal_payload.get("edge_score")
        if edge_score is not None:
            payload_json["edge_score"] = edge_score
        transport_hint = "telegram"
        status = await self.notifier.send_message(
            signal_payload["message"],
            dedupe_key=f"{rule.rule_id if rule else 'ml'}:{market_id}",
            cooldown_secs=cooldown_secs,
        )
        if status != "sent":
            transport_hint = "telegram-dry-run"
            self.circuit_breaker.record_failure(rule_name, market_id)
        else:
            self.circuit_breaker.reset(rule_name, market_id)
        payload_json["transport"] = transport_hint
        score = signal_payload.get("score")
        if score is None:
            score = edge_score
        level = signal_payload.get("level")
        if level is None and rule:
            level = rule.config.get("outputs", {}).get("level", "P2")
        level = level or "P2"
        signal_id = await signals_repo.insert_signal(
            self.db,
            {
                "market_id": market_id,
                "option_id": signal_payload.get("option_id"),
                "rule_id": rule.rule_id if rule else None,
                "level": level,
                "score": score,
                "payload_json": payload_json,
                "edge_score": edge_score,
                "source": signal_payload.get("source", "rule"),
                "confidence": signal_payload.get("confidence"),
                "ml_features": signal_payload.get("ml_features"),
                "reason": signal_payload.get("reason"),
            },
        )
        await kpi_repo.record_kpi(
            self.db,
            rule_type=rule.type if rule else "ML",
            level=level,
            gap=payload_json.get("gap"),
            est_edge_bps=payload_json.get("estimated_edge_bps"),
        )
        await signals_repo.insert_audit(
            self.db,
            actor="rules_engine",
            action="signal_emitted",
            target_id=str(signal_id),
            meta_json={"rule": rule.name if rule else "ML", "market_id": market_id},
        )
        rule_type = rule.type if rule else "ML"
        source = signal_payload.get("source", "rule")
        signals_counter.labels(rule=rule_type, source=source).inc()

    async def _evaluate_rule(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if rule.type == "SPIKE_DETECT":
            return self._rule_spike(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "ENDGAME_SWEEP":
            return self._rule_endgame(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "DUTCH_BOOK_DETECT":
            return self._rule_dutch_book(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "CRYPTO_LEAD_LAG":
            return self._rule_crypto_lead_lag(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "TEMPORAL_ARBITRAGE":
            return self._rule_temporal_arbitrage(rule, market, latest_ticks, options_meta)
        if rule.type == "ORDER_BOOK_IMBALANCE":
            return self._rule_order_book_imbalance(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "CROSS_MARKET_MISPRICE":
            return None
        return None

    def _fuse_signals(
        self,
        rule_signals: list[tuple[Optional[Rule], str, dict[str, Any]]],
        ml_signals: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        fused: list[dict[str, Any]] = []
        rule_map: dict[str, tuple[Optional[Rule], dict[str, Any]]] = {}
        for rule, market_id, payload in rule_signals:
            existing = rule_map.get(market_id)
            if not existing or (payload.get("score", 0) or 0) > (existing[1].get("score", 0) or 0):
                rule_map[market_id] = (rule, payload)
        ml_map: dict[str, dict[str, Any]] = {entry["market_id"]: entry for entry in ml_signals}
        all_markets = set(rule_map.keys()) | set(ml_map.keys())
        for market_id in all_markets:
            rule_entry = rule_map.get(market_id)
            ml_entry = ml_map.get(market_id)
            reason_parts: list[str] = []
            edge = 0.0
            source_label = "rule"
            confidence = None
            ml_features = None
            rule_obj: Optional[Rule] = None
            if ml_entry:
                confidence = ml_entry.get("confidence")
                ml_features = ml_entry.get("ml_features")
                if confidence is not None:
                    edge += confidence * 100 * self.settings.ml_fusion_confidence_weight
                    reason_parts.append(ml_entry.get("reason", "ML confidence spike"))
                source_label = "ml"
            if rule_entry:
                rule_obj, payload = rule_entry
                edge += self.settings.ml_fusion_rule_bonus
                reason_parts.append(payload.get("message", payload.get("reason", "Rule signal")))
                fused_payload = payload.copy()
                trade_rationale = (payload.get("payload") or {}).get("suggested_trade", {}).get("rationale")
                if trade_rationale:
                    reason_parts.append(trade_rationale)
                if ml_entry:
                    source_label = "hybrid"
            else:
                fused_payload = {
                    "message": ml_entry.get("reason", "ML signal") if ml_entry else "Hybrid signal",
                    "score": edge,
                    "payload": {},
                    "level": "P2",
                }
            fused_payload["edge_score"] = edge
            fused_payload["score"] = fused_payload.get("score", edge)
            fused_payload["source"] = source_label
            fused_payload["confidence"] = confidence
            fused_payload["ml_features"] = ml_features
            fused_payload["reason"] = "; ".join(reason_parts) if reason_parts else fused_payload.get("reason")
            fused_payload["market_id"] = market_id
            fused_payload["rule"] = rule_obj
            fused.append(fused_payload)
        return fused

    def _rule_dutch_book(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not latest_ticks:
            return None
        params = rule.config.get("params", {})
        threshold = params.get("sum_price_lt", 0.995)
        min_liq = params.get("min_liquidity", 0.0)
        total = sum(_to_float(tick.get("price")) for tick in latest_ticks.values())
        min_liquidity = min(_to_float(tick.get("liquidity")) for tick in latest_ticks.values())
        if total >= threshold or min_liquidity < min_liq:
            return None
        edge = max(0.0, 1.0 - total)
        avg_spread = sum(
            max(0.0, _to_float(tick.get("best_ask")) - _to_float(tick.get("best_bid")))
            for tick in latest_ticks.values()
        ) / max(len(latest_ticks), 1)
        metrics = {
                "liquidity": min_liquidity / 10,
                "spread": 1 / max(avg_spread, 0.01),
                "edge": edge * 100,
            }
        score = scoring.compute_score(
            rule.config.get("outputs", {}).get("score", {}).get("base", 75),
            rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
            metrics,
        )
        legs = [
            {
                "option_id": option_id,
                "price": _to_float(tick.get("price")),
                "liquidity": _to_float(tick.get("liquidity")),
            }
            for option_id, tick in latest_ticks.items()
        ]
        message = self._format_message(
            rule,
            market,
            f"Dutch edge {edge*100:.2f}% (sum={total:.3f})",
        )
        book_snapshot = self._book_snapshot(options_meta, latest_ticks)
        trade_legs = [
            self._build_trade_leg(market["market_id"], item["option_id"], "buy", item["price"])
            for item in legs
        ]
        suggested_trade = self._trade_plan(
            "dutch_book_basket",
            f"Allocate across {len(trade_legs)} legs to capture {edge*100:.2f}% Dutch edge",
            trade_legs,
            estimated_edge_bps=edge * 10000,
        )
        return {
            "score": score,
            "message": message,
            "payload": {
                "total_price": total,
                "legs": legs,
                "edge": edge,
                "estimated_edge_bps": edge * 10000,
                "book_snapshot": book_snapshot,
                "suggested_trade": suggested_trade,
            },
            "edge_score": edge,
        }

    def _rule_spike(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        window_secs = rule.config.get("params", {}).get("window_secs", 10)
        pct_threshold = rule.config.get("params", {}).get("pct_change_gt", 0.03)
        min_liq = rule.config.get("params", {}).get("min_liquidity", 0)
        label_map = {opt["option_id"]: opt.get("label", opt["option_id"]) for opt in options_meta}
        now = datetime.now(timezone.utc)
        filtered = [tick for tick in recent_ticks if (now - tick["ts"]).total_seconds() <= window_secs]
        if not filtered:
            return None
        filtered.reverse()
        for option_id in {t["option_id"] for t in filtered if t["option_id"]}:
            option_ticks = [t for t in filtered if t["option_id"] == option_id]
            if len(option_ticks) < 2:
                continue
            start_price = _to_float(option_ticks[0]["price"])
            end_price = _to_float(option_ticks[-1]["price"])
            pct_change = (end_price - start_price) / max(start_price, 0.01)
            latest = latest_ticks.get(option_id, {})
            liquidity = _to_float(latest.get("liquidity"))
            if abs(pct_change) >= pct_threshold and liquidity >= min_liq:
                metrics = {
                    "velocity": abs(pct_change) * 100,
                    "liquidity": liquidity / 10,
                    "spread": 1.0,
                }
                score = scoring.compute_score(
                    rule.config.get("outputs", {}).get("score", {}).get("base", 50),
                    rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                    metrics,
                )
                direction = "up" if pct_change > 0 else "down"
                label = label_map.get(option_id, option_id)
                message = self._format_message(
                    rule,
                    market,
                    f"{label} {direction} {pct_change*100:.2f}%/{window_secs}s",
                )
                book_snapshot = self._book_snapshot(options_meta, latest_ticks)
                trade_side = "buy" if pct_change > 0 else "sell"
                trade_plan = self._trade_plan(
                    "momentum_follow" if pct_change > 0 else "mean_revert",
                    f"{label} moved {pct_change*100:.2f}% over {window_secs}s ({direction})",
                    [
                        self._build_trade_leg(
                            market["market_id"],
                            option_id,
                            trade_side,
                            _to_float(latest.get("price")),
                            label=label,
                        )
                    ],
                    estimated_edge_bps=abs(pct_change) * 10000,
                )
                return {
                    "score": score,
                    "message": message,
                    "option_id": option_id,
                    "payload": {
                        "pct_change": pct_change,
                        "window_secs": window_secs,
                        "book_snapshot": book_snapshot,
                        "suggested_trade": trade_plan,
                    },
                    "edge_score": abs(pct_change),
            }
        return None

    def _rule_crypto_lead_lag(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        symbol = self._map_crypto_symbol(market.get("title", ""))
        if not symbol:
            return None
        feed = self.binance_cache.get_price_data(symbol)
        if not feed:
            return None
        params = rule.config.get("params", {})
        threshold = params.get("return_threshold", 0.003)
        poly_drift_threshold = params.get("poly_drift_threshold", 0.002)
        if abs(feed.return_1s) < threshold:
            return None
        option_id, label, tick = self._primary_option(latest_ticks, options_meta)
        if not option_id:
            return None
        recent_price = None
        for entry in recent_ticks:
            if entry.get("option_id") == option_id:
                recent_price = _to_float(entry.get("price"))
                break
        poly_price = _to_float(tick.get("price"))
        if recent_price is not None and abs(poly_price - recent_price) > poly_drift_threshold:
            return None
        ts_field = tick.get("ts")
        if isinstance(ts_field, datetime):
            poly_ts = ts_field.timestamp()
        elif isinstance(ts_field, (int, float)):
            poly_ts = float(ts_field)
        else:
            poly_ts = time.time()
        latency_gap = abs(feed.ts - poly_ts)
        side = "buy" if feed.return_1s > 0 else "sell"
        metrics = {
            "momentum": abs(feed.return_1s) * 1000,
            "liquidity": _to_float(tick.get("liquidity")) / 10,
        }
        score = scoring.compute_score(
            rule.config.get("outputs", {}).get("score", {}).get("base", 55),
            rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
            metrics,
        )
        trade_plan = self._trade_plan(
            "lead_lag_follow",
            f"{symbol} 1s return {feed.return_1s*100:.2f}% vs Polymarket lag",
            [
                self._build_trade_leg(
                    market["market_id"],
                    option_id,
                    side,
                    poly_price,
                    label=label,
                )
            ],
            estimated_edge_bps=abs(feed.return_1s) * 10000,
        )
        return {
            "score": score,
            "message": self._format_message(
                rule,
                market,
                f"{symbol} lead-lag {feed.return_1s*100:.2f}%",
            ),
            "option_id": option_id,
            "payload": {
                "binance_price": feed.price,
                "poly_price": poly_price,
                "latency_gap": latency_gap,
                "suggested_trade": trade_plan,
            },
            "edge_score": abs(feed.return_1s),
        }

    def _rule_temporal_arbitrage(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        snapshot = self._latest_snapshots.get(market["market_id"]) or {}
        peer_ids = snapshot.get("synonym_ids") or []
        if not peer_ids or not market.get("ends_at"):
            return None
        base_title = self._normalize_title(market.get("title", ""))
        params = rule.config.get("params", {})
        threshold = params.get("spread_gt", 0.02)
        for peer_id in peer_ids:
            peer_snapshot = self._latest_snapshots.get(peer_id)
            if not peer_snapshot:
                continue
            peer_market = peer_snapshot.get("market")
            if not peer_market or not peer_market.get("ends_at"):
                continue
            if self._normalize_title(peer_market.get("title", "")) != base_title:
                continue
            near_market = market
            far_market = peer_market
            if peer_market["ends_at"] < market["ends_at"]:
                near_market, far_market = peer_market, market
            near_ticks, near_opts = (
                (latest_ticks, options_meta)
                if near_market is market
                else (
                    peer_snapshot.get("ticks") or {},
                    peer_snapshot.get("options") or [],
                )
            )
            far_snapshot = (
                peer_snapshot
                if far_market is peer_market
                else self._latest_snapshots.get(market["market_id"]) or {}
            )
            far_ticks = far_snapshot.get("ticks") or {}
            far_opts = far_snapshot.get("options") or []
            near_option = self._primary_option(near_ticks, near_opts)
            far_option = self._primary_option(far_ticks, far_opts)
            if not near_option[0] or not far_option[0]:
                continue
            gap = _to_float(near_option[2].get("price")) - _to_float(far_option[2].get("price"))
            if gap <= threshold:
                continue
            trade = self._trade_plan(
                "temporal_spread",
                f"Sell {near_market['market_id']} buy {far_market['market_id']} gap {gap*100:.2f}%",
                [
                    self._build_trade_leg(
                        near_market["market_id"],
                        near_option[0],
                        "sell",
                        _to_float(near_option[2].get("price")),
                        label=near_option[1],
                    ),
                    self._build_trade_leg(
                        far_market["market_id"],
                        far_option[0],
                        "buy",
                        _to_float(far_option[2].get("price")),
                        label=far_option[1],
                    ),
                ],
                estimated_edge_bps=gap * 10000,
            )
            return {
                "score": 60 + gap * 500,
                "message": self._format_message(
                    rule,
                    market,
                    f"Temporal arbitrage {gap*100:.2f}%",
                ),
                "payload": {
                    "gap": gap,
                    "near_market": near_market["market_id"],
                    "far_market": far_market["market_id"],
                    "suggested_trade": trade,
                },
                "edge_score": gap,
            }
        return None

    def _rule_order_book_imbalance(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        snapshot = self._latest_snapshots.get(market["market_id"]) or {}
        features = extract_features_realtime(
            market,
            latest_ticks,
            recent_ticks,
            snapshot.get("synonym_peers"),
        )
        if not features:
            return None
        imbalance = features.get("size_imbalance")
        spread = features.get("spread")
        if imbalance is None or spread is None:
            return None
        params = rule.config.get("params", {})
        if abs(imbalance) <= params.get("imbalance_threshold", 0.8):
            return None
        if spread > params.get("max_spread", 0.02):
            return None
        option_id, label, tick = self._primary_option(latest_ticks, options_meta)
        if not option_id:
            return None
        side = "buy" if imbalance > 0 else "sell"
        price = _to_float(tick.get("price"))
        trade = self._trade_plan(
            "orderbook_follow",
            f"Imbalance {imbalance:.2f} spread {spread:.3f}",
            [
                self._build_trade_leg(
                    market["market_id"],
                    option_id,
                    side,
                    price,
                    label=label,
                )
            ],
            estimated_edge_bps=(params.get("max_spread", 0.02) - spread) * 10000,
        )
        return {
            "score": 55 + abs(imbalance) * 10,
            "message": self._format_message(
                rule,
                market,
                f"Order book imbalance {imbalance:.2f}",
            ),
            "option_id": option_id,
            "payload": {
                "size_imbalance": imbalance,
                "spread": spread,
                "suggested_trade": trade,
            },
            "edge_score": abs(imbalance),
        }

    def _rule_volatility_harvest(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not self.ml_model:
            return None
        snapshot = self._latest_snapshots.get(market["market_id"]) or {}
        features = extract_features_realtime(
            market,
            latest_ticks,
            recent_ticks,
            snapshot.get("synonym_peers"),
        )
        if not features:
            return None
        params = rule.config.get("params", {})
        drop_threshold = params.get("drop_threshold", -0.05)
        spread_limit = params.get("spread_limit", 0.1)
        min_liq = params.get("min_liquidity", 1000.0)
        mid_price = features.get("mid_price", 0.0)
        price_velocity = features.get("price_velocity_10s", 0.0)
        drop_pct = price_velocity / max(mid_price, 1e-6)
        if drop_pct >= drop_threshold:
            return None
        if features.get("spread", 0.0) > spread_limit:
            return None
        top_option_id, label, top_tick = self._primary_option(latest_ticks, options_meta)
        if not top_option_id or _to_float(top_tick.get("liquidity")) < min_liq:
            return None
        prob = self._predict_ml_probability(features)
        if prob is None or prob < params.get("ml_min_confidence", 0.6):
            return None
        fair_value_gap = prob - mid_price
        trade = self._trade_plan(
            "volatility_harvest",
            f"Drop {drop_pct*100:.2f}% but ML confidence {prob*100:.1f}%",
            [
                self._build_trade_leg(
                    market["market_id"],
                    top_option_id,
                    "buy",
                    mid_price,
                    label=label,
                )
            ],
            estimated_edge_bps=fair_value_gap * 10000,
            confidence=prob,
        )
        return {
            "score": 60 + prob * 20,
            "message": self._format_message(
                rule,
                market,
                f"Volatility harvest {drop_pct*100:.2f}% drop",
            ),
            "option_id": top_option_id,
            "payload": {
                "drop_pct": drop_pct,
                "ml_confidence": prob,
                "fair_value_gap": fair_value_gap,
                "suggested_trade": trade,
            },
            "edge_score": abs(fair_value_gap),
        }

    def _rule_zombie_hunter(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not self.ml_model:
            return None
        snapshot = self._latest_snapshots.get(market["market_id"]) or {}
        features = extract_features_realtime(
            market,
            latest_ticks,
            recent_ticks,
            snapshot.get("synonym_peers"),
        )
        if not features:
            return None
        params = rule.config.get("params", {})
        max_price = params.get("max_price", 0.03)
        min_liq = params.get("min_liquidity", 500.0)
        expiry_limit = params.get("expiry_days_limit", 7)
        option_id, label, tick = self._primary_option(latest_ticks, options_meta)
        if not option_id:
            return None
        price = _to_float(tick.get("price"))
        liquidity = _to_float(tick.get("liquidity"))
        if price > max_price or liquidity < min_liq:
            return None
        days_to_expiry = features.get("days_to_expiry", 0.0)
        if days_to_expiry > expiry_limit:
            return None
        prob = self._predict_ml_probability(features)
        if prob is None or prob >= params.get("ml_max_confidence", 0.01):
            return None
        trade = self._trade_plan(
            "zombie_hunter",
            f"Sell overpriced tail risk ({price:.3f}, model {prob:.3f})",
            [
                self._build_trade_leg(
                    market["market_id"],
                    option_id,
                    "sell",
                    price,
                    label=label,
                )
            ],
            estimated_edge_bps=(price - prob) * 10000,
            confidence=1 - prob,
        )
        return {
            "score": 50 + (1 - prob) * 25,
            "message": self._format_message(
                rule,
                market,
                f"Zombie Hunter: expiry {days_to_expiry:.1f}d",
            ),
            "option_id": option_id,
            "payload": {
                "days_to_expiry": days_to_expiry,
                "implied_prob": price,
                "model_prob": prob,
                "suggested_trade": trade,
            },
            "edge_score": price - prob,
        }

    def _rule_endgame(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        min_price = rule.config.get("params", {}).get("min_price", 0.95)
        minutes_limit = rule.config.get("params", {}).get("minutes_to_end", 30)
        min_liq = rule.config.get("params", {}).get("min_liquidity", 0)
        label_map = {opt["option_id"]: opt.get("label", opt["option_id"]) for opt in options_meta}
        minutes_to_end = self._minutes_to_end(market)
        if minutes_to_end is None or minutes_to_end > minutes_limit:
            return None
        for option_id, tick in latest_ticks.items():
            price = _to_float(tick.get("price"))
            liquidity = _to_float(tick.get("liquidity"))
            if price >= min_price and liquidity >= min_liq:
                option_ticks = [t for t in recent_ticks if t["option_id"] == option_id]
                if len(option_ticks) < 3:
                    continue
                volumes = [_to_float(t.get("volume")) for t in option_ticks[:20]]
                if len(volumes) < 2:
                    continue
                vol_mean = mean(volumes)
                vol_std = stdev(volumes) if len(volumes) >= 2 else 1
                last_vol = volumes[0]
                z_score = (last_vol - vol_mean) / max(vol_std, 1)
                min_z = rule.config.get("params", {}).get("vol_surge_z", 1.0)
                if z_score >= min_z:
                    metrics = {
                        "time_to_end": minutes_limit - minutes_to_end,
                        "liquidity": liquidity / 10,
                        "vol_surge": z_score * 10,
                    }
                    score = scoring.compute_score(
                        rule.config.get("outputs", {}).get("score", {}).get("base", 60),
                        rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                        metrics,
                    )
                    label = label_map.get(option_id, option_id)
                    message = self._format_message(
                        rule,
                        market,
                        f"{label} trades at {price:.2f} with {minutes_to_end:.1f}m left",
                    )
                    book_snapshot = self._book_snapshot(options_meta, latest_ticks)
                    trade_plan = self._trade_plan(
                        "endgame_sweep",
                        f"Buy {label} at {price:.2f} with {minutes_to_end:.1f}m to expiry (z={z_score:.2f})",
                        [
                            self._build_trade_leg(
                                market["market_id"],
                                option_id,
                                "buy",
                                price,
                                label=label,
                            )
                        ],
                        estimated_edge_bps=max(0.0, price - min_price) * 10000,
                    )
                    return {
                        "score": score,
                        "message": message,
                        "option_id": option_id,
                        "payload": {
                            "price": price,
                            "minutes_to_end": minutes_to_end,
                            "z_score": z_score,
                            "book_snapshot": book_snapshot,
                            "suggested_trade": trade_plan,
                        },
                        "edge_score": max(0.0, price - min_price),
                    }
        return None

    def _rule_cross_market(
        self,
        rule: Rule,
        groups: List[dict[str, Any]],
        snapshots: dict[str, dict[str, Any]],
    ) -> List[tuple[str, dict[str, Any]]]:
        payloads: list[tuple[str, dict[str, Any]]] = []
        params = rule.config.get("params", {})
        min_size = params.get("group_min_size", 2)
        gap_threshold = params.get("price_diff_threshold", 0.05)
        min_liq = params.get("min_liquidity", 0.0)
        for group in groups:
            market_maps: list[tuple[str, dict[str, dict[str, Any]]]] = []
            for market_id in group.get("members", []):
                snapshot = snapshots.get(market_id)
                if not snapshot:
                    continue
                labelled = self._labelled_options(snapshot)
                if not labelled:
                    continue
                market_maps.append((market_id, labelled))
            if len(market_maps) < min_size:
                continue
            best: Optional[dict[str, Any]] = None
            for idx in range(len(market_maps)):
                market_a, options_a = market_maps[idx]
                for jdx in range(idx + 1, len(market_maps)):
                    market_b, options_b = market_maps[jdx]
                    shared_labels = set(options_a.keys()) & set(options_b.keys())
                    if not shared_labels:
                        continue
                    for label_key in shared_labels:
                        option_a = options_a[label_key]
                        option_b = options_b[label_key]
                        liquidity = min(option_a["liquidity"], option_b["liquidity"])
                        if liquidity < min_liq:
                            continue
                        gap = abs(option_a["price"] - option_b["price"])
                        if gap < gap_threshold:
                            continue
                        if option_a["price"] >= option_b["price"]:
                            leader, laggard = option_a, option_b
                        else:
                            leader, laggard = option_b, option_a
                        candidate = {
                            "gap": gap,
                            "leader": leader,
                            "laggard": laggard,
                            "label": option_a["label"],
                            "liquidity": liquidity,
                        }
                        if not best or candidate["gap"] > best["gap"]:
                            best = candidate
            if not best:
                continue
            minutes_to_end = self._minutes_to_end(best["laggard"]["market"]) or 0
            leader_title = best["leader"]["market"].get("title") if best["leader"].get("market") else best["leader"]["market_id"]
            laggard_title = (
                best["laggard"]["market"].get("title") if best["laggard"].get("market") else best["laggard"]["market_id"]
            )
            metrics = {
                "gap": best["gap"] * 100,
                "liquidity": best["liquidity"] / 10,
                "time_to_end": minutes_to_end / 10,
            }
            score = scoring.compute_score(
                rule.config.get("outputs", {}).get("score", {}).get("base", 65),
                rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                metrics,
            )
            label_text = best["label"]
            insight = (
                f"{label_text} misprice {best['gap']*100:.2f}% "
                f"({leader_title} vs {laggard_title})"
            )
            laggard_snapshot = snapshots.get(best["laggard"]["market_id"]) or {}
            book_snapshot = self._book_snapshot(
                laggard_snapshot.get("options") or [],
                laggard_snapshot.get("ticks") or {},
            )
            trade_plan = self._trade_plan(
                "cross_market_pair",
                f"Buy {laggard_title} ({best['laggard']['market_id']}) {label_text} and sell "
                f"{leader_title} ({best['leader']['market_id']}) {label_text} gap {best['gap']*100:.2f}%",
                [
                    self._build_trade_leg(
                        best["laggard"]["market_id"],
                        best["laggard"]["option_id"],
                        "buy",
                        best["laggard"]["price"],
                        label=label_text,
                    ),
                    self._build_trade_leg(
                        best["leader"]["market_id"],
                        best["leader"]["option_id"],
                        "sell",
                        best["leader"]["price"],
                        label=label_text,
                    ),
                ],
                estimated_edge_bps=best["gap"] * 10000,
            )
            payloads.append(
                (
                    best["laggard"]["market_id"],
                    {
                        "score": score,
                        "message": self._format_message(rule, best["laggard"]["market"], insight),
                        "payload": {
                            "gap": best["gap"],
                            "leader": best["leader"]["market_id"],
                            "laggard": best["laggard"]["market_id"],
                            "target_label": label_text,
                            "estimated_edge_bps": best["gap"] * 10000,
                            "comparables": [
                    {
                        "market_id": best["leader"]["market_id"],
                        "title": leader_title,
                        "price": best["leader"]["price"],
                        "liquidity": best["leader"]["liquidity"],
                        "role": "leader",
                        "label": label_text,
                    },
                    {
                        "market_id": best["laggard"]["market_id"],
                        "title": laggard_title,
                        "price": best["laggard"]["price"],
                        "liquidity": best["laggard"]["liquidity"],
                        "role": "laggard",
                        "label": label_text,
                    },
                            ],
                            "book_snapshot": book_snapshot,
                            "suggested_trade": trade_plan,
                        },
                        "edge_score": best["gap"],
                    },
                )
            )
        return payloads

    def _book_snapshot(
        self,
        options_meta: list[dict[str, Any]],
        latest_ticks: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        label_map = {opt.get("option_id"): opt.get("label", opt.get("option_id")) for opt in options_meta}
        snapshot: list[dict[str, Any]] = []
        for option_id, tick in latest_ticks.items():
            # Skip synthetic placeholder option_ids like "<market_id>-0/1" that contain '-'
            if "-" in str(option_id or ""):
                continue
            ts_value = tick.get("ts")
            if isinstance(ts_value, datetime):
                ts_value = ts_value.isoformat()
            snapshot.append(
                {
                    "option_id": option_id,
                    "label": label_map.get(option_id, option_id),
                    "price": _to_float(tick.get("price")),
                    "best_bid": _to_float(tick.get("best_bid")),
                    "best_ask": _to_float(tick.get("best_ask")),
                    "liquidity": _to_float(tick.get("liquidity")),
                    "ts": ts_value,
                }
            )
        # De-duplicate by label: keep most recent timestamp if duplicates remain
        if snapshot:
            grouped = {}
            for row in snapshot:
                label = row.get("label") or row.get("option_id")
                prev = grouped.get(label)
                if prev is None or (row.get("ts") or "") > (prev.get("ts") or ""):
                    grouped[label] = row
            snapshot = list(grouped.values())
        if not snapshot and options_meta:
            for opt in options_meta:
                snapshot.append(
                    {
                        "option_id": opt.get("option_id"),
                        "label": opt.get("label"),
                        "price": 0.0,
                        "best_bid": 0.0,
                        "best_ask": 0.0,
                        "liquidity": 0.0,
                        "ts": None,
                    }
                )
        snapshot.sort(key=lambda item: item.get("label") or item.get("option_id", ""))
        return snapshot

    def _build_trade_leg(
        self,
        market_id: str,
        option_id: str,
        side: str,
        price: float,
        *,
        label: Optional[str] = None,
        qty: float = 1.0,
    ) -> dict[str, Any]:
        price = _to_float(price) or 0.0
        slip = self.settings.exec_slippage_bps / 10000
        if side == "buy":
            limit_price = min(0.999, price * (1 + slip) if price else slip)
        else:
            limit_price = max(0.001, price * (1 - slip))
        return {
            "market_id": market_id,
            "option_id": option_id,
            "side": side,
            "qty": qty,
            "reference_price": price,
            "limit_price": limit_price,
            "label": label or option_id,
        }

    def _trade_plan(
        self,
        action: str,
        rationale: str,
        legs: list[dict[str, Any]],
        *,
        estimated_edge_bps: Optional[float] = None,
        confidence: Optional[float] = None,
    ) -> dict[str, Any]:
        return {
            "action": action,
            "rationale": rationale,
            "legs": legs,
            "estimated_edge_bps": estimated_edge_bps,
            "confidence": confidence,
        }

    def _format_message(self, rule: Rule, market: dict[str, Any], insight: str) -> str:
        base_url = "http://localhost:5173/markets"
        return (
            f"*{rule.name}*\n"
            f"Market: {market['title']}\n"
            f"Insight: {insight}\n"
            f"Detail: {base_url}/{market['market_id']}"
        )

    def _primary_option(
        self,
        ticks: dict[str, dict[str, Any]],
        options_meta: Optional[list[dict[str, Any]]] = None,
    ) -> tuple[Optional[str], Optional[str], dict[str, Any]]:
        if not ticks:
            return (None, None, {})
        option_id, tick = max(ticks.items(), key=lambda item: _to_float(item[1].get("price")))
        label = option_id
        if options_meta:
            label_map = {opt.get("option_id"): opt.get("label", opt.get("option_id")) for opt in options_meta}
            label = label_map.get(option_id, label)
        return option_id, label, tick

    def _labelled_options(self, snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
        ticks = snapshot.get("ticks") or {}
        if not ticks:
            return {}
        options_meta = snapshot.get("options") or []
        option_to_label: dict[str, str] = {}
        for opt in options_meta:
            option_id = opt.get("option_id")
            if not option_id:
                continue
            label = (opt.get("label") or option_id or "").strip()
            if not label:
                continue
            option_to_label[option_id] = label
        entries: dict[str, dict[str, Any]] = {}
        for option_id, tick in ticks.items():
            label = option_to_label.get(option_id, str(option_id))
            label_key = label.lower()
            price = _to_float(tick.get("price"))
            liquidity = _to_float(tick.get("liquidity"))
            existing = entries.get(label_key)
            if existing and liquidity <= existing["liquidity"]:
                continue
            entries[label_key] = {
                "market_id": snapshot["market"]["market_id"],
                "market": snapshot["market"],
                "option_id": option_id,
                "label": label,
                "price": price,
                "liquidity": liquidity,
            }
        return entries

    def _map_crypto_symbol(self, title: str) -> Optional[str]:
        lowered = title.lower()
        if "bitcoin" in lowered or "btc" in lowered:
            return "BTC"
        if "ethereum" in lowered or "eth" in lowered:
            return "ETH"
        if "solana" in lowered or "sol" in lowered:
            return "SOL"
        return None

    def _normalize_title(self, title: str) -> str:
        return "".join(ch for ch in title.lower() if ch.isalpha() or ch.isspace())

    def _minutes_to_end(self, market: dict[str, Any]) -> Optional[float]:
        ends_at = market.get("ends_at")
        if not ends_at:
            return None
        now = datetime.now(timezone.utc)
        delta = (ends_at - now).total_seconds() / 60
        return max(delta, 0)

    def _predict_ml_probability(self, feature_map: dict[str, Any]) -> Optional[float]:
        if not self.ml_model:
            return None
        feature_names = getattr(getattr(self.ml_model, "model", None), "feature_name_", None)
        if feature_names:
            payload = {name: feature_map.get(name, 0.0) for name in feature_names}
        else:
            payload = feature_map
        df = pd.DataFrame([payload]).fillna(0)
        probs = self.ml_model.predict_proba_batch(df)
        return probs[0] if probs else None

    def _is_market_enabled(self, market: dict[str, Any]) -> bool:
        if self.settings.data_source == "mock":
            return True
        platform = (market.get("platform") or "polymarket").lower()
        return platform == "polymarket"

    def _market_in_scope(self, rule: Rule, market: dict[str, Any]) -> bool:
        scope = rule.config.get("scope") or {}
        platforms = scope.get("platforms")
        if platforms and (market.get("platform") not in platforms):
            return False
        tags = scope.get("tags")
        if tags:
            market_tags = set(market.get("tags") or [])
            if not market_tags.intersection(set(tags)):
                return False
        return True
