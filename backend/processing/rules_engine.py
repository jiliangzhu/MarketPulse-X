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
        group_rules = [rule for rule in self.rules if rule.type in {"SYNONYM_MISPRICE", "CROSS_MARKET_MISPRICE"}]
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
            }
            for rule in self.rules:
                if rule.type in {"SYNONYM_MISPRICE", "CROSS_MARKET_MISPRICE"}:
                    continue
                if not self._market_in_scope(rule, market):
                    continue
                signal_payload = await self._evaluate_rule(rule, market, ticks, recent, options)
                if signal_payload:
                    rule_signals.append((rule, market_id, signal_payload))
        if group_rules:
            groups = await self.synonym_matcher.build_groups(self.db)
            for rule in group_rules:
                if rule.type == "SYNONYM_MISPRICE":
                    payloads = self._rule_synonym(rule, groups, snapshots)
                else:
                    payloads = self._rule_cross_market(rule, groups, snapshots)
                for market_id, payload in payloads:
                    rule_signals.append((rule, market_id, payload))

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
        if rule.type == "SUM_LT_1":
            return self._rule_sum_lt_1(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "SPIKE_DETECT":
            return self._rule_spike(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "ENDGAME_SWEEP":
            return self._rule_endgame(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "DUTCH_BOOK_DETECT":
            return self._rule_dutch_book(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "TREND_BREAKOUT":
            return self._rule_trend_breakout(rule, market, latest_ticks, recent_ticks, options_meta)
        if rule.type == "SYNONYM_MISPRICE":
            return None
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

    def _rule_sum_lt_1(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not latest_ticks:
            return None
        threshold = rule.config.get("thresholds", {}).get("sum_price_lt", 1.0)
        min_liq = rule.config.get("thresholds", {}).get("min_liquidity", 0.0)
        label_map = {opt.get("option_id"): opt.get("label", opt.get("option_id")) for opt in options_meta}
        total = sum(_to_float(tick.get("price")) for tick in latest_ticks.values())
        min_liquidity = min(_to_float(tick.get("liquidity")) for tick in latest_ticks.values())
        spread = sum(
            max(0.0, _to_float(tick.get("best_ask")) - _to_float(tick.get("best_bid")))
            for tick in latest_ticks.values()
        ) / max(len(latest_ticks), 1)
        if total < threshold and min_liquidity >= min_liq:
            edge = max(0.0, threshold - total)
            metrics = {
                "liquidity": min_liquidity / 10,
                "spread": 1 / max(spread, 0.01),
                "time_to_end": (self._minutes_to_end(market) or 0) / 10,
                "edge": edge * 100,
            }
            score = scoring.compute_score(
                rule.config.get("outputs", {}).get("score", {}).get("base", 60),
                rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                metrics,
            )
            message = self._format_message(
                rule,
                market,
                f"Total price {total:.3f} (< {threshold})",
            )
            book_snapshot = self._book_snapshot(options_meta, latest_ticks)
            trade_legs = [
                self._build_trade_leg(
                    market["market_id"],
                    option_id,
                    "buy",
                    _to_float(tick.get("price")),
                    label=label_map.get(option_id, option_id),
                )
                for option_id, tick in latest_ticks.items()
            ]
            suggested_trade = self._trade_plan(
                "basket_fill",
                f"Buy each outcome to lock {edge*100:.2f}% edge (sum {total:.3f})",
                trade_legs,
                estimated_edge_bps=edge * 10000,
            )
            return {
                "score": score,
                "message": message,
                "payload": {
                    "total_price": total,
                    "spread": spread,
                    "liquidity": min_liquidity,
                    "edge": edge,
                    "estimated_edge_bps": edge * 10000,
                    "book_snapshot": book_snapshot,
                    "suggested_trade": suggested_trade,
                },
                "edge_score": edge,
            }
        return None

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

    def _rule_trend_breakout(
        self,
        rule: Rule,
        market: dict[str, Any],
        latest_ticks: dict[str, dict[str, Any]],
        recent_ticks: list[dict[str, Any]],
        options_meta: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        params = rule.config.get("params", {})
        window_secs = params.get("lookback_secs", 300)
        pct_threshold = params.get("pct_breakout", 0.02)
        min_points = params.get("min_points", 5)
        min_liq = params.get("min_liquidity", 0.0)
        now = datetime.now(timezone.utc)
        filtered = [tick for tick in recent_ticks if (now - tick["ts"]).total_seconds() <= window_secs]
        if not filtered:
            return None
        label_map = {opt["option_id"]: opt.get("label", opt["option_id"]) for opt in options_meta}
        option_ids = {tick["option_id"] for tick in filtered if tick.get("option_id")}
        for option_id in option_ids:
            series = [tick for tick in filtered if tick["option_id"] == option_id]
            if len(series) < min_points:
                continue
            avg_price = sum(_to_float(t.get("price")) for t in series) / max(len(series), 1)
            if avg_price <= 0:
                continue
            latest = latest_ticks.get(option_id)
            if not latest:
                continue
            liquidity = _to_float(latest.get("liquidity"))
            if liquidity < min_liq:
                continue
            current_price = _to_float(latest.get("price"))
            delta = (current_price - avg_price) / avg_price
            if abs(delta) < pct_threshold:
                continue
            metrics = {
                "velocity": abs(delta) * 100,
                "time_to_end": (self._minutes_to_end(market) or 0) / 10,
                "liquidity": liquidity / 10,
            }
            score = scoring.compute_score(
                rule.config.get("outputs", {}).get("score", {}).get("base", 55),
                rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                metrics,
            )
            direction = "up" if delta > 0 else "down"
            label = label_map.get(option_id, option_id)
            message = self._format_message(
                rule,
                market,
                f"{label} breakout {direction} {delta*100:.2f}%/{window_secs}s",
            )
            book_snapshot = self._book_snapshot(options_meta, latest_ticks)
            trade_side = "buy" if delta > 0 else "sell"
            trade_plan = self._trade_plan(
                "trend_breakout",
                f"{label} trading {direction} {abs(delta)*100:.2f}% vs {window_secs}s average",
                [
                    self._build_trade_leg(
                        market["market_id"],
                        option_id,
                        trade_side,
                        current_price,
                        label=label,
                    )
                ],
                estimated_edge_bps=abs(delta) * 10000,
            )
            return {
                "score": score,
                "message": message,
                "option_id": option_id,
                "payload": {
                    "avg_price": avg_price,
                    "current_price": current_price,
                    "delta": delta,
                    "window_secs": window_secs,
                    "book_snapshot": book_snapshot,
                    "suggested_trade": trade_plan,
                },
                "edge_score": abs(delta),
            }
        return None

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

    def _rule_synonym(
        self,
        rule: Rule,
        groups: List[dict[str, Any]],
        snapshots: dict[str, dict[str, Any]],
    ) -> List[tuple[str, dict[str, Any]]]:
        payloads: list[tuple[str, dict[str, Any]]] = []
        min_size = rule.config.get("params", {}).get("group_min_size", 2)
        gap_threshold = rule.config.get("params", {}).get("price_gap_gt", 0.02)
        min_liq = rule.config.get("params", {}).get("min_liquidity", 0)
        for group in groups:
            members = []
            for market_id in group.get("members", []):
                snapshot = snapshots.get(market_id)
                if not snapshot:
                    continue
                ticks = snapshot.get("ticks") or {}
                if not ticks:
                    continue
                top_option_id, top_tick = max(
                    ticks.items(),
                    key=lambda item: _to_float(item[1].get("price")),
                )
                members.append(
                    {
                        "market_id": market_id,
                        "option_id": top_option_id,
                        "price": _to_float(top_tick.get("price")),
                        "liquidity": _to_float(top_tick.get("liquidity")),
                        "market": snapshot["market"],
                        "book_snapshot": self._book_snapshot(snapshot.get("options") or [], ticks),
                    }
                )
            if len(members) < min_size:
                continue
            leader = max(members, key=lambda x: x["price"])
            laggard = min(members, key=lambda x: x["price"])
            gap = leader["price"] - laggard["price"]
            if gap < gap_threshold:
                continue
            if min(leader["liquidity"], laggard["liquidity"]) < min_liq:
                continue
            metrics = {
                "gap": gap * 100,
                "liquidity": min(leader["liquidity"], laggard["liquidity"]) / 10,
                "time_to_end": (self._minutes_to_end(laggard["market"]) or 0) / 10,
            }
            score = scoring.compute_score(
                rule.config.get("outputs", {}).get("score", {}).get("base", 60),
                rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                metrics,
            )
            message = (
                f"{group.get('name')} gap {gap*100:.2f}% "
                f"({leader['market_id']} vs {laggard['market_id']})"
            )
            payloads.append(
                (
                    laggard["market_id"],
                    {
                        "score": score,
                        "message": self._format_message(rule, laggard["market"], message),
                        "payload": {
                            "gap": gap,
                            "leader": leader["market_id"],
                            "laggard": laggard["market_id"],
                            "estimated_edge_bps": gap * 10000,
                            "comparables": [
                                {
                                    "market_id": leader["market_id"],
                                    "price": leader["price"],
                                    "liquidity": leader["liquidity"],
                                    "role": "leader",
                                },
                                {
                                    "market_id": laggard["market_id"],
                                    "price": laggard["price"],
                                    "liquidity": laggard["liquidity"],
                                    "role": "laggard",
                                },
                            ],
                            "book_snapshot": laggard.get("book_snapshot"),
                            "suggested_trade": self._trade_plan(
                                "pair_trade",
                                f"Long {laggard['market_id']} vs short {leader['market_id']} gap {gap*100:.2f}%",
                                [
                                    self._build_trade_leg(
                                        laggard["market_id"],
                                        laggard.get("option_id", laggard["market_id"]),
                                        "buy",
                                        laggard["price"],
                                        label="laggard",
                                    ),
                                    self._build_trade_leg(
                                        leader["market_id"],
                                        leader.get("option_id", leader["market_id"]),
                                        "sell",
                                        leader["price"],
                                        label="leader",
                                    ),
                                ],
                                estimated_edge_bps=gap * 10000,
                            ),
                        },
                        "edge_score": gap,
                    },
                )
            )
        return payloads

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
        target_label = (params.get("target_label") or "").lower()
        for group in groups:
            members: list[dict[str, Any]] = []
            for market_id in group.get("members", []):
                snapshot = snapshots.get(market_id)
                if not snapshot:
                    continue
                option_id = self._select_option_by_label(snapshot.get("options") or [], target_label)
                if not option_id:
                    continue
                tick = snapshot.get("ticks", {}).get(option_id)
                if not tick:
                    continue
                price = _to_float(tick.get("price"))
                liquidity = _to_float(tick.get("liquidity"))
                if liquidity < min_liq:
                    continue
                members.append(
                    {
                        "market_id": market_id,
                        "price": price,
                        "liquidity": liquidity,
                        "market": snapshot["market"],
                        "option_id": option_id,
                    }
                )
            if len(members) < min_size:
                continue
            leader = max(members, key=lambda x: x["price"])
            laggard = min(members, key=lambda x: x["price"])
            gap = leader["price"] - laggard["price"]
            if gap < gap_threshold:
                continue
            metrics = {
                "gap": gap * 100,
                "liquidity": min(leader["liquidity"], laggard["liquidity"]) / 10,
                "time_to_end": (self._minutes_to_end(laggard["market"]) or 0) / 10,
            }
            score = scoring.compute_score(
                rule.config.get("outputs", {}).get("score", {}).get("base", 65),
                rule.config.get("outputs", {}).get("score", {}).get("weights", {}),
                metrics,
            )
            insight = (
                f"{target_label or 'yes'} misprice {gap*100:.2f}% "
                f"({leader['market_id']} vs {laggard['market_id']})"
            )
            laggard_snapshot = snapshots.get(laggard["market_id"]) or {}
            book_snapshot = self._book_snapshot(
                laggard_snapshot.get("options") or [],
                laggard_snapshot.get("ticks") or {},
            )
            trade_legs = [
                self._build_trade_leg(
                    laggard["market_id"],
                    laggard["option_id"],
                    "buy",
                    laggard["price"],
                    label="laggard",
                )
            ]
            trade_legs.append(
                self._build_trade_leg(
                    leader["market_id"],
                    leader["option_id"],
                    "sell",
                    leader["price"],
                    label="leader",
                )
            )
            trade_plan = self._trade_plan(
                "cross_market_pair",
                f"Buy {laggard['market_id']} {target_label or 'leg'} and sell {leader['market_id']} gap {gap*100:.2f}%",
                trade_legs,
                estimated_edge_bps=gap * 10000,
            )
            payloads.append(
                (
                    laggard["market_id"],
                    {
                        "score": score,
                        "message": self._format_message(rule, laggard["market"], insight),
                        "payload": {
                            "gap": gap,
                            "leader": leader["market_id"],
                            "laggard": laggard["market_id"],
                            "target_label": target_label,
                            "estimated_edge_bps": gap * 10000,
                            "comparables": [
                                {
                                    "market_id": leader["market_id"],
                                    "price": leader["price"],
                                    "liquidity": leader["liquidity"],
                                    "role": "leader",
                                },
                                {
                                    "market_id": laggard["market_id"],
                                    "price": laggard["price"],
                                    "liquidity": laggard["liquidity"],
                                    "role": "laggard",
                                },
                            ],
                            "book_snapshot": book_snapshot,
                            "suggested_trade": trade_plan,
                        },
                        "edge_score": gap,
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

    def _select_option_by_label(self, options: list[dict[str, Any]], target: str) -> Optional[str]:
        if not options:
            return None
        if target:
            for opt in options:
                label = (opt.get("label") or "").lower()
                if target in label:
                    return opt.get("option_id")
        return options[0].get("option_id")

    def _minutes_to_end(self, market: dict[str, Any]) -> Optional[float]:
        ends_at = market.get("ends_at")
        if not ends_at:
            return None
        now = datetime.now(timezone.utc)
        delta = (ends_at - now).total_seconds() / 60
        return max(delta, 0)

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
