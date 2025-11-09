from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()

ingest_latency_ms = Histogram(
    "mpx_ingest_latency_ms",
    "Latency of ingestion polling in milliseconds",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2000),
    labelnames=("source",),
    registry=REGISTRY,
)

rule_eval_ms = Histogram(
    "mpx_rule_eval_ms",
    "Rule evaluation latency",
    buckets=(5, 10, 50, 100, 250, 500, 1000),
    registry=REGISTRY,
)

signals_counter = Counter(
    "mpx_signals_total",
    "Signals emitted",
    labelnames=("rule", "source"),
    registry=REGISTRY,
)

ml_inference_ms = Histogram(
    "mpx_ml_inference_ms",
    "ML inference latency",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000),
    registry=REGISTRY,
)

order_intent_counter = Counter(
    "mpx_order_intents_total",
    "Order intents created per status",
    labelnames=("status",),
    registry=REGISTRY,
)

telegram_failures = Counter(
    "mpx_telegram_failures_total",
    "Telegram send failures",
    registry=REGISTRY,
)

ingest_last_tick_ts = Gauge(
    "mpx_ingest_last_tick_timestamp",
    "Unix timestamp for the last successful tick ingestion",
    labelnames=("source",),
    registry=REGISTRY,
)
