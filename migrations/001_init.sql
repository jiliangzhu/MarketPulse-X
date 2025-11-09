CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS market (
  market_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  platform TEXT NOT NULL DEFAULT 'polymarket',
  status TEXT NOT NULL,
  starts_at TIMESTAMPTZ,
  ends_at TIMESTAMPTZ,
  tags TEXT[]
);

CREATE TABLE IF NOT EXISTS market_option (
  option_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
  label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tick (
  ts TIMESTAMPTZ NOT NULL,
  market_id TEXT NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
  option_id TEXT REFERENCES market_option(option_id) ON DELETE CASCADE,
  price NUMERIC(12,6) NOT NULL,
  volume NUMERIC(18,6),
  best_bid NUMERIC(12,6),
  best_ask NUMERIC(12,6),
  PRIMARY KEY (ts, market_id, option_id)
);
SELECT create_hypertable('tick', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS rule_def (
  rule_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  dsl_yaml TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS signal (
  signal_id BIGSERIAL PRIMARY KEY,
  market_id TEXT NOT NULL,
  option_id TEXT NULL,
  rule_id INT,
  level TEXT NOT NULL,
  score NUMERIC(6,2),
  payload_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
  id BIGSERIAL PRIMARY KEY,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  target_id TEXT,
  meta_json JSONB,
  ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tick_market_ts ON tick (market_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_signal_created ON signal (created_at DESC);

ALTER TABLE tick ADD COLUMN IF NOT EXISTS liquidity NUMERIC(18,6);
