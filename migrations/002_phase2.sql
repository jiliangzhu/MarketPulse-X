CREATE TABLE IF NOT EXISTS execution_policy (
  policy_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  mode TEXT NOT NULL,
  max_notional_per_order NUMERIC(18,2),
  max_concurrent_orders INT,
  max_daily_notional NUMERIC(18,2),
  slippage_bps INT,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_intent (
  intent_id BIGSERIAL PRIMARY KEY,
  signal_id BIGINT REFERENCES signal(signal_id),
  market_id TEXT NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC(18,6) NOT NULL,
  limit_price NUMERIC(12,6),
  ttl_secs INT DEFAULT 60,
  status TEXT NOT NULL DEFAULT 'suggested',
  policy_id INT REFERENCES execution_policy(policy_id),
  detail_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_order_intent_ctime ON order_intent(created_at DESC);

CREATE TABLE IF NOT EXISTS synonym_group (
  group_id SERIAL PRIMARY KEY,
  method TEXT NOT NULL,
  title TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS synonym_group_member (
  group_id INT REFERENCES synonym_group(group_id) ON DELETE CASCADE,
  market_id TEXT NOT NULL,
  PRIMARY KEY(group_id, market_id)
);

CREATE TABLE IF NOT EXISTS rule_kpi_daily (
  day DATE,
  rule_type TEXT,
  signals INT,
  p1_signals INT,
  avg_gap NUMERIC(10,6),
  est_edge_bps NUMERIC(10,2),
  PRIMARY KEY (day, rule_type)
);
