# MarketPulse-X Capabilities Overview

This document summarizes the capabilities delivered by the current MarketPulse-X codebase. It is intended as the baseline reference before starting the next iteration.

---
## 1. Architecture at a Glance

| Layer | Technology | Responsibilities |
| --- | --- | --- |
| **Ingestion** | Python 3.11, `asyncio`, `httpx` | Pulls market metadata and pricing data from Polymarket (Gamma API). Writes normalized ticks/markets/options into TimescaleDB. |
| **Processing / Rules** | FastAPI background workers, custom rule engine | Loads DSL rules from `configs/rules/*`, evaluates latest ticks per market, computes scores, emits signals, and writes telemetry/KPI/audit logs. |
| **Execution & Risk** | FastAPI API + async executor | Creates order intents from signals, enforces notional/concurrency/slippage/circuit breaker limits, simulates fills when `DATA_SOURCE=mock`. |
| **Alerting** | Telegram notifier (dry‑run when disabled) | Sends Markdown alerts for each signal; dedupes and retries. |
| **Persistence** | PostgreSQL 15 + TimescaleDB | Stores markets, options, ticks, rule definitions, KPIs, order intents, audit logs. |
| **Frontend** | Vite + React 18 | Dashboard with Signal Stream + execution modal, Market list/detail with sparkline & synonym tags, KPI cards, runbook tips. |
| **Ops** | Docker Compose + Makefile | Orchestrates Postgres, API, ingestion worker, rules worker, frontend dev server, Adminer. |

---
## 2. Data Acquisition & Normalization

### 2.1 Real Polymarket Source
- `backend/ingestion/source_real.py` first hits Gamma `/markets` for metadata, then `/markets/{id}` for `clobTokenIds`, `outcomePrices`, liquidity/volume stats.
- For each token it calls `https://clob.polymarket.com/book?token_id=...` (batched via TTL cache) to grab best bid/ask, timestamp and basic depth; cache TTL=5s avoids hammering hot markets.
- Ticks store:
  - True token IDs as `option_id` (so downstream execution can map back to Polymarket tokens).
  - Order-book derived bid/ask/mid price; fallback to Gamma price when CLOB missing.
  - Liquidity/volume from Gamma detail.
- `StreamProcessor` improvements:
  - Parallel chunk polling (`scheduler.max_concurrency`) + exponential backoff with jitter.
  - In-memory price cache (per-market+option) ensures unchanged ticks never hit DB.
  - Prometheus `mpx_ingest_latency_ms{source=RealPolymarketSource}` + `mpx_ingest_last_tick_timestamp{source=...}` track freshness.

### 2.2 Mock Data Source
- `backend/ingestion/source_mock.py` generates deterministic markets and pseudo-random price drifts, ensuring local runs and tests always have activity.
- Toggle via `.env` → `DATA_SOURCE=mock` or `real`.

---
## 3. Storage Schema Highlights

- **Timescale hypertables** for ticks (`migrations/001_init.sql`).
- **Markets & options** with cascading FK relationships.
- **Signals & rule_def** to persist DSL versions and fired signals.
- `signal.edge_score` (NUMERIC) captures normalized arbitrage edge for downstream ranking/UI。
- **Execution tables** (`execution_policy`, `order_intent`) added in Phase 2 for semi-auto flow.
- **Synonym grouping tables** (`synonym_group`, `synonym_group_member`) to track equivalent markets.
- **KPI table** (`rule_kpi_daily`) aggregated per rule per day.
- **Audit log** capturing uploads, signal emissions, execution actions.

---
## 4. Rule Engine & Alerts

### 4.1 DSL Coverage
Located in `configs/rules/`:
- `SUM_LT_1` — Detects when multi-outcome prices sum < 1.
- `SPIKE_DETECT` — Monitors 10-second price velocity and liquidity threshold.
- `ENDGAME_SWEEP` — Flags high-price, near-expiration sweeps with volume z-score check.
- `SYNONYM_MISPRICE` — Uses `synonym_matcher` to compare grouped markets and trigger when price gap > 2.5%.
- `DUTCH_BOOK_DETECT` — Detects baskets whose probabilities sum < 0.995, computes `edge_score = 1 - Σp`.
- `CROSS_MARKET_MISPRICE` — Within synonym groups, compares identical outcomes (e.g., “Yes”) between markets; triggers when price diff exceeds configurable threshold.
- `TREND_BREAKOUT` — Looks for >X% deviation between latest price and rolling mean within lookback window, capturing trend shifts.

Each rule handler now returns an `edge_score` (e.g., price gap, dutch edge, breakout delta) which is stored in `signal.edge_score`, exposed via `/api/signals`, and surfaced on the dashboard/execution modal.

### 4.2 Engine Flow (`backend/processing/rules_engine.py`)
1. Loads YAML rules → persists to `rule_def`.
2. Every cycle (default 2s):
   - Pulls latest ticks and 5-minute window per market.
   - Evaluates each rule, honoring per-rule cooldown plus circuit breaker state.
   - Emits signals → writes to `signal`, updates KPI, records audit, increments Prometheus counters.
   - Sends Telegram alert (dry-run if disabled) with Markdown message linking to frontend detail view.

### 4.3 Telegram Notifier
- Config via `.env` (`TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`).
- dry-run logs payload and marks `payload_json.transport = telegram-dry-run` when disabled/missing credentials.
- Rolling dedupe + TTL cache to avoid spamming same market/rule.

---
## 5. Execution & Risk Controls

### 5.1 Semi-auto Intent Lifecycle
- `POST /api/execution/intent` (P1/P2 signals only):
  - Pulls latest price snapshot for market.
  - Applies rule-specific heuristics for qty/limit price.
  - Persists `order_intent` with status `suggested` + detail JSON.

- `POST /api/execution/confirm/{intent_id}`:
  - Validates notional (`EXEC_MAX_NOTIONAL_PER_ORDER`), daily limit, concurrent intents.
  - Validates slippage (guardrail compares limit price vs latest bid/ask).
  - Applies circuit breaker (per rule/market). Mock source auto-fills.
  - Status transitions to `filled` or `rejected`, reasons logged in `detail_json.checks`.

- `GET /api/execution/intents?status=...` returns recent intents for UI/analysis.

### 5.2 Risk Modules
- `backend/risk/limits.py`: Notional, concurrent, daily caps.
- `backend/risk/guardrails.py`: Slippage enforcement using latest bids/asks. Decimal-safe.
- `backend/risk/circuit_breaker.py`: Threshold + cooldown per rule/market.

---
## 6. API Surface

| Endpoint | Description |
| --- | --- |
| `GET /api/healthz` | Returns DB connectivity and last signal heartbeat status. |
| `GET /api/markets?limit=&offset=&status=` | Paginated market summaries with latest option price snapshots. |
| `GET /api/markets/{id}` | Detailed view with options, recent sparkline, synonym peers. |
| `GET /api/signals?limit=&offset=&level=&since=` | Paginated signal stream. |
| `POST /api/rules` | Uploads YAML DSL (requires `x-api-key`). |
| `GET /api/kpi/daily` | Past 7-day per-rule KPI stats for frontend cards. |
| `POST /api/execution/intent` | Generates suggested order intent from signal. |
| `POST /api/execution/confirm/{id}` | Runs risk checks and simulates execution. |
| `GET /api/execution/intents` | List stored intents filtered by status. |
| `POST /api/alerts/test` | Sends a test Telegram message (respects dry-run). |
| `GET /metrics` | Prometheus exposition: ingest latency, rule eval, signal totals, order intent totals, Telegram failures, request counter, health gauge. |

---
## 7. Frontend Features

Located under `frontend/src/` (Vite + React + TS).

### 7.1 Dashboard (`src/pages/Dashboard.tsx`)
- **KPI Cards** (`KpiCards`): Aggregated signals/edge per rule type for past 7 days.
- **Signal Stream** (`SignalList`): Polls `/api/signals` every 5s，支持 level 过滤、页码/页量 (10/20/50) 选择，列表中展示 `edge_score` 与 `rule_type`，并内嵌“下单”按钮触发执行 Modal。
- **Execution Modal** (`ExecutionModal`): 展示 rule/edge/transport 摘要、风险校验结果（彩色提示）、detail JSON，并允许手动确认（mock 模式自动 filled）。
- **Markets Pane** (`MarketList`): Paginated (10/20/50 per page) view of markets with latest option prices and status badges.
- **Runbook Card** (`RunbookCard`): On-page troubleshooting tips and handy CLI commands.
- **Health Card**: Visual indicator driven by `/api/healthz`.

### 7.2 Market Detail (`src/pages/MarketDetail.tsx`)
- Shows 3-minute sparkline (SVG path) from `/api/markets/{id}` sparkline data.
- Lists options with latest prices.
- “相似市场” section linking to synonym peers.

### 7.3 Dev Experience
- Hot reload via Vite dev server container (`npm install && npm run dev -- --host 0.0.0.0 --port 5173`).
- Proxy to API through docker network; no cors issues for local dev.

---
## 8. Operational Tooling

### 8.1 Docker Compose Services
- `postgres`: TimescaleDB with persisted volume.
- `api`: FastAPI server (uvicorn) with shared code volume.
- `ingestor`: Worker running `backend.workers.ingestor`.
- `worker`: Rules/alert worker (`backend.workers.rules_worker`).
- `frontend`: Vite dev server.
- `adminer`: DB inspection UI.

### 8.2 Makefile Targets
| Target | Command |
| --- | --- |
| `make up` | `docker compose up -d --build` |
| `make migrate` | `docker compose exec api python -m backend.scripts.migrate` |
| `make seed` | `docker compose exec api python scripts/seed_mock_data.py` |
| `make smoke` | `python -m pytest --cov=backend --cov-report=term-missing -q` |
| `make logs` | `docker compose logs -f --tail=200` |
| `make down` | `docker compose down -v` |

### 8.3 Telemetry & Health
- `/metrics` scrapes ingestion latency, rule eval time, signal count per rule, order intent counts, telegram failure counter, request totals.
- `/api/healthz` ensures DB connectivity and monitors rule heartbeat (based on last signal timestamp).
- Logger emits JSON lines with consistent fields (`app`, `market_id`, `rule_id`, etc.), making ingestion into ELK trivial.

### 8.4 Testing & Coverage
- `tests/` includes API smoke tests, ingestion mock tests, rules-engine unit tests, notifier dry-run test, risk/guardrail tests, execution flow tests, synonym matcher, stream dedupe, etc.
- `pytest` coverage currently ~63% (see `make smoke`).

---
## 9. Seed & Mock Utilities
- `scripts/seed_mock_data.py`: Seeds mock markets/ticks for quick bootstrapping (used after `make migrate`).
- `scripts/gen_demo_traffic.py`: Generates mock tick bursts for load demos.
- `scripts/backtest_rules.py`: Replays historical ticks between `--start`/`--end` for a specific rule, outputs CSV hits.

---
## 10. Configuration & Secrets
- `.env` / `.env.example` define DB connection, data source, Telegram options, service role, execution limits.
- `SERVICE_ROLE` allows running “api only” vs “ingestor only” vs combined (useful in dev).
- All sensitive values (DB password, Telegram tokens, API key) are env-driven—no secrets committed.

---
## 11. Current Limitations / TODO Seeds for Next Iteration
1. Ingestion still relies on REST polling; a Polymarket CLOB WebSocket subscriber would cut latency and bandwidth dramatically.
2. Synonym matcher remains keyword/explicit based—embedding + fuzzy clustering would capture more equivalent markets (especially多语言标题)。
3. Execution is mock/semi-auto only; integrating an OEM or portfolio simulator (with fills, cancels, PnL) is the logical next step.
4. KPI/replay outputs are CSV/stdout; exposing a richer analytics API or Grafana dashboards (sparkline, hit-rate trends) would aid ops.
5. Production build/deploy pipeline (vite build + nginx, FastAPI behind gunicorn, log shipping) is not yet scripted.
6. Alerting channel limited to Telegram; Slack/webhook/Email fallbacks would harden incident response.

---
## 12. How to Verify Real-Data Flow
1. Set `DATA_SOURCE=real` in `.env`, rebuild/restart api/ingestor/worker.
2. Watch `docker compose logs ingestor -f` for `HTTP Request ... gamma-api`. No errors should appear.
3. Query DB:
   ```sql
   SELECT market_id, title FROM market LIMIT 5;
   SELECT count(*) FROM tick;
   ```
   Titles should match current Polymarket markets (e.g., “Fed rate hike in 2025?”).
4. `/api/markets` should return the same titles; front-end list updates accordingly.

---
This document captures the feature set as of the latest deployment. Use it as the baseline reference when planning further enhancements (e.g., integrating CLOB quotes, advanced analytics, additional rule types, or production-grade deployment).
