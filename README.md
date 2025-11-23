# MarketPulse-X

> **MarketPulse-X** is an end-to-end Polymarket arbitrage monitor. It ingests live markets via the official Gamma + CLOB APIs (or a fully offline mock source), evaluates configurable rules, persists signals in TimescaleDB, visualises activity with a React dashboard, and exposes a semi-automatic execution/risk pipeline with Telegram alerting.

---

## 1. Key Capabilities

- **Real-time data ingestion** with asyncio + uvloop, concurrent polling, order-book caching, and Timescale dedupe writes.
- **Rule DSL**: built-in `SPIKE_DETECT`, `ENDGAME_SWEEP`, `DUTCH_BOOK_DETECT`, `CROSS_MARKET_MISPRICE` + ML fusion. Each signal carries `edge_score`和结构化 payload，后续执行/风控可直接引用。
- **Synonym grouping** from `configs/synonyms.yml` → `synonym_group*` tables, enabling cross-market arbitrage detection and “similar markets” UI hints.
- **Semi-automatic execution**: FastAPI endpoints + React modal create intents, run limit/guardrail/circuit-breaker checks, and (in mock mode) auto-fill confirmed orders.
- **Alerting & observability**: Telegram bot with dry-run fallback, Prometheus metrics (`/metrics`), structured JSON logs, and dashboard runbooks.
- **Backtesting & replay**: `scripts/backtest_rules.py` replays historical ticks (CSV output + optional accelerated replay) for strategy validation.

---

## 2. Architecture

| Layer | Services / Modules | Technologies |
| --- | --- | --- |
| **Data Lake** | `postgres` (TimescaleDB) | TimescaleDB 2.15 + PostgreSQL 15 |
| **API** | `backend/app.py`, FastAPI routers | FastAPI, Uvicorn, asyncpg |
| **Ingestion Worker** | `backend/workers/ingestor.py`, `StreamProcessor` | Python 3.11, asyncio, httpx, uvloop |
| **Rules Worker** | `backend/workers/rules_worker.py`, `RulesEngine` | FastAPI deps, YAML DSL, Telegram notifier |
| **Execution/Risk** | `backend/execution/*`, `backend/risk/*` | Pydantic, async repositories |
| **Frontend** | `frontend/` Vite + React dashboard | React 18, TypeScript, Vite |
| **Ops** | Docker Compose, Makefile, Prometheus metrics | docker compose, Makefile targets |

Container topology:
```
postgres ─┬─ api (FastAPI, /api & /metrics)
          ├─ ingestor (async data pump)
          ├─ worker (rules engine + Telegram)
          ├─ frontend (Vite dev server, port 5173)
          └─ adminer (DB console, port 8090)
```

---

## 3. Technology Stack

- **Backend**: Python 3.11, FastAPI, asyncpg, httpx, uvloop, cachetools, Pydantic settings.
- **Frontend**: React 18, TypeScript, Vite, CSS modules.
- **Database**: PostgreSQL 15 + TimescaleDB (ticks hypertable, rule/audit/intent tables).
- **Messaging**: Telegram Bot API (dry-run when disabled).
- **Testing**: pytest + httpx TestClient + asyncio fixtures, coverage reporting.
- **Observability**: Prometheus client, `/metrics` endpoint, JSON logs with structured fields.

---

## 4. Data Model Highlights

- `market`, `market_option`: canonical metadata for Polymarket markets/outcomes.
- `tick`: Timescale hypertable storing per-option prices, bids/asks, liquidity (`create_hypertable` in `001_init.sql`).
- `rule_def`, `signal`: DSL versions and emitted signals (now include `edge_score NUMERIC`).
- `execution_policy`, `order_intent`: semi-automatic execution config and order lifecycle.
- `synonym_group`, `synonym_group_member`: keyword/explicit synonym clusters.
- `rule_kpi_daily`: per-rule daily aggregates (signal count, P1 count, avg gap/edge bps).
- `audit_log`: persistence for lifecycle actions (rule loads, signals, execution changes).

---

## 5. Directory Layout

```
backend/
  api/                  # FastAPI routers (health, markets, signals, execution, KPI, alerts)
  ingestion/            # polymarket clients + mock/real sources
  processing/           # stream processor, rules engine, scoring, synonym matcher
  execution/            # executor, order router, OEMS helpers
  risk/                 # limits, guardrails, circuit breakers
  repo/                 # DB repositories (markets, ticks, signals, execution, KPI)
  workers/              # ingestor + rules worker entrypoints
  alerting/             # Telegram notifier (with dedupe/dry-run)
  utils/, settings.py   # logging, configuration
configs/
  app.yaml              # ingestion / rules cadence, scheduler knobs
  rules/*.yaml          # DSL definitions (core arbitrage strategies)
  synonyms.yml          # keyword dictionaries for grouping
frontend/               # React dashboard (App.tsx, SignalList, MarketList, ExecutionModal...)
migrations/             # SQL migrations (Timescale + execution + KPI + edge_score)
scripts/                # seed data, demo traffic, rule backtesting
tests/                  # pytest suites (ingestion, rules, execution, risk, API smoke, etc.)
docker-compose.yml
Makefile
.env.example
```

---

## 6. Setup

### 6.1 Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for local pytest / scripts)
- Node 20 (only if developing frontend outside Docker)
- Telegram Bot token & chat ID (optional; defaults to dry-run)

### 6.2 Clone & Configure
```bash
git clone <your repo> marketpulse-x
cd marketpulse-x
cp .env.example .env
```

Populate `.env` (see table below) before launching services.

---

## 7. Configuration (`.env`)

| Key | Default | Notes |
| --- | --- | --- |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | postgres / 5432 / mpx / mpx / mpx_pass | Compose service names, also used by API workers. |
| `API_PORT` | 8080 | FastAPI port exposed to host. |
| `DATA_SOURCE` | `mock` | `mock` = synthetic markets, `real` = Polymarket Gamma + CLOB. |
| `SERVICE_ROLE` | `api` | Controls process behavior (`api`, `ingestor`, `worker`, `all`). |
| `TELEGRAM_ENABLED` | false | When false or missing tokens → dry-run logging only. |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | empty | Provided by BotFather + `getUpdates`. |
| `ADMIN_API_TOKEN` | `change-me` | Optional header for `POST /api/rules`. |
| `EXEC_MODE` | `semi_auto` | Execution policy default. |
| `EXEC_MAX_NOTIONAL_PER_ORDER` | 200 | USD-equivalent notional limit. |
| `EXEC_MAX_CONCURRENT_ORDERS` | 2 | Open intent concurrency limit. |
| `EXEC_MAX_DAILY_NOTIONAL` | 1000 | Daily notional cap. |
| `EXEC_SLIPPAGE_BPS` | 80 | Guardrail slippage tolerance (0.80%). |

### Switching to real Polymarket data
1. Set `DATA_SOURCE=real` in `.env`.
2. Ensure outbound HTTPS access to `gamma-api.polymarket.com` + `clob.polymarket.com`.
3. Restart services: `docker compose restart api ingestor worker`.

---

## 8. Running the Stack

### 8.1 Make targets
```
make up        # docker compose up -d --build
make migrate   # docker compose exec api python -m backend.scripts.migrate
make seed      # docker compose exec api python scripts/seed_mock_data.py
make smoke     # python3 -m pytest --cov=backend --cov-report=term-missing -q
make logs      # docker compose logs -f --tail=200
make down      # docker compose down -v
```

### 8.2 First-time bootstrap
```bash
make up
make migrate
make seed          # optional for mock mode
```

Once services are up:
- API: http://localhost:8080 (e.g., `/api/healthz`, `/api/markets`, `/api/signals`)
- Frontend: http://localhost:5173
- Adminer: http://localhost:8090 (default creds from `.env`)

---

## 9. Testing & Backtesting

- **Unit & integration tests**: `python3 -m pytest --cov=backend --cov-report=term-missing`
- **Smoke expectations**:
  - `/api/healthz` → `db:"ok"`, `rules_heartbeat:"ok"`
  - `/api/markets` returns non-empty list
  - `/api/signals?level=P1` shows ≥1 entry within a few minutes
- **Backtest & replay**:
  ```bash
  python3 scripts/backtest_rules.py \
    --rule DUTCH_BOOK_DETECT \
    --start 2025-01-01T00:00:00Z \
    --end 2025-01-02T00:00:00Z \
    --csv-out dutch_hits.csv \
    --speed 5
  ```
  Produces CSV of hits and optional accelerated replay logs.

---

## 10. Rule Engine & Execution Flow

1. **Ingestion** polls Gamma `/markets`, `/markets/{id}` + CLOB `/book?token_id=...`. Order books are cached (TTL 5s) to minimise load; ticks are deduped before insert.
2. **Rules Engine** loads YAML DSL, fetches最新ticks +5分钟窗口，再评估以下核心规则：
   - `SPIKE_DETECT`：在指定 window 内的价格变动超过阈值。
   - `ENDGAME_SWEEP`：临近收盘、高赔率配合成交量飙升的机会。
   - `DUTCH_BOOK_DETECT`：篮子概率和 < 阈值的 dutch book 机会。
   - `CROSS_MARKET_MISPRICE`：同一 outcome（标签匹配）在不同市场出现明显价差。
   - **ML Fusion**：LightGBM 对实时特征推断套利概率，再与规则信号融合成 `edge_score`。
3. **Signal persistence**: each signal writes to DB with `edge_score`, `payload_json`, and audit logs; KPI daily aggregates update concurrently.
4. **Alerting**: Telegram notifier debounces duplicate alerts and falls back to dry-run logging if disabled.
5. **Execution**: UI or API can create intents for P1/P2 signals. Intent creation uses rule-specific heuristics (basket sizing, slippage clamps). Confirmation triggers limit + guardrail checks; mock mode marks intents as filled.

---

## 11. Observability & Runbook

- **Health check**: `GET /api/healthz` (status/db/rules heartbeat).
- **Metrics** (`GET /metrics`):
  - `mpx_ingest_latency_ms{source=...}`
  - `mpx_ingest_last_tick_timestamp{source=...}`
  - `mpx_rule_eval_ms`
  - `mpx_signals_total{rule=...}`
  - `mpx_order_intents_total{status=...}`
  - `mpx_telegram_failures_total`
  - `mpx_requests_total`, `mpx_health`
- **Runbook (dashboard card + README quick notes)**:
  - *No signals*: check worker logs + `/metrics` `mpx_rule_eval_ms`.
  - *Markets stale*: inspect ingestor logs, confirm `mpx_ingest_last_tick_timestamp` advancing, verify data source connectivity.
  - *Telegram silent*: ensure `.env` tokens set, `TELEGRAM_ENABLED=true`, run `curl -X POST http://localhost:8080/api/alerts/test -H 'Content-Type: application/json' -d '{"text":"MarketPulse-X Telegram ✅"}'`.
  - *Execution rejected*: look at modal detail JSON (`checks.reasons`), adjust `EXEC_*` limits or ensure other intents are closed.

---

## 12. Scripts & Utilities

- `scripts/seed_mock_data.py`: bootstrap mock markets/options/ticks.
- `scripts/gen_demo_traffic.py`: generate synthetic tick bursts (demo loads).
- `scripts/backtest_rules.py`: time-window replay + CSV hit export.
- `backend/scripts/migrate.py`: applies SQL migrations (run via `make migrate`).

---

## 13. Telegram Configuration Checklist

1. Create bot via `@BotFather`, obtain token.
2. Send `/start` to the bot from your Telegram account.
3. Call `https://api.telegram.org/bot<token>/getUpdates` to find `chat.id`.
4. Set `.env`:
   ```
   TELEGRAM_ENABLED=true
   TELEGRAM_BOT_TOKEN=<token>
   TELEGRAM_CHAT_ID=<chat_id>
   ```
5. Restart `worker` container and send test alert:
   ```bash
   curl -X POST http://localhost:8080/api/alerts/test \
     -H 'Content-Type: application/json' \
     -d '{"text":"MarketPulse-X Telegram ✅"}'
   ```

---

## 14. Deployment Notes

- **Docker**: Primary distribution via the included compose file. Ensure Docker Hub connectivity for `python:3.11-slim`, `node:20-alpine`, `timescale/timescaledb:2.15`.
- **GitHub**: After cloning and filling `.env`, commit your changes (`git add -A && git commit -m "..."`) and push to your own remote.
- **Extensibility**: add new DSL files under `configs/rules/`, run `make migrate` when schema evolves, restart workers to load new rules.

---

Happy monitoring & arbitraging! For questions, inspect logs (`make logs`) or reach out via the issue tracker.***
