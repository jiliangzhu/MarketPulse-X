# MarketPulse-X

## 自检摘要
- ✅ `make up && make migrate && make seed`：TimescaleDB 扩展 + schema 最新化，api / ingestor / worker / frontend / adminer 均可无误启动。
- ✅ `make smoke`：`python3 -m pytest --cov=backend --cov-report=term-missing`，20 个测试全部通过，当前覆盖率 **62%**。
- ✅ `/api/healthz`：返回 `{"status":"ok","db":"ok","rules_heartbeat":"ok"}`，规则心跳 <30s；`app.state.ingestion_last_run` 也映射到 `/metrics`。
- ✅ `/metrics`：`mpx_ingest_latency_ms`、`mpx_ingest_last_tick_timestamp`、`mpx_rule_eval_ms`、`mpx_signals_total`、`mpx_order_intents_total`、`mpx_telegram_failures_total`… Prometheus 可直接 scrape。
- ✅ Telegram dry-run：`TELEGRAM_ENABLED=false` 或缺 token/chatId 时，`/api/alerts/test` → `{"status":"dry-run"}`，信号 payload 记录 `transport=telegram-dry-run`。
- ✅ React 前端：Dashboard 展示 KPI → Signal Stream（含分页、edge score、下单弹窗）→ Markets（分页/页量选择），详情页含 sparkline + 相似市场区块。

> 明早验收：把 `.env` 中 `TELEGRAM_ENABLED=true`、填入 Bot Token/ChatID 后，`curl -X POST http://localhost:8080/api/alerts/test -d '{"text":"MarketPulse-X Telegram ✅"}' -H 'Content-Type: application/json'` 即可触发真实推送。

---

## Phase 2 功能快照
| 模块 | 亮点 |
| --- | --- |
| **执行/风控** | `backend/execution/*` + `backend/risk/*`，支持基于 P1/P2 信号生成半自动意向单、额度/日净额/滑点校验、熔断、模拟成交；前端信号流可一键生成 & 确认。|
| **智能套利规则** | 新增 `DUTCH_BOOK_DETECT`、`CROSS_MARKET_MISPRICE`、`TREND_BREAKOUT`，所有规则都会输出 `edge_score`/`estimated_edge_bps`，API + UI 均可排序/展示。|
| **同义市场聚合** | `configs/synonyms.yml` + `synonym_group*` 表，`SynonymMatcher` 根据关键词/词典自动建群；`SYNONYM_MISPRICE` + Cross-market 规则捕捉组内/跨市场错价，市场详情展示相似标的。|
| **回测与重放** | `scripts/backtest_rules.py --rule ... --speed 5` 支持历史 tick 回放与 CSV 命中输出。`rule_kpi_daily` 存 7 日 KPI，Dashboard KPI 卡片实时展示触发数/边际。|
| **采集 & 可观测性** | 实时源改为 Gamma + CLOB 订单簿组合，内存 TTL 缓存（热市场 5s 内不重复请求），`mpx_ingest_last_tick_timestamp` 衡量最新写入；Runbook 覆盖采集中断、规则沉默、告警未达排查步骤。|

---

## 目录
```
marketpulse-x/
├─ backend/
│   ├─ api/               # health/markets/signals/alerts/kpi/execution REST
│   ├─ alerting/          # Telegram notifier (dry-run + retry + metrics)
│   ├─ execution/         # executor + OEMS router + policy bootstrap
│   ├─ risk/              # limits / guardrails / circuit breaker
│   ├─ ingestion/         # polymarket client + mock/real sources
│   ├─ processing/        # stream processor, rules engine, synonym matcher
│   ├─ repo/              # 数据仓储、KPI、执行策略等
│   ├─ workers/           # `python -m backend.workers.{ingestor,rules_worker}`
│   ├─ app.py + deps.py   # FastAPI 入口与生命周期
│   └─ settings.py        # `.env` 映射
├─ configs/
│   ├─ app.yaml           # 采样/调度参数
│   ├─ rules/*.yaml       # DSL（SUM_LT_1 / SPIKE / ENDGAME / SYNONYM / DUTCH_BOOK / CROSS_MARKET / TREND_BREAKOUT）
│   └─ synonyms.yml       # 同义市场词典
├─ migrations/
│   ├─ 001_init.sql       # Timescale schema
│   └─ 002_phase2.sql     # execution_policy/order_intent/synonym/rule_kpi
├─ frontend/              # Vite + React + TS
├─ scripts/
│   ├─ seed_mock_data.py
│   ├─ gen_demo_traffic.py
│   └─ backtest_rules.py  # 回测/事件重放
├─ tests/                 # pytest + httpx 客户端
├─ docker-compose.yml     # postgres/api/ingestor/worker/frontend/adminer
├─ Makefile               # up/migrate/seed/smoke/logs/down
└─ .env.example
```

---

## 环境准备
1. Docker + Docker Compose
2. Python >=3.11（可本地运行 `pytest` / backtest 脚本）
3. Node 20（若单独运行前端开发服务器）

复制环境后：
```bash
cp .env.example .env
```

`.env` 关键项：
```ini
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=mpx
POSTGRES_USER=mpx
POSTGRES_PASSWORD=mpx_pass
API_PORT=8080
DATA_SOURCE=mock                 # mock | real
SERVICE_ROLE=api                 # api | ingestor | worker | all

TELEGRAM_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
ADMIN_API_TOKEN=

EXEC_MODE=semi_auto
EXEC_MAX_NOTIONAL_PER_ORDER=200
EXEC_MAX_CONCURRENT_ORDERS=2
EXEC_MAX_DAILY_NOTIONAL=1000
EXEC_SLIPPAGE_BPS=80
```
> `SERVICE_ROLE` 由 docker-compose 控制：`api` 仅暴露 HTTP；`ingestor` 运行 `backend.workers.ingestor`；`worker` 运行规则引擎。`all` 适合本地单进程调试。
>
> 切换 Polymarket 实盘：把 `.env` 的 `DATA_SOURCE=real`，然后 `docker compose restart api ingestor worker`（ingestor 会自动使用 Gamma + CLOB 组合源）。

---

## 一键命令
| 命令 | 说明 |
| --- | --- |
| `make up` | 构建镜像，启动 postgres + api + ingestor + worker + frontend + adminer |
| `make migrate` | `docker compose exec api python -m backend.scripts.migrate`，创建/升级 Timescale schema |
| `make seed` | `docker compose exec api python scripts/seed_mock_data.py`，写入 mock 市场与第一帧 tick |
| `make smoke` | `python3 -m pytest --cov=backend --cov-report=term-missing -q` |
| `make logs` | `docker compose logs -f --tail=200` |
| `make down` | `docker compose down -v` |

---

## 服务拓扑
```
postgres (TimescaleDB)
│
├─ api        -> FastAPI (健康/市场/信号/API/执行/kpi)，只读 DB
├─ ingestor   -> backend.workers.ingestor (uvloop + 并行 poll + 缓存去重)
├─ worker     -> backend.workers.rules_worker (规则 DSL + Telegram 通知)
├─ frontend   -> React (Vite dev server, 5173)
└─ adminer    -> DB UI（8090）
```
- Ingestor 使用指数退避 + N 份 chunk 并行轮询 + 内存缓存（无变化不落库），`ingest_latency_ms{source=...}` 指示延迟。
- Rules worker 加载 DSL（`configs/rules/*.yaml`），所有信号落入 `signal` + `rule_kpi_daily`，并通过 `TelegramNotifier`（默认 dry-run）推送。

---

## Phase 2 模块
### 1. 执行与风控
- **API**：
  - `POST /api/execution/intent`：输入 `signal_id`（P1/P2）生成 `order_intent`（状态 `suggested`）。
  - `POST /api/execution/confirm/{intent_id}`：调用风控（额度/并发/日净额 + 滑点 guardrail + 熔断）后发送；mock 源直接 `filled`。
  - `GET /api/execution/intents?status=pending`：查询历史意向单。
- **后端**：
  - `backend/execution/executor.py` 负责二次校验和状态迁移。
  - `backend/risk/limits.py`/`guardrails.py`/`circuit_breaker.py`：额度、滑点、熔断。
  - `execution_policy` 表保存默认策略，`order_intent` 存意向单；`oems.bootstrap_policy` 会按 `.env` 自动建默认策略。
- **前端**：信号流中“下单”按钮 → Modal 显示风控结论，可一键确认。

### 2. 同义/等价市场聚合
- `configs/synonyms.yml` 提供关键词/显式列表。
- `SynonymMatcher` 每次规则周期自动刷新 `synonym_group` & `synonym_group_member`。
- 新规则 `SYNONYM_MISPRICE` 检测组内 >2.5% 价差，payload 包含 `leader/laggard/gap`。
- 市场详情 `/api/markets/{id}` 返回 `synonyms`，前端展示互链。

### 3. 回测 & 事件重放
- `scripts/backtest_rules.py --rule SUM_LT_1 --start 2024-03-01T00:00:00Z --end ... --speed 5 --csv-out hits.csv`
  - 从 Timescale `tick` 回放窗口，复用规则逻辑计算命中并输出 CSV。
  - `--speed`>0 时按 1x/5x 节奏 sleep，可观察规则轨迹。
- `rule_kpi_daily` 日表记录每种规则的信号计数、P1 数、平均 gap/edge；Dashboard KPI 卡片按规则聚合 7 日数据。

### 4. 可观测性
- `/metrics` 暴露：
  - `mpx_ingest_latency_ms{source=mock}`
  - `mpx_rule_eval_ms`
  - `mpx_signals_total{rule="SUM_LT_1"}`
  - `mpx_order_intents_total{status="filled"}`
  - `mpx_telegram_failures_total`
- 结构化 JSON 日志包含 `ruleId/marketId/trace` 字段，方便 ELK / Loki。

---

## 数据模型补充
- `execution_policy`：执行策略参数（mode、notional、并发、slippage）。
- `order_intent`：意向单（signal -> suggested -> sent/filled）。
- `synonym_group` + `synonym_group_member`：同义市场分组。
- `rule_kpi_daily`：按日统计规则命中 / 边际。
- `signal.edge_score`：归一化套利边际（sum gap、dutch edge、breakout delta），供 UI / 执行模块排序。

---

## API 速查
| Endpoint | 描述 |
| --- | --- |
| `GET /api/healthz` | 健康检查（DB + 最近信号时间） |
| `GET /api/markets` / `/{id}` | 市场列表/详情（含 sparkline & synonyms） |
| `GET /api/signals` | 信号流 (level filter + since) |
| `POST /api/rules` | 上传 DSL（`x-api-key: ADMIN_API_TOKEN`） |
| `GET /metrics` | Prometheus 文本 |
| `POST /api/alerts/test` | Telegram 测试消息（可在 dry-run 下自测） |
| `GET /api/kpi/daily` | 近 7 日规则 KPI |
| `POST /api/execution/intent` | 生成意向单 |
| `POST /api/execution/confirm/{intent_id}` | 确认/模拟执行 |
| `GET /api/execution/intents` | 意向单列表 |

---

## 前端入口
- `http://localhost:5173` 默认展示：
  - 左列：KPI 卡片 + 信号流（含“下单”弹窗）+ Health 信息。
  - 右列：市场列表（价格快照、状态 Badges）。
- `/markets/:id`：3 分钟 sparkline、选项价格表、相似市场链接。

---

## Telegram 配置
同 MVP 阶段，但新增测试入口：
```bash
curl -X POST http://localhost:8080/api/alerts/test \
  -H 'Content-Type: application/json' \
  -d '{"text":"MarketPulse-X Telegram ✅"}'
```
- Dry-run：`TELEGRAM_ENABLED=false` 或缺 Token/ChatID 时仍落库，并在 `signal.payload_json.transport = telegram-dry-run`。
- 正式：设置 `.env` 并 `docker compose restart worker`（规则 worker 中的 notifier 会读取最新配置）。

---

## 回测 / 重放
```bash
python3 scripts/backtest_rules.py \
  --rule SUM_LT_1 \
  --start 2024-03-01T00:00:00Z \
  --end   2024-03-01T01:00:00Z \
  --speed 5 \
  --csv-out out/hits.csv
```
- `--rule`：规则类型（与 DSL `type` 一致）。
- `--speed`：>0 时按 `Δt/speed` sleep，可观察事件重放；0 表示快速计算。
- CSV 字段：`ts, market_id, message, score`。

---

## Runbook（常见故障排查）
| 场景 | 排查步骤 |
| --- | --- |
| **采集中断** | `docker compose logs ingestor` 查看 `stream-error`；若连续退避 >30s，检查 Polymarket API / 网络；确认 `DATA_SOURCE`；可运行 `python scripts/gen_demo_traffic.py` 造流。|
| **规则沉默** | `docker compose logs worker` 查看 `rules-loaded` & `rules-engine`；确认 `configs/rules/*.yaml` `enabled=true` 且 mock tick 在写入；查询 `rule_kpi_daily` 是否更新；查看 `/metrics` 中 `mpx_rule_eval_ms` 是否持续增量。|
| **告警未达** | `worker` 日志如有 `telegram-error`，检查 `.env`；`/api/alerts/test` 是否返回 `sent`；`/metrics` 的 `mpx_telegram_failures_total` 是否 >0；若网断，可保留 dry-run 并明早补发。|

---

## 扩展路线
1. **消息中间件**：把 `StreamProcessor` 输出推送到 Kafka，再由多 worker 消费。
2. **Embedding 同义**：在 `SynonymMatcher` 中注入向量数据库，支持跨语言更精准的市场聚合。
3. **自动执行**：扩展 `EXEC_MODE=auto`，串接真实交易所 API & 三级风控（额度/策略/账户）。
4. **Prometheus + Grafana**：基于现有 `/metrics` 可快速接入 Grafana dashboard（ingest latency、rule delay、intent status、telegram failures）。
5. **高级回测**：在 `scripts/backtest_rules.py` 内加入 PnL 模拟、策略参数扫描。

---

祝验收顺利，如需更多 Phase 2/3 功能（Kafka、多账户、自动下单、风控可视化）可在此基础上继续扩展。
