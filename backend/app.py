from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from backend.api import alerts, health, kpi, markets, signals
from backend.deps import lifespan, require_admin_token
from backend.metrics import REGISTRY
from backend.execution import router as execution_router
from backend.settings import get_settings
from backend.utils.rate_limit import RateLimitMiddleware, RateLimiter

request_counter = Counter("mpx_requests_total", "API requests", registry=REGISTRY)
health_gauge = Gauge("mpx_health", "Health status", registry=REGISTRY)


settings = get_settings()
app = FastAPI(title="MarketPulse-X", lifespan=lifespan)

if settings.cors_allow_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
    )

limiter = RateLimiter(
    max_requests=settings.rate_limit_requests_per_minute,
    window_seconds=settings.rate_limit_window_seconds,
)
app.add_middleware(
    RateLimitMiddleware,
    limiter=limiter,
    exempt_paths=("/api/healthz", "/metrics"),
)
app.include_router(health.router)
app.include_router(markets.router)
app.include_router(signals.router)
app.include_router(execution_router.router)
app.include_router(alerts.router)
app.include_router(kpi.router)


@app.middleware("http")
async def record_metrics(request, call_next):  # pragma: no cover - instrumentation
    response = await call_next(request)
    request_counter.inc()
    if response.status_code == 200:
        health_gauge.set(1)
    return response


@app.get("/metrics")
async def metrics(_: str = Depends(require_admin_token)):
    if not settings.metrics_enabled:
        raise HTTPException(status_code=404, detail="metrics disabled")
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
