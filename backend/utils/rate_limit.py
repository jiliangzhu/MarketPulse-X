from __future__ import annotations

from collections import defaultdict, deque
from time import monotonic
from typing import Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._records: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        now = monotonic()
        records = self._records[key]
        while records and now - records[0] > self.window_seconds:
            records.popleft()
        if len(records) >= self.max_requests:
            return False
        records.append(now)
        return True


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        limiter: RateLimiter,
        exempt_paths: Iterable[str] | None = None,
    ):
        super().__init__(app)
        self.limiter = limiter
        self.exempt_paths = tuple(exempt_paths or ())

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if any(path.startswith(prefix) for prefix in self.exempt_paths):
            return await call_next(request)
        client_id = request.client.host if request.client else "unknown"
        if not self.limiter.allow(client_id):
            return JSONResponse(status_code=429, content={"detail": "rate limit exceeded"})
        return await call_next(request)
