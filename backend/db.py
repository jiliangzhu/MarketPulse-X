from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Coroutine, Optional

import asyncpg


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        async with self._pool.acquire() as conn:
            yield conn

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args: Any) -> str:
        async with self.connection() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_list: list[tuple[Any, ...]]) -> None:
        if not args_list:
            return
        async with self.connection() as conn:
            await conn.executemany(query, args_list)

    async def transaction(self, func: Callable[[asyncpg.Connection], Coroutine[Any, Any, Any]]) -> Any:
        async with self.connection() as conn:
            async with conn.transaction():
                return await func(conn)


async def run_sync(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
