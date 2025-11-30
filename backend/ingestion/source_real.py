from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from cachetools import TTLCache

from backend.utils.logging import get_logger

from .polymarket_client import MarketDataSource


class RealPolymarketSource(MarketDataSource):
    BASE_URL = "https://gamma-api.polymarket.com"
    CLOB_URL = "https://clob.polymarket.com"
    MAX_MARKETS = 500
    DETAIL_TTL = 120
    ORDERBOOK_TTL = 5
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 0.5

    def __init__(
        self,
        *,
        gamma_client: Optional[httpx.AsyncClient] = None,
        clob_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._gamma = gamma_client or httpx.AsyncClient(timeout=10.0)
        self._clob = clob_client or httpx.AsyncClient(timeout=10.0)
        self._detail_cache: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=512, ttl=self.DETAIL_TTL)
        self._orderbook_cache: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=2048, ttl=self.ORDERBOOK_TTL)
        self._logger = get_logger("polymarket-real")

    async def aclose(self) -> None:
        await self._gamma.aclose()
        await self._clob.aclose()

    async def list_markets(self) -> List[Dict[str, Any]]:
        payload = await self._request(
            self._gamma,
            "GET",
            f"{self.BASE_URL}/markets",
            params={"limit": self.MAX_MARKETS, "offset": 0, "closed": "false"},
        )
        data = payload.get("markets", payload) if isinstance(payload, dict) else payload
        markets: list[dict[str, Any]] = []
        for item in data[: self.MAX_MARKETS]:
            markets.append(
                {
                    "market_id": str(item.get("id")),
                    "title": item.get("question") or item.get("title"),
                    "status": "closed" if item.get("closed") else "active",
                    "starts_at": self._parse_iso(item.get("startDate")),
                    "ends_at": self._parse_iso(item.get("endDate")),
                    "platform": "polymarket",
                    "tags": item.get("categories") or item.get("tags") or [],
                }
            )
        return markets

    async def list_options(self, market_id: str) -> List[Dict[str, Any]]:
        detail = await self._get_market_detail(market_id)
        payload: list[dict[str, Any]] = []
        for option in detail.get("options", []):
            payload.append(
                {
                    "option_id": option["option_id"],
                    "market_id": market_id,
                    "label": option["label"],
                }
            )
        return payload

    async def poll_ticks(self, market_ids: list[str]) -> List[Dict[str, Any]]:
        tasks = [self._market_ticks(market_id) for market_id in market_ids]
        results = await asyncio.gather(*tasks)
        ticks: list[dict[str, Any]] = []
        for bucket in results:
            ticks.extend(bucket)
        return ticks

    async def _market_ticks(self, market_id: str) -> List[Dict[str, Any]]:
        try:
            detail = await self._get_market_detail(market_id)
        except Exception as exc:  # pragma: no cover - defensive logging
            self._logger.warning("market-detail-error", extra={"market_id": market_id, "error": str(exc)})
            return []
        options = detail.get("options", [])
        if not options:
            return []
        books = await asyncio.gather(*[self._fetch_orderbook(opt.get("token_id")) for opt in options])
        ticks: list[dict[str, Any]] = []
        for option, book in zip(options, books):
            best_bid = self._best_price(book, side="bid") if book else None
            best_ask = self._best_price(book, side="ask") if book else None
            price = self._resolve_price(option.get("price"), best_bid, best_ask)
            ts = self._book_ts(book)
            ticks.append(
                {
                    "ts": ts,
                    "market_id": market_id,
                    "option_id": option["option_id"],
                    "price": price,
                    "volume": detail.get("volume"),
                    "liquidity": detail.get("liquidity"),
                    "best_bid": best_bid if best_bid is not None else price,
                    "best_ask": best_ask if best_ask is not None else price,
                }
            )
        return ticks

    async def _fetch_orderbook(self, token_id: Optional[str]) -> Optional[dict[str, Any]]:
        if not token_id:
            return None
        try:
            return await self._get_orderbook(token_id)
        except Exception as exc:  # pragma: no cover - defensive logging
            self._logger.warning("orderbook-error", extra={"token_id": token_id, "error": str(exc)})
            return None

    async def _get_market_detail(self, market_id: str) -> dict[str, Any]:
        cached = self._detail_cache.get(market_id)
        if cached:
            return cached
        payload = await self._request(self._gamma, "GET", f"{self.BASE_URL}/markets/{market_id}")
        normalized = self._normalize_detail(payload)
        self._detail_cache[market_id] = normalized
        return normalized

    async def _get_orderbook(self, token_id: str) -> dict[str, Any]:
        cached = self._orderbook_cache.get(token_id)
        if cached:
            return cached
        payload = await self._request(
            self._clob,
            "GET",
            f"{self.CLOB_URL}/book",
            params={"token_id": token_id},
        )
        self._orderbook_cache[token_id] = payload
        return payload

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Any:
        delay = self.RETRY_BASE_DELAY
        attempt = 0
        while True:
            try:
                resp = await client.request(method, url, **kwargs)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                attempt += 1
                if attempt >= self.MAX_RETRIES:
                    self._logger.error("polymarket-request-failed", extra={"url": url, "error": str(exc)})
                    raise
                await asyncio.sleep(delay)
                delay *= 2

    def _normalize_detail(self, payload: dict[str, Any]) -> dict[str, Any]:
        outcomes = self._parse_outcomes(payload.get("outcomes"))
        tokens = self._parse_list(payload.get("clobTokenIds"))
        outcome_prices = self._parse_float_list(payload.get("outcomePrices"))
        options: list[dict[str, Any]] = []
        for idx, label in enumerate(outcomes):
            token_id = tokens[idx] if idx < len(tokens) else None
            option_id = token_id or f"{payload.get('id')}-{idx}"
            price = outcome_prices[idx] if idx < len(outcome_prices) else None
            options.append(
                {
                    "option_id": option_id,
                    "token_id": token_id,
                    "label": label,
                    "price": price,
                }
            )
        return {
            "market_id": str(payload.get("id")),
            "title": payload.get("question") or payload.get("title"),
            "status": "closed" if payload.get("closed") else "active",
            "starts_at": self._parse_iso(payload.get("startDate")),
            "ends_at": self._parse_iso(payload.get("endDate")),
            "options": options,
            "liquidity": self._to_float(payload.get("liquidityClob") or payload.get("liquidity")),
            "volume": self._to_float(payload.get("volume24hrClob") or payload.get("volume24hr")),
        }

    def _resolve_price(
        self,
        option_price: Optional[float],
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> float:
        mid = self._mid(best_bid, best_ask)
        if mid is None:
            mid = option_price if option_price is not None else 0.5
        elif option_price is not None:
            mid = (mid + option_price) / 2
        return round(float(mid), 4)

    def _mid(self, bid: Optional[float], ask: Optional[float]) -> Optional[float]:
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        if bid is not None:
            return bid
        if ask is not None:
            return ask
        return None

    def _best_price(self, book: dict[str, Any], *, side: str) -> Optional[float]:
        if not book:
            return None
        rows = book.get("bids" if side == "bid" else "asks")
        if not rows:
            return None
        prices: list[float] = []
        for row in rows:
            try:
                prices.append(float(row.get("price")))
            except (TypeError, ValueError):
                continue
        if not prices:
            return None
        return max(prices) if side == "bid" else min(prices)

    def _book_ts(self, book: Optional[dict[str, Any]]) -> datetime:
        if not book:
            return datetime.now(timezone.utc)
        ts = book.get("timestamp")
        try:
            if ts is None:
                raise ValueError("missing timestamp")
            return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
        except (ValueError, TypeError):
            return datetime.now(timezone.utc)

    def _parse_outcomes(self, raw: Any) -> List[str]:
        data = self._parse_list(raw)
        return data or ["Yes", "No"]

    def _parse_list(self, raw: Any) -> List[str]:
        if not raw:
            return []
        if isinstance(raw, list):
            return [str(item) for item in raw]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except json.JSONDecodeError:
                return [raw]
        return []

    def _parse_float_list(self, raw: Any) -> List[float]:
        floats: list[float] = []
        for item in self._parse_list(raw):
            try:
                floats.append(float(item))
            except (TypeError, ValueError):
                continue
        return floats

    def _to_float(self, value: Any) -> float:
        try:
            if value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _parse_iso(self, value: Any):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
