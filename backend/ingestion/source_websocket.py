from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
import contextlib
from typing import Any, Dict, List

try:  # pragma: no cover - optional dependency
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None  # type: ignore

from backend.utils.logging import get_logger


websocket_available = websockets is not None


class WebSocketMarketSource:
    WEBSOCKET_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, asset_to_market_map: dict[str, str]) -> None:
        self.asset_to_market_map = asset_to_market_map
        self.logger = get_logger("ws-market-source")

    async def run(self, data_queue: asyncio.Queue, all_asset_ids: list[str], chunk_size: int = 100) -> None:
        if not websocket_available:
            raise RuntimeError("websockets package not installed")
        if not all_asset_ids:
            self.logger.warning("ws-no-assets")
            return
        chunks = [all_asset_ids[i : i + chunk_size] for i in range(0, len(all_asset_ids), chunk_size)]
        self.logger.info(
            "ws-chunks-created",
            extra={"chunks": len(chunks), "assets": len(all_asset_ids), "chunk_size": chunk_size},
        )
        tasks = [asyncio.create_task(self._run_connection(chunk, data_queue)) for chunk in chunks]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _run_connection(self, asset_ids: list[str], data_queue: asyncio.Queue) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.WEBSOCKET_URI, ping_interval=None) as socket:
                    await socket.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
                    self.logger.info(
                        "ws-subscribed",
                        extra={"assets": len(asset_ids)},
                    )
                    ping_task = asyncio.create_task(self._ping_loop(socket))
                    try:
                        async for message in socket:
                            await self._handle_message(message, data_queue)
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await ping_task
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.exception("ws-connection-error")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            backoff = 1

    async def _ping_loop(self, socket: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(10)
            try:
                await socket.send("PING")
            except Exception:  # pragma: no cover - connection closed
                return

    async def _handle_message(self, message: str, data_queue: asyncio.Queue) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            self.logger.warning("ws-bad-json", extra={"payload": message[:200]})
            return
        if isinstance(payload, list):
            for item in payload:
                await self._handle_event(item, data_queue)
        else:
            await self._handle_event(payload, data_queue)

    async def _handle_event(self, data: dict[str, Any], data_queue: asyncio.Queue) -> None:
        if not isinstance(data, dict):
            return
        event_type = data.get("event_type")
        if event_type in {"price_change", "last_trade_price"}:
            await self._parse_ticks(data, data_queue)
        elif event_type == "book":
            self.logger.info("ws-book-snapshot", extra={"asset_id": data.get("asset_id")})

    async def _parse_ticks(self, data: dict[str, Any], data_queue: asyncio.Queue) -> None:
        timestamp = data.get("timestamp")
        if timestamp is None:
            return
        try:
            ts_value = float(timestamp)
        except (TypeError, ValueError):
            return
        ts = datetime.fromtimestamp(ts_value / 1000, tz=timezone.utc)
        event_type = data.get("event_type")
        ticks_list: list[dict[str, Any]] = []
        if event_type == "price_change":
            for change in data.get("price_changes", []):
                asset_id = change.get("asset_id")
                if not asset_id:
                    continue
                market_id = self.asset_to_market_map.get(asset_id)
                if not market_id:
                    continue
                price = self._to_float(change.get("price"))
                best_bid = self._to_float(change.get("best_bid"))
                best_ask = self._to_float(change.get("best_ask"))
                if price == 0 and best_bid and best_ask:
                    price = (best_bid + best_ask) / 2
                liquidity = self._derive_liquidity(change)
                volume = self._to_float(change.get("size"))
                tick = {
                    "ts": ts,
                    "market_id": market_id,
                    "option_id": asset_id,
                    "price": price or None,
                    "best_bid": best_bid or None,
                    "best_ask": best_ask or None,
                    "liquidity": liquidity,
                    "volume": volume or None,
                }
                ticks_list.append(tick)
        elif event_type == "last_trade_price":
            asset_id = data.get("asset_id")
            if not asset_id:
                return
            market_id = self.asset_to_market_map.get(asset_id)
            if not market_id:
                return
            tick = {
                "ts": ts,
                "market_id": market_id,
                "option_id": asset_id,
                "price": self._to_float(data.get("price")) or None,
                "best_bid": None,
                "best_ask": None,
                "liquidity": self._derive_liquidity(data),
                "volume": self._to_float(data.get("size")) or None,
            }
            ticks_list.append(tick)
        if ticks_list:
            await data_queue.put(ticks_list)

    def _to_float(self, value: Any) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _derive_liquidity(self, payload: dict[str, Any]) -> float | None:
        candidates = [
            payload.get("liquidity"),
            payload.get("size"),
            payload.get("best_bid_size"),
            payload.get("best_ask_size"),
            payload.get("volume"),
        ]
        for candidate in candidates:
            value = self._to_float(candidate)
            if value > 0:
                return value
        return None
