from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from typing import ClassVar, Deque, Dict, Optional, Tuple

try:  # pragma: no cover - optional dependency in tests
    import websockets
except Exception:  # pragma: no cover
    websockets = None  # type: ignore

from backend.utils.logging import get_logger


@dataclass
class PriceSnapshot:
    """缓存 Binance 行情，包含 1 秒收益率。"""

    price: float
    return_1s: float
    ts: float


class BinancePriceCache:
    """Binance 现货行情缓存（单例）。"""

    STREAMS: ClassVar[Dict[str, str]] = {
        "btcusdt": "BTC",
        "ethusdt": "ETH",
        "solusdt": "SOL",
    }
    WS_URI: ClassVar[str] = "wss://stream.binance.us:9443/ws"
    _instance: ClassVar[Optional["BinancePriceCache"]] = None

    def __init__(self) -> None:
        self.logger = get_logger("binance-feed")
        self._state: Dict[str, PriceSnapshot] = {}
        self._history: Dict[str, Deque[Tuple[float, float]]] = {
            symbol: deque(maxlen=500) for symbol in self.STREAMS.values()
        }
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    @classmethod
    def get_instance(cls) -> "BinancePriceCache":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def ensure_running(self) -> None:
        """在当前事件循环中启动后台任务。"""
        loop = asyncio.get_running_loop()
        if self._task is None or self._task.done():
            self._task = loop.create_task(self._run(), name="binance-price-feed")

    async def _run(self) -> None:
        params = [f"{stream}@trade" for stream in self.STREAMS.keys()]
        subscribe_payload = json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1})
        while True:
            if websockets is None:
                self.logger.warning("binance-feed-disabled", extra={"reason": "websockets-missing"})
                await asyncio.sleep(5)
                continue
            try:
                self.logger.info("binance-feed-connecting", extra={"uri": self.WS_URI})
                async with websockets.connect(
                    self.WS_URI,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(subscribe_payload)
                    self.logger.info("binance-feed-subscribed", extra={"uri": self.WS_URI})
                    async for raw_msg in ws:
                        await self._handle_message(raw_msg)
            except Exception as exc:  # pragma: no cover - 网络异常兜底
                self.logger.warning("binance-feed-retry", extra={"error": repr(exc), "uri": self.WS_URI})
                await asyncio.sleep(1)

    async def _handle_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            if payload.get("e") != "trade":
                return
            stream_symbol = payload.get("s", "").lower()
            asset = self.STREAMS.get(stream_symbol)
            if not asset:
                return
            price = float(payload.get("p", 0.0))
            ts_ms = payload.get("T") or int(time.time() * 1000)
            ts = ts_ms / 1000
        except Exception as exc:  # pragma: no cover - 解包异常
            self.logger.error("binance-feed-parse", extra={"error": str(exc)})
            return
        await self._update_state(asset, price, ts)

    async def _update_state(self, asset: str, price: float, ts: float) -> None:
        async with self._lock:
            history = self._history[asset]
            history.append((ts, price))
            cutoff = ts - 1.0
            base_price = price
            while history and history[0][0] < cutoff:
                base_price = history[0][1]
                history.popleft()
            if history:
                base_price = history[0][1]
            return_1s = (price - base_price) / base_price if base_price else 0.0
            self._state[asset] = PriceSnapshot(price=price, return_1s=return_1s, ts=ts)

    def get_price_data(self, symbol: str) -> Optional[PriceSnapshot]:
        snapshot = self._state.get(symbol.upper())
        if not snapshot:
            return None
        # 返回浅拷贝，避免调用方修改内部缓存
        return PriceSnapshot(price=snapshot.price, return_1s=snapshot.return_1s, ts=snapshot.ts)
