from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class CircuitBreaker:
    threshold: int = 3
    cooldown_secs: int = 300

    def __post_init__(self) -> None:
        self._state: Dict[Tuple[str, str], Tuple[int, float]] = {}

    def record_failure(self, rule: str, market_id: str) -> bool:
        key = (rule, market_id)
        count, ts = self._state.get(key, (0, 0))
        now = time.time()
        if now - ts > self.cooldown_secs:
            count = 0
        count += 1
        self._state[key] = (count, now)
        return count >= self.threshold

    def reset(self, rule: str, market_id: str) -> None:
        self._state.pop((rule, market_id), None)

    def is_open(self, rule: str, market_id: str) -> bool:
        count, ts = self._state.get((rule, market_id), (0, 0))
        if count >= self.threshold and (time.time() - ts) < self.cooldown_secs:
            return True
        if time.time() - ts >= self.cooldown_secs:
            self._state.pop((rule, market_id), None)
        return False
