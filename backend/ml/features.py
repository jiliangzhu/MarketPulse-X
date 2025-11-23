from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any, Dict, Iterable, List, Optional


def extract_features_realtime(
    market: dict[str, Any],
    latest_ticks: dict[str, dict[str, Any]],
    recent_ticks: List[dict[str, Any]],
    synonym_peers: List[dict[str, Any]] | None = None,
) -> Optional[dict[str, Any]]:
    if not latest_ticks:
        return None
    top_tick = max(latest_ticks.values(), key=lambda t: float(t.get("volume") or 0))
    best_bid = _to_float(top_tick.get("best_bid"))
    best_ask = _to_float(top_tick.get("best_ask"))
    mid_price = _mid(best_bid, best_ask, _to_float(top_tick.get("price")))
    spread = (best_ask - best_bid) if (best_ask and best_bid) else 0.0
    volume = _to_float(top_tick.get("volume"))
    best_bid_size = volume
    best_ask_size = volume
    size_imbalance = 0.0
    if best_bid_size or best_ask_size:
        size_imbalance = (best_bid_size - best_ask_size) / max(best_bid_size + best_ask_size, 1e-6)

    zscore_spread = _spread_zscore(recent_ticks)
    price_velocity = _price_velocity(recent_ticks, window_secs=10)
    time_to_expiry = _time_to_expiry_minutes(market)
    synonym_delta = _synonym_price_delta(mid_price, synonym_peers)
    volatility_5m = _price_volatility(recent_ticks)
    days_to_expiry = _days_to_expiry(market)

    features = {
        "mid_price": mid_price,
        "spread": spread,
        "volume": volume,
        "best_bid_size": best_bid_size,
        "best_ask_size": best_ask_size,
        "size_imbalance": size_imbalance,
        "zscore_spread_5min": zscore_spread,
        "price_velocity_10s": price_velocity,
        "time_to_expiry_minutes": time_to_expiry,
        "synonym_price_delta_zscore": synonym_delta,
        "volatility_5m": volatility_5m,
        "days_to_expiry": days_to_expiry,
    }
    if any(value is None for value in features.values()):
        return None
    return features


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _mid(best_bid: float, best_ask: float, fallback: float) -> float:
    if best_bid and best_ask:
        return (best_bid + best_ask) / 2
    if best_bid:
        return best_bid
    if best_ask:
        return best_ask
    return fallback


def _spread_zscore(recent_ticks: Iterable[dict[str, Any]]) -> float:
    spreads: list[float] = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=5)
    for tick in recent_ticks:
        ts = tick.get("ts")
        if not isinstance(ts, datetime) or ts < cutoff:
            continue
        bid = _to_float(tick.get("best_bid"))
        ask = _to_float(tick.get("best_ask"))
        if bid and ask:
            spreads.append(max(ask - bid, 0))
    if len(spreads) < 2:
        return 0.0
    latest = spreads[0]
    s_mean = mean(spreads)
    s_std = stdev(spreads) or 1.0
    return (latest - s_mean) / s_std


def _price_velocity(recent_ticks: List[dict[str, Any]], window_secs: int) -> float:
    if not recent_ticks:
        return 0.0
    now = datetime.now(timezone.utc)
    latest_price = _to_float(recent_ticks[0].get("price"))
    past_price = latest_price
    for tick in recent_ticks:
        ts = tick.get("ts")
        if not isinstance(ts, datetime):
            continue
        if (now - ts).total_seconds() >= window_secs:
            past_price = _to_float(tick.get("price"))
            break
    return latest_price - past_price


def _time_to_expiry_minutes(market: dict[str, Any]) -> float:
    ends_at = market.get("ends_at")
    if not isinstance(ends_at, datetime):
        return 0.0
    delta = (ends_at - datetime.now(timezone.utc)).total_seconds() / 60
    return max(delta, 0.0)


def _days_to_expiry(market: dict[str, Any]) -> float:
    ends_at = market.get("ends_at")
    if not isinstance(ends_at, datetime):
        return 0.0
    delta = (ends_at - datetime.now(timezone.utc)).total_seconds() / 86400
    return max(delta, 0.0)


def _synonym_price_delta(mid_price: float, peers: List[dict[str, Any]] | None) -> float:
    if not peers:
        return 0.0
    peer_prices = [_to_float(peer.get("price")) for peer in peers if peer.get("price") is not None]
    if not peer_prices:
        return 0.0
    avg_peer = sum(peer_prices) / len(peer_prices)
    diffs = [price - avg_peer for price in peer_prices]
    if len(diffs) >= 2:
        std = stdev(diffs) or 1.0
    else:
        std = 1.0
    delta = mid_price - avg_peer
    return delta / std


def _price_volatility(recent_ticks: List[dict[str, Any]]) -> float:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=5)
    prices: list[float] = []
    for tick in recent_ticks:
        ts = tick.get("ts")
        if not isinstance(ts, datetime) or ts < cutoff:
            continue
        prices.append(_to_float(tick.get("price")))
    if len(prices) < 2:
        return 0.0
    return stdev(prices)
