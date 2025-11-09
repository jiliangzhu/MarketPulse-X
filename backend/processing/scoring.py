from __future__ import annotations

from typing import Dict


def compute_score(base: float, weights: dict[str, float], metrics: Dict[str, float]) -> float:
    score = base
    for key, weight in weights.items():
        value = float(metrics.get(key, 0.0))
        score += weight * value
    return round(max(0.0, min(100.0, score)), 2)
