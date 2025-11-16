from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import scripts.synonyms_report as report


def build_market(mid: str, title: str, embedding: list[float], volume: float = 0.0) -> report.MarketRow:
    """构造测试用的 MarketRow。"""
    return report.MarketRow(
        market_id=mid,
        condition_id=f"cond-{mid}",
        title=title,
        category="Test",
        ends_at=datetime.now(timezone.utc) + timedelta(days=30),
        closed=False,
        embedding=embedding,
        tags=[],
        recent_volume=volume,
    )


def test_build_report_sections(monkeypatch):
    """验证生成的 Markdown 报告包含关键分区。"""
    markets = [
        build_market("m1", "Alpha", [1.0, 0.0, 0.0]),
        build_market("m2", "Beta", [-1.0, 0.0, 0.0]),
        build_market("m3", "Gamma", [0.0, 1.0, 0.0]),
        build_market("m4", "Delta", [0.0, 0.8, 0.2]),
        build_market("m5", "Epsilon", [0.0, 0.0, 1.0], volume=5000),
    ]
    fake_util = SimpleNamespace(
        community_detection=lambda vectors, threshold, min_community_size: [
            list(range(min(len(vectors), 4)))
        ]
        if threshold < 0.8
        else [list(range(len(vectors)))]
    )
    monkeypatch.setattr(report, "util", fake_util, raising=False)
    config = {
        "threshold": 0.75,
        "min_size": 2,
        "groups": [
            {"name": "Group A", "explicit": ["m1", "m2"]},
            {"name": "Group B", "explicit": ["m3", "m4"]},
        ],
    }
    ctx = report.analyze_markets_for_report(markets, config, days_back=30, min_volume=0, cutoff_days=90)
    output = report.build_report(ctx)
    assert "【Section 1】总体概览" in output
    assert "Group A" in output
    assert "候选合并" in output
    assert "候选拆分" in output
    assert "未分组但重要的市场" in output
