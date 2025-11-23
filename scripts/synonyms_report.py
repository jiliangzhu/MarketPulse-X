"""
每周同义词组巡检工具，生成 Markdown 报告帮助维护 configs/synonyms.yml。
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

try:  # pragma: no cover - 若未安装 numpy 将禁用部分统计
    import numpy as np
except Exception:  # pragma: no cover
    np = None  # type: ignore

try:  # pragma: no cover - 若缺 sentence_transformers 则禁用社区检测
    from sentence_transformers import util
except Exception:  # pragma: no cover
    util = None  # type: ignore

from backend.db import Database
from backend.settings import get_settings
from backend.utils.logging import configure_logging, get_logger

# ===== 报告使用到的数据结构 =====


@dataclass
class MarketRow:
    market_id: str
    condition_id: str | None
    title: str
    category: str | None
    ends_at: datetime | None
    closed: bool
    embedding: list[float] | None
    tags: list[str]
    recent_volume: float


@dataclass
class NeighborSuggestion:
    market_id: str
    similarity: float
    title: str
    category: str | None
    explicit_group: str | None


@dataclass
class GroupInsight:
    name: str
    member_ids: list[str]
    members: list[MarketRow]
    missing_ids: list[str]
    avg_similarity: float | None
    min_similarity: float | None
    lowest_pair: tuple[str, str, float] | None
    category_counts: dict[str, int]
    neighbor_suggestions: list[NeighborSuggestion]
    historical: bool


@dataclass
class AutoCluster:
    cluster_id: str
    members: list[str]
    avg_similarity: float | None
    min_similarity: float | None


@dataclass
class CandidateMerge:
    cluster: AutoCluster
    involved_groups: list[str]
    members: list[tuple[MarketRow, str | None]]


@dataclass
class SplitInsight:
    group_name: str
    reasons: list[str]
    avg_similarity: float | None
    min_similarity: float | None
    subclusters: list[list[MarketRow]]


@dataclass
class UngroupedInsight:
    market: MarketRow
    auto_cluster_id: str | None
    auto_cluster_size: int
    neighbors: list[NeighborSuggestion]


@dataclass
class ReportArtifacts:
    analysis_days: int
    window_start: datetime
    window_end: datetime
    markets: list[MarketRow]
    explicit_insights: list[GroupInsight]
    auto_clusters: list[AutoCluster]
    candidate_merges: list[CandidateMerge]
    candidate_splits: list[SplitInsight]
    historical_groups: list[GroupInsight]
    important_markets: list[UngroupedInsight]
    explicit_total: int
    libs_status: dict[str, bool]
    min_volume: float


# ===== 配置与数据库访问 =====


def load_synonym_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if "groups" not in data:
        raise ValueError("synonyms.yml 格式错误: 缺少 groups 字段")
    data.setdefault("threshold", 0.75)
    data.setdefault("min_size", 2)
    return data


async def fetch_recent_markets(db: Database, days_back: int) -> list[MarketRow]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    rows = await db.fetch(
        """
        WITH recent_stats AS (
            SELECT market_id,
                   SUM(COALESCE(volume, 0)) AS recent_volume,
                   MAX(ts) AS last_tick_ts
            FROM tick
            WHERE ts >= $1
            GROUP BY market_id
        )
        SELECT m.market_id,
               NULL::text AS condition_id,
               m.title,
               NULL::text AS category,
               m.ends_at,
               (m.status = 'closed') AS closed,
               m.embedding,
               m.tags,
               COALESCE(rs.recent_volume, 0) AS recent_volume,
               COALESCE(rs.last_tick_ts, m.ends_at, now()) AS freshness
        FROM market m
        LEFT JOIN recent_stats rs ON rs.market_id = m.market_id
        WHERE m.embedding IS NOT NULL
          AND (
                rs.last_tick_ts >= $1
                OR COALESCE(m.ends_at, now()) >= $2
              )
        """,
        cutoff,
        cutoff,
    )
    def _normalize_embedding(value: Any) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            return [float(v) for v in value]
        if isinstance(value, str):
            cleaned = value.strip("[]{}()")
            if not cleaned:
                return None
            return [float(part) for part in cleaned.split(",") if part.strip()]
        return None

    return [
        MarketRow(
            market_id=row["market_id"],
            condition_id=row.get("condition_id"),
            title=row.get("title", ""),
            category=row.get("category"),
            ends_at=row.get("ends_at"),
            closed=row.get("closed", False),
            embedding=_normalize_embedding(row.get("embedding")),
            tags=row.get("tags") or [],
            recent_volume=float(row.get("recent_volume") or 0),
        )
        for row in rows
        if row.get("embedding") is not None
    ]


# ===== 相似度与聚类工具 =====


def build_vector_map(rows: Iterable[MarketRow]) -> dict[str, "np.ndarray"]:
    if np is None:
        return {}
    vector_map: dict[str, "np.ndarray"] = {}
    for row in rows:
        if row.embedding is None:
            continue
        vector_map[row.market_id] = np.asarray(row.embedding, dtype=np.float32)
    return vector_map


def cosine_sim_matrix(vectors: "np.ndarray") -> "np.ndarray":
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return (vectors @ vectors.T) / (norms @ norms.T)


def compute_similarity_stats(member_ids: list[str], vector_map: dict[str, "np.ndarray"]) -> tuple[float | None, float | None, tuple[str, str, float] | None]:
    if np is None or not member_ids:
        return None, None, None
    vectors = []
    kept_ids = []
    for mid in member_ids:
        vec = vector_map.get(mid)
        if vec is None:
            continue
        vectors.append(vec)
        kept_ids.append(mid)
    if len(vectors) < 2:
        return None, None, None
    matrix = cosine_sim_matrix(np.vstack(vectors))
    triu = np.triu_indices(len(kept_ids), k=1)
    sims = matrix[triu]
    avg_sim = float(sims.mean()) if sims.size else None
    min_sim = float(sims.min()) if sims.size else None
    lowest_pair = None
    if sims.size:
        idx = sims.argmin()
        lowest_pair = (kept_ids[triu[0][idx]], kept_ids[triu[1][idx]], float(sims[idx]))
    return avg_sim, min_sim, lowest_pair


def build_neighbor_map(vector_map: dict[str, "np.ndarray"], top_k: int = 5) -> dict[str, list[tuple[str, float]]]:
    if np is None or not vector_map:
        return {}
    ids = list(vector_map.keys())
    vectors = np.vstack([vector_map[mid] for mid in ids])
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    normalized = vectors / norms
    sims = normalized @ normalized.T
    neighbor_map: dict[str, list[tuple[str, float]]] = {}
    for idx, mid in enumerate(ids):
        row = sims[idx]
        order = np.argsort(-row)
        picks: list[tuple[str, float]] = []
        for j in order:
            if j == idx:
                continue
            picks.append((ids[j], float(row[j])))
            if len(picks) >= top_k:
                break
        neighbor_map[mid] = picks
    return neighbor_map


def build_auto_clusters(rows: Iterable[MarketRow], threshold: float, min_size: int, vector_map: dict[str, "np.ndarray"]) -> list[AutoCluster]:
    if np is None or util is None:
        return []
    vectors = []
    ids = []
    for row in rows:
        vec = vector_map.get(row.market_id)
        if vec is None:
            continue
        vectors.append(vec)
        ids.append(row.market_id)
    if not vectors:
        return []
    communities = util.community_detection(vectors, threshold=threshold, min_community_size=min_size)
    clusters: list[AutoCluster] = []
    for idx, community in enumerate(communities, start=1):
        members = sorted({ids[i] for i in community})
        if len(members) < 2:
            continue
        avg_sim, min_sim, _ = compute_similarity_stats(members, vector_map)
        clusters.append(AutoCluster(cluster_id=f"Auto Cluster {idx}", members=members, avg_similarity=avg_sim, min_similarity=min_sim))
    return clusters


def suggest_subclusters(member_ids: list[str], vector_map: dict[str, "np.ndarray"], high_threshold: float) -> list[list[str]]:
    if np is None or util is None or len(member_ids) < 3:
        return []
    vectors = []
    ids = []
    for mid in member_ids:
        vec = vector_map.get(mid)
        if vec is None:
            continue
        vectors.append(vec)
        ids.append(mid)
    if len(vectors) < 3:
        return []
    communities = util.community_detection(vectors, threshold=high_threshold, min_community_size=2)
    clusters: list[list[str]] = []
    for community in communities:
        members = [ids[i] for i in community]
        if len(members) >= 2:
            clusters.append(members)
    return clusters


# ===== 分析逻辑 =====


def build_explicit_map(groups: Iterable[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in groups:
        name = entry.get("name", "Unnamed Group")
        for mid in entry.get("explicit", []) or []:
            mapping[mid] = name
    return mapping


def compute_group_insights(
    config_groups: list[dict[str, Any]],
    market_dict: dict[str, MarketRow],
    vector_map: dict[str, "np.ndarray"],
    neighbor_map: dict[str, list[tuple[str, float]]],
    explicit_map: dict[str, str],
    cutoff: datetime,
    neighbor_threshold: float = 0.82,
) -> list[GroupInsight]:
    insights: list[GroupInsight] = []
    for entry in config_groups:
        name = entry.get("name", "Unnamed Group")
        member_ids = [mid for mid in entry.get("explicit", []) or []]
        members = [market_dict[mid] for mid in member_ids if mid in market_dict]
        missing = [mid for mid in member_ids if mid not in market_dict]
        avg_sim, min_sim, lowest_pair = compute_similarity_stats(member_ids, vector_map)
        category_counts = Counter((m.category or "未知") for m in members)
        historical = bool(members) and all((m.closed and m.ends_at and m.ends_at < cutoff) for m in members)
        suggestions: list[NeighborSuggestion] = []
        seen_neighbors: set[str] = set()
        for m in members:
            for neighbor_id, score in neighbor_map.get(m.market_id, []):
                if neighbor_id in member_ids or neighbor_id in seen_neighbors:
                    continue
                if score < neighbor_threshold:
                    continue
                neighbor = market_dict.get(neighbor_id)
                if not neighbor:
                    continue
                suggestions.append(
                    NeighborSuggestion(
                        market_id=neighbor_id,
                        similarity=score,
                        title=neighbor.title,
                        category=neighbor.category,
                        explicit_group=explicit_map.get(neighbor_id),
                    )
                )
                seen_neighbors.add(neighbor_id)
                if len(suggestions) >= 5:
                    break
            if len(suggestions) >= 5:
                break
        insights.append(
            GroupInsight(
                name=name,
                member_ids=member_ids,
                members=members,
                missing_ids=missing,
                avg_similarity=avg_sim,
                min_similarity=min_sim,
                lowest_pair=lowest_pair,
                category_counts=dict(category_counts),
                neighbor_suggestions=suggestions,
                historical=historical,
            )
        )
    return insights


def detect_candidate_merges(
    auto_clusters: list[AutoCluster],
    explicit_map: dict[str, str],
    market_dict: dict[str, MarketRow],
) -> list[CandidateMerge]:
    merges: list[CandidateMerge] = []
    for cluster in auto_clusters:
        group_hits: dict[str, list[str]] = defaultdict(list)
        for member in cluster.members:
            group_name = explicit_map.get(member)
            if group_name:
                group_hits[group_name].append(member)
        if len(group_hits) < 2:
            continue
        cluster_members: list[tuple[MarketRow, str | None]] = []
        for mid in cluster.members:
            market = market_dict.get(mid)
            if not market:
                continue
            cluster_members.append((market, explicit_map.get(mid)))
        merges.append(CandidateMerge(cluster=cluster, involved_groups=sorted(group_hits.keys()), members=cluster_members))
    return merges


def detect_candidate_splits(
    insights: list[GroupInsight],
    vector_map: dict[str, "np.ndarray"],
    base_threshold: float,
) -> list[SplitInsight]:
    splits: list[SplitInsight] = []
    for group in insights:
        reasons: list[str] = []
        if group.min_similarity is not None and group.min_similarity < 0.6:
            reasons.append(f"组内最小相似度仅 {group.min_similarity:.2f}")
        if len(group.category_counts) > 1:
            reasons.append("成员类别差异较大")
        if not reasons:
            continue
        high_threshold = min(0.95, max(base_threshold + 0.05, 0.8))
        subclusters_ids = suggest_subclusters(group.member_ids, vector_map, high_threshold)
        subclusters: list[list[MarketRow]] = []
        for ids in subclusters_ids:
            members = [m for m in group.members if m.market_id in ids]
            if members:
                subclusters.append(members)
        splits.append(
            SplitInsight(
                group_name=group.name,
                reasons=reasons,
                avg_similarity=group.avg_similarity,
                min_similarity=group.min_similarity,
                subclusters=subclusters,
            )
        )
    return splits


def find_ungrouped_markets(
    rows: list[MarketRow],
    explicit_map: dict[str, str],
    auto_cluster_map: dict[str, str],
    auto_cluster_sizes: dict[str, int],
    neighbor_map: dict[str, list[tuple[str, float]]],
    min_volume: float,
    market_dict: dict[str, MarketRow],
) -> list[UngroupedInsight]:
    candidates: list[UngroupedInsight] = []
    for row in rows:
        if explicit_map.get(row.market_id):
            continue
        cluster_id = auto_cluster_map.get(row.market_id)
        cluster_size = auto_cluster_sizes.get(cluster_id or "", 0) if cluster_id else 0
        if cluster_size > 1:
            continue
        if min_volume > 0 and row.recent_volume < min_volume:
            continue
        neighbor_items: list[NeighborSuggestion] = []
        for neighbor_id, score in neighbor_map.get(row.market_id, [])[:3]:
            neighbor = market_dict.get(neighbor_id)
            if not neighbor:
                continue
            neighbor_items.append(
                NeighborSuggestion(
                    market_id=neighbor_id,
                    similarity=score,
                    title=neighbor.title,
                    category=neighbor.category,
                    explicit_group=explicit_map.get(neighbor_id),
                )
            )
        candidates.append(
            UngroupedInsight(
                market=row,
                auto_cluster_id=cluster_id,
                auto_cluster_size=cluster_size,
                neighbors=neighbor_items,
            )
        )
    candidates.sort(key=lambda item: item.market.recent_volume, reverse=True)
    return candidates[:10]


def analyze_markets_for_report(
    rows: list[MarketRow],
    config: dict,
    days_back: int,
    min_volume: float,
    cutoff_days: int,
) -> ReportArtifacts:
    now = datetime.now(timezone.utc)
    market_dict = {row.market_id: row for row in rows}
    vector_map = build_vector_map(rows)
    neighbor_map = build_neighbor_map(vector_map)
    threshold = float(config.get("threshold", 0.75))
    min_size = int(config.get("min_size", 2))
    auto_clusters = build_auto_clusters(rows, threshold, min_size, vector_map)
    auto_cluster_map: dict[str, str] = {}
    auto_cluster_sizes: dict[str, int] = {}
    for cluster in auto_clusters:
        auto_cluster_sizes[cluster.cluster_id] = len(cluster.members)
        for member in cluster.members:
            auto_cluster_map[member] = cluster.cluster_id
    explicit_groups = config.get("groups", [])
    explicit_map = build_explicit_map(explicit_groups)
    cutoff = now - timedelta(days=cutoff_days)
    explicit_insights = compute_group_insights(
        explicit_groups,
        market_dict,
        vector_map,
        neighbor_map,
        explicit_map,
        cutoff,
    )
    candidate_merges = detect_candidate_merges(auto_clusters, explicit_map, market_dict)
    candidate_splits = detect_candidate_splits(explicit_insights, vector_map, threshold)
    historical_groups = [ins for ins in explicit_insights if ins.historical]
    important_markets = find_ungrouped_markets(
        rows,
        explicit_map,
        auto_cluster_map,
        auto_cluster_sizes,
        neighbor_map,
        min_volume,
        market_dict,
    )
    return ReportArtifacts(
        analysis_days=days_back,
        window_start=now - timedelta(days=days_back),
        window_end=now,
        markets=rows,
        explicit_insights=explicit_insights,
        auto_clusters=auto_clusters,
        candidate_merges=candidate_merges,
        candidate_splits=candidate_splits,
        historical_groups=historical_groups,
        important_markets=important_markets,
        explicit_total=len(explicit_groups),
        libs_status={"numpy": np is not None, "sentence_transformers": util is not None},
        min_volume=min_volume,
    )


# ===== Markdown 构建 =====


def format_market_row(row: MarketRow) -> str:
    end_str = row.ends_at.strftime("%Y-%m-%d") if row.ends_at else "N/A"
    return f"| `{row.market_id}` | `{row.condition_id or 'n/a'}` | {row.title} | {row.category or '未知'} | {end_str} | {row.closed} | {row.recent_volume:.0f} |"


def build_section_overview(ctx: ReportArtifacts) -> list[str]:
    span = f"{ctx.window_start.date()} ~ {ctx.window_end.date()}"
    lines = [
        "## 【Section 1】总体概览",
        "",
        f"- 分析时间：{span}（最近 {ctx.analysis_days} 天）",
        f"- 纳入市场数量：{len(ctx.markets)}",
        f"- 自动聚类数量：{len(ctx.auto_clusters)}",
        f"- 显式分组数量：{ctx.explicit_total}",
        f"- 候选合并数量：{len(ctx.candidate_merges)}",
        f"- 候选拆分数量：{len(ctx.candidate_splits)}",
        f"- 纯历史显式组数量：{len(ctx.historical_groups)}",
        f"- 重要未分组市场：{len(ctx.important_markets)}（阈值：近{ctx.analysis_days}天成交量 ≥ {ctx.min_volume:.0f}）",
    ]
    if not ctx.libs_status.get("numpy"):
        lines.append("- ⚠️ 当前环境缺少 numpy，部分相似度统计不可用")
    if not ctx.libs_status.get("sentence_transformers"):
        lines.append("- ⚠️ 当前环境缺少 sentence-transformers，未生成自动聚类")
    lines.append("")
    return lines


def build_section_explicit(ctx: ReportArtifacts) -> list[str]:
    lines = ["## 【Section 2】显式分组健康检查", ""]
    if not ctx.explicit_insights:
        lines.append("- 当前配置中没有显式分组。")
        lines.append("")
        return lines
    for insight in ctx.explicit_insights:
        lines.append(f"### {insight.name}")
        lines.append(f"- 成员数量：{len(insight.members)}（缺失 {len(insight.missing_ids)}）")
        if insight.avg_similarity is not None:
            lines.append(f"- 组内平均相似度：{insight.avg_similarity:.3f}")
        if insight.min_similarity is not None:
            lines.append(f"- 组内最小相似度：{insight.min_similarity:.3f}")
        if insight.lowest_pair:
            a, b, score = insight.lowest_pair
            lines.append(f"- 离群对：`{a}` vs `{b}` 相似度 {score:.3f}")
        if insight.category_counts:
            category_str = ", ".join(f"{k}×{v}" for k, v in insight.category_counts.items())
            lines.append(f"- 类别分布：{category_str}")
        if insight.historical:
            lines.append("- ⚠️ 全员已关闭且远离分析窗口，可考虑迁移到历史配置。")
        if insight.missing_ids:
            missing = ", ".join(f"`{mid}`" for mid in insight.missing_ids)
            lines.append(f"- 缺失成员：{missing}")
        if insight.members:
            lines.append("")
            lines.append("| Market ID | Condition | 标题 | 类别 | 截止 | Closed | 近期开单量 |")
            lines.append("| --- | --- | --- | --- | --- | --- | --- |")
            for member in insight.members:
                lines.append(format_market_row(member))
        else:
            lines.append("- 当前窗口内未找到任何成员的行情记录。")
        if insight.neighbor_suggestions:
            lines.append("")
            lines.append("- 组外候选：")
            for suggestion in insight.neighbor_suggestions:
                extra = f"，来自显式组 {suggestion.explicit_group}" if suggestion.explicit_group else ""
                lines.append(
                    f"  - `{suggestion.market_id}` · {suggestion.title} · {suggestion.category or '未知'} · 相似度 {suggestion.similarity:.2f}{extra}"
                )
        lines.append("")
    return lines


def build_section_merges(ctx: ReportArtifacts) -> list[str]:
    lines = ["## 【Section 3】候选合并", ""]
    if not ctx.candidate_merges:
        lines.append("- 暂无需要关注的候选合并。")
        lines.append("")
        return lines
    for idx, merge in enumerate(ctx.candidate_merges, start=1):
        cluster = merge.cluster
        lines.append(f"### 候选合并 #{idx} · {cluster.cluster_id}")
        lines.append(f"- 涉及显式组：{', '.join(merge.involved_groups)}")
        if cluster.avg_similarity is not None:
            lines.append(f"- 聚类平均相似度：{cluster.avg_similarity:.3f}")
        if cluster.min_similarity is not None:
            lines.append(f"- 聚类最小相似度：{cluster.min_similarity:.3f}")
        lines.append("- 成员清单：")
        for market, group_name in merge.members:
            lines.append(
                f"  - `{market.market_id}` · {market.title} · {market.category or '未知'} · 来自组：{group_name or '无'}"
            )
        lines.append("")
    return lines


def build_section_splits(ctx: ReportArtifacts) -> list[str]:
    lines = ["## 【Section 4】候选拆分", ""]
    if not ctx.candidate_splits:
        lines.append("- 暂无需要拆分的显式组。")
        lines.append("")
        return lines
    for split in ctx.candidate_splits:
        lines.append(f"### {split.group_name}")
        if split.avg_similarity is not None:
            lines.append(f"- 平均相似度：{split.avg_similarity:.3f}")
        if split.min_similarity is not None:
            lines.append(f"- 最小相似度：{split.min_similarity:.3f}")
        lines.append(f"- 拆分理由：{'；'.join(split.reasons)}")
        if split.subclusters:
            lines.append("- 建议子簇：")
            for idx, cluster_members in enumerate(split.subclusters, start=1):
                ids = ", ".join(f"`{m.market_id}`" for m in cluster_members)
                example = cluster_members[0].title if cluster_members else ""
                lines.append(f"  - 子簇 {idx}（示例：{example}）：{ids}")
        else:
            lines.append("- 暂无法提供子簇建议（向量或依赖缺失）。")
        lines.append("")
    return lines


def build_section_historical(ctx: ReportArtifacts) -> list[str]:
    lines = ["## 【Section 5】纯历史显式分组", ""]
    if not ctx.historical_groups:
        lines.append("- 暂无完全过期的显式组。")
        lines.append("")
        return lines
    for group in ctx.historical_groups:
        lines.append(f"- {group.name}（成员 {len(group.members)}）")
    lines.append("")
    return lines


def build_section_ungrouped(ctx: ReportArtifacts) -> list[str]:
    lines = ["## 【Section 6】未分组但重要的市场", ""]
    if not ctx.important_markets:
        lines.append("- 未发现满足阈值的市场。")
        lines.append("")
        return lines
    for item in ctx.important_markets:
        market = item.market
        lines.append(
            f"### `{market.market_id}` · {market.title}（{market.category or '未知'}） · 近期开单量 {market.recent_volume:.0f}"
        )
        if item.auto_cluster_id:
            lines.append(f"- 所属自动簇：{item.auto_cluster_id}（规模 {item.auto_cluster_size}）")
        else:
            lines.append("- 所属自动簇：暂无（孤立点）")
        if item.neighbors:
            lines.append("- 最近邻建议：")
            for neighbor in item.neighbors:
                extra = f"，显式组 {neighbor.explicit_group}" if neighbor.explicit_group else ""
                lines.append(
                    f"  - `{neighbor.market_id}` · {neighbor.title} · {neighbor.category or '未知'} · 相似度 {neighbor.similarity:.2f}{extra}"
                )
        else:
            lines.append("- 最近邻建议：依赖缺失或暂不可用。")
        lines.append("")
    return lines


def build_report(ctx: ReportArtifacts) -> str:
    body: list[str] = ["# Synonyms Weekly Report", ""]
    body.extend(build_section_overview(ctx))
    body.extend(build_section_explicit(ctx))
    body.extend(build_section_merges(ctx))
    body.extend(build_section_splits(ctx))
    body.extend(build_section_historical(ctx))
    body.extend(build_section_ungrouped(ctx))
    return "\n".join(body)


# ===== CLI 入口 =====


async def generate_synonyms_report(
    days_back: int,
    min_volume: float,
    output_path: Path,
    cutoff_days: int = 90,
) -> None:
    configure_logging()
    logger = get_logger("synonyms-report")
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    try:
        config_path = settings.config_synonyms_path
        config = load_synonym_config(config_path)
        rows = await fetch_recent_markets(db, days_back)
        artifacts = analyze_markets_for_report(rows, config, days_back, min_volume, cutoff_days)
        report = build_report(artifacts)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        logger.info("synonyms-report-generated", extra={"path": str(output_path)})
    finally:
        await db.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synonyms Weekly Report")
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--min-volume", type=float, default=0.0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cutoff-days", type=int, default=90)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(generate_synonyms_report(args.days_back, args.min_volume, args.output, args.cutoff_days))
