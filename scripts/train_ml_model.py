from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, precision_recall_curve, roc_auc_score
from sklearn.model_selection import train_test_split

from backend.db import Database
from backend.settings import get_settings
from backend.utils.logging import configure_logging, get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train ML model for MarketPulse-X")
    parser.add_argument("--start", required=False, help="Start ISO timestamp")
    parser.add_argument("--end", required=False, help="End ISO timestamp")
    parser.add_argument("--days-back", type=int, default=7, help="回溯天数，未提供 start 时生效")
    parser.add_argument("--max-ticks", type=int, default=0, help="可选的 tick 限制，0 表示不限")
    parser.add_argument("--output", type=Path, default=None, help="Override model output path")
    parser.add_argument(
        "--threshold-grid",
        type=str,
        default="0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.50",
        help="逗号分隔的阈值列表，用于 PR 掃描",
    )
    parser.add_argument(
        "--metrics-path",
        type=Path,
        default=Path("reports/train_metrics.csv"),
        help="保存阈值扫描结果的路径",
    )
    return parser.parse_args()


async def fetch_ticks(db: Database, start: datetime, end: datetime, max_rows: int = 0) -> pd.DataFrame:
    limit_clause = "LIMIT $3" if max_rows and max_rows > 0 else ""
    params: list[Any] = [start, end]
    if limit_clause:
        params.append(max_rows)
    rows = await db.fetch(
        f"""
        SELECT t.ts,
               t.market_id,
               t.option_id,
               t.price,
               t.volume,
               t.best_bid,
               t.best_ask,
               t.liquidity,
               m.ends_at
        FROM tick t
        JOIN market m ON m.market_id = t.market_id
        WHERE ts BETWEEN $1 AND $2
        ORDER BY ts
        {limit_clause}
        """,
        *params,
    )
    return pd.DataFrame([dict(row) for row in rows])


async def fetch_signals(db: Database, start: datetime, end: datetime) -> pd.DataFrame:
    rows = await db.fetch(
        """
        SELECT signal_id, market_id, created_at
        FROM signal
        WHERE created_at BETWEEN $1 AND $2
          AND source = 'rule'
        """,
        start,
        end,
    )
    return pd.DataFrame([dict(row) for row in rows])


def build_features(ticks: pd.DataFrame) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame()
    ticks = ticks.copy()
    for col in ["price", "volume", "best_bid", "best_ask", "liquidity"]:
        if col in ticks.columns:
            ticks[col] = pd.to_numeric(ticks[col], errors="coerce")
    ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True)
    ticks["ends_at"] = pd.to_datetime(ticks["ends_at"], utc=True, errors="coerce")
    ticks.sort_values("ts", inplace=True)
    ticks["mid_price"] = ticks[["best_bid", "best_ask"]].mean(axis=1, skipna=True).fillna(ticks["price"])
    ticks["spread"] = (ticks["best_ask"] - ticks["best_bid"]).abs().fillna(0)
    ticks["volume"] = ticks["volume"].fillna(0)
    ticks["best_bid_size"] = ticks["volume"]
    ticks["best_ask_size"] = ticks["volume"]
    ticks["size_imbalance"] = 0.0

    def _spread_zscore(group: pd.Series) -> pd.Series:
        mean = group.rolling(50, min_periods=1).mean()
        std = group.rolling(50, min_periods=1).std().replace(0, 1).fillna(1)
        return (group - mean) / std

    ticks["zscore_spread_5min"] = (
        ticks.groupby("market_id")["spread"].transform(_spread_zscore)
    ).fillna(0)
    ticks["price_velocity_10s"] = (
        ticks.groupby("market_id")["mid_price"].transform(lambda s: s.diff().fillna(0))
    )
    def _vol_rolling(group: pd.DataFrame) -> pd.Series:
        group = group.sort_values("ts")
        series = (
            group.set_index("ts")["mid_price"]
            .rolling("5min")
            .std()
            .reset_index(drop=True)
        )
        series.index = group.index
        return series

    ticks["volatility_5m"] = (
        ticks.groupby("market_id", group_keys=False)
        .apply(_vol_rolling)
        .fillna(0)
    )
    ticks["time_to_expiry_minutes"] = (
        (ticks["ends_at"] - ticks["ts"]).dt.total_seconds().div(60).clip(lower=0).fillna(0)
    )
    ticks["days_to_expiry"] = (
        (ticks["ends_at"] - ticks["ts"]).dt.total_seconds().div(86400).clip(lower=0).fillna(0)
    )
    ticks["synonym_price_delta_zscore"] = 0.0
    ticks["feature_ts"] = ticks["ts"]
    return ticks[
        [
            "feature_ts",
            "market_id",
            "mid_price",
            "spread",
            "volume",
             "best_bid_size",
             "best_ask_size",
            "size_imbalance",
            "zscore_spread_5min",
            "price_velocity_10s",
            "time_to_expiry_minutes",
            "synonym_price_delta_zscore",
            "volatility_5m",
            "days_to_expiry",
        ]
    ]


def align_labels(features: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features
    features = features.copy()
    features["label"] = 0
    if not signals.empty:
        signals = signals.copy()
        signals["created_at"] = pd.to_datetime(signals["created_at"], utc=True)
        signals = signals.set_index("created_at").sort_index()
        for idx, row in features.iterrows():
            window_start = row["feature_ts"] - pd.Timedelta(seconds=5)
            window_end = row["feature_ts"] + pd.Timedelta(seconds=5)
            hits = signals.loc[window_start:window_end]
            if not hits.empty and row["market_id"] in hits["market_id"].values:
                features.at[idx, "label"] = 1
    return features.drop(columns=["feature_ts"])


def _scan_thresholds(y_true: pd.Series, y_proba: np.ndarray, grid: list[float]) -> pd.DataFrame:
    """阈值扫描，返回精确率、召回、F1 及 AUC 供参考。"""
    rows = []
    auc = roc_auc_score(y_true, y_proba) if len(np.unique(y_true)) > 1 else float("nan")
    for thr in grid:
        pred = (y_proba >= thr).astype(int)
        tp = ((pred == 1) & (y_true == 1)).sum()
        fp = ((pred == 1) & (y_true == 0)).sum()
        fn = ((pred == 0) & (y_true == 1)).sum()
        precision = tp / (tp + fp + 1e-9)
        recall = tp / (tp + fn + 1e-9)
        f1 = 2 * precision * recall / (precision + recall + 1e-9)
        rows.append({"threshold": thr, "precision": precision, "recall": recall, "f1": f1, "auc": auc})
    return pd.DataFrame(rows)


def train_model(features: pd.DataFrame, model_path: Path, threshold_grid: list[float], metrics_path: Path) -> None:
    logger = get_logger("ml-train")
    if features.empty:
        logger.warning("No training data available")
        return
    y = features.pop("label")
    X_train, X_test, y_train, y_test = train_test_split(
        features.fillna(0),
        y,
        test_size=0.2,
        random_state=42,
        stratify=y if y.sum() else None,
    )
    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=31,
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]
    report = classification_report(y_test, preds, zero_division=0)
    auc = roc_auc_score(y_test, proba) if len(np.unique(y_test)) > 1 else float("nan")
    scan_df = _scan_thresholds(pd.Series(y_test), proba, threshold_grid)
    best_row = scan_df.sort_values("f1", ascending=False).iloc[0]
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    scan_df.to_csv(metrics_path, index=False)
    logger.info(
        "ml-training-report",
        extra={
            "report": report,
            "auc": auc,
            "best_threshold": best_row["threshold"],
            "best_f1": best_row["f1"],
            "best_recall": best_row["recall"],
        },
    )
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    logger.info("ml-model-saved", extra={"path": str(model_path)})


async def main() -> None:
    configure_logging()
    logger = get_logger("ml-train")
    args = parse_args()
    settings = get_settings()
    db = Database(settings.database_dsn)
    await db.connect()
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00")) if args.end else datetime.now(timezone.utc)
    default_start = end - timedelta(days=args.days_back)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")) if args.start else default_start
    ticks = await fetch_ticks(db, start, end, max_rows=args.max_ticks)
    signals = await fetch_signals(db, start, end)
    await db.disconnect()
    features = build_features(ticks)
    dataset = align_labels(features, signals)
    if "market_id" in dataset.columns:
        dataset = dataset.drop(columns=["market_id"])
    output_path = args.output or settings.ml_model_path
    grid = [float(x) for x in args.threshold_grid.split(",") if x]
    train_model(dataset, output_path, grid, args.metrics_path)
    logger.info("ml-training-done")


if __name__ == "__main__":
    asyncio.run(main())
