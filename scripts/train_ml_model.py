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
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

from backend.db import Database
from backend.settings import get_settings
from backend.utils.logging import configure_logging, get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train ML model for MarketPulse-X")
    parser.add_argument("--start", required=False, help="Start ISO timestamp")
    parser.add_argument("--end", required=False, help="End ISO timestamp")
    parser.add_argument("--output", type=Path, default=None, help="Override model output path")
    return parser.parse_args()


async def fetch_ticks(db: Database, start: datetime, end: datetime) -> pd.DataFrame:
    rows = await db.fetch(
        """
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
        """,
        start,
        end,
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
    ticks["volatility_5m"] = (
        ticks.groupby("market_id")
        .apply(
            lambda g: g.set_index("ts")["mid_price"]
            .rolling("5min")
            .std()
            .reset_index(level=0, drop=True)
        )
        .reset_index(level=0, drop=True)
    )
    ticks["volatility_5m"] = ticks["volatility_5m"].fillna(0)
    ticks["days_to_expiry"] = (
        (ticks["ends_at"] - ticks["ts"]).dt.total_seconds().div(86400).clip(lower=0).fillna(0)
    )
    ticks["feature_ts"] = ticks["ts"]
    return ticks[
        [
            "feature_ts",
            "market_id",
            "mid_price",
            "spread",
            "volume",
            "size_imbalance",
            "zscore_spread_5min",
            "price_velocity_10s",
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
        signals = signals.set_index("created_at")
        for idx, row in features.iterrows():
            window_start = row["feature_ts"] - pd.Timedelta(seconds=5)
            window_end = row["feature_ts"] + pd.Timedelta(seconds=5)
            hits = signals.loc[window_start:window_end]
            if not hits.empty and row["market_id"] in hits["market_id"].values:
                features.at[idx, "label"] = 1
    return features.drop(columns=["feature_ts"])


def train_model(features: pd.DataFrame, model_path: Path) -> None:
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
    report = classification_report(y_test, preds, zero_division=0)
    logger.info("ml-training-report", extra={"report": report})
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
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")) if args.start else end - timedelta(days=7)
    ticks = await fetch_ticks(db, start, end)
    signals = await fetch_signals(db, start, end)
    await db.disconnect()
    features = build_features(ticks)
    dataset = align_labels(features, signals)
    if "market_id" in dataset.columns:
        dataset = dataset.drop(columns=["market_id"])
    output_path = args.output or settings.ml_model_path
    train_model(dataset, output_path)
    logger.info("ml-training-done")


if __name__ == "__main__":
    asyncio.run(main())
