from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "mpx"
    postgres_user: str = "mpx"
    postgres_password: str = "mpx_pass"

    api_port: int = 8080
    data_source: Literal["mock", "real"] = "mock"
    service_role: Literal["api", "ingestor", "worker", "all"] = "api"

    telegram_enabled: bool = False
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    admin_api_token: Optional[str] = None

    exec_mode: Literal["manual", "semi_auto", "auto"] = "semi_auto"
    exec_max_notional_per_order: float = 200.0
    exec_max_concurrent_orders: int = 2
    exec_max_daily_notional: float = 1000.0
    exec_slippage_bps: int = 80

    # ML Model Inference Settings
    ml_enabled: bool = False
    ml_model_path: Path = Path("models/gboost_latest.pkl")
    ml_confidence_threshold: float = 0.20
    ml_inference_interval_secs: float = 5.0
    ml_fusion_confidence_weight: float = 1.0
    ml_fusion_rule_bonus: float = 20.0

    # Market ingestion controls
    market_bootstrap_limit: int = 200
    market_min_liquidity: float = 0.0
    market_min_volume_24h: float = 0.0

    config_app_path: Path = Path("configs/app.yaml")
    config_rules_path: Path = Path("configs/rules")
    config_synonyms_path: Path = Path("configs/synonyms.yml")

    metrics_enabled: bool = True

    @property
    def database_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
