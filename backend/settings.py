from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, field_validator

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres_host: str = Field(..., env="POSTGRES_HOST")
    postgres_port: int = Field(..., env="POSTGRES_PORT")
    postgres_db: str = Field(..., env="POSTGRES_DB")
    postgres_user: str = Field(..., env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")

    api_port: int = 8080
    data_source: Literal["mock", "real"] = "mock"
    service_role: Literal["api", "ingestor", "worker", "all"] = "api"

    telegram_enabled: bool = False
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    admin_api_token: str = Field(..., env="ADMIN_API_TOKEN", min_length=12)

    cors_allow_origins: list[str] = Field(default_factory=list)

    rate_limit_requests_per_minute: int = 120
    rate_limit_window_seconds: int = 60

    rule_payload_max_bytes: int = 16000

    exec_mode: Literal["manual", "semi_auto", "auto"] = "semi_auto"
    exec_max_notional_per_order: float = 200.0
    exec_max_concurrent_orders: int = 2
    exec_max_daily_notional: float = 1000.0
    exec_slippage_bps: int = 80

    # ML Model Inference Settings
    ml_enabled: bool = False
    ml_model_path: Path = Path("models/lgbm_v1.pkl")
    ml_confidence_threshold: float = 0.7
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

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def _parse_origins(cls, value):
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value or []

    @field_validator(
        "postgres_host",
        "postgres_port",
        "postgres_db",
        "postgres_user",
        "postgres_password",
        "admin_api_token",
    )
    @classmethod
    def _not_empty(cls, value):
        if value in (None, ""):
            raise ValueError("must be provided via environment variables")
        return value

    @property
    def database_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
