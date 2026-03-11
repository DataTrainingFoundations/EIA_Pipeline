from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    eia_api_key: str
    app_timezone: str
    api_cache_ttl_seconds: int
    query_timeout_seconds: int
    default_days: int
    airflow_log_dir: Path
    registry_path: Path

    @property
    def minio_endpoint_with_scheme(self) -> str:
        if self.minio_endpoint.startswith("http://") or self.minio_endpoint.startswith("https://"):
            return self.minio_endpoint
        return f"http://{self.minio_endpoint}"

    def default_window(self) -> tuple[datetime, datetime]:
        end_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start_utc = end_utc - timedelta(days=self.default_days)
        return start_utc, end_utc


def load_config() -> AppConfig:
    return AppConfig(
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "platform"),
        postgres_user=os.getenv("POSTGRES_USER", "platform"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "platform"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        eia_api_key=os.getenv("EIA_API_KEY", ""),
        app_timezone=os.getenv("APP_TIMEZONE", "UTC"),
        api_cache_ttl_seconds=int(os.getenv("ADMIN_APP_API_CACHE_TTL_SECONDS", "300")),
        query_timeout_seconds=int(os.getenv("ADMIN_APP_QUERY_TIMEOUT_SECONDS", "60")),
        default_days=int(os.getenv("ADMIN_APP_DEFAULT_DAYS", "7")),
        airflow_log_dir=Path(os.getenv("ADMIN_APP_LOG_DIR", "/opt/airflow/logs")),
        registry_path=Path(os.getenv("ADMIN_APP_DATASET_REGISTRY", "/workspace/ingestion/src/dataset_registry.yml")),
    )
