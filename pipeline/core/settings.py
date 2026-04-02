from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


@dataclass(frozen=True)
class EiaSettings:
    api_key: str
    base_url: str = "https://api.eia.gov/v2"
    default_rolling_hours: float = 2.0
    max_retries: int = 3
    retry_backoff_seconds: int = 2


def load_snowflake_settings(*, schema: str | None = None) -> SnowflakeSettings:
    return SnowflakeSettings(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "EIA_PIPELINE"),
        schema=schema or os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )


def load_app_snowflake_settings() -> SnowflakeSettings:
    return load_snowflake_settings(schema=os.environ.get("SNOWFLAKE_APP_SCHEMA", "GOLD"))


def load_eia_settings() -> EiaSettings:
    return EiaSettings(
        api_key=os.environ["EIA_API_KEY"],
        default_rolling_hours=float(os.environ.get("ROLLING_HOURS", "2")),
    )
