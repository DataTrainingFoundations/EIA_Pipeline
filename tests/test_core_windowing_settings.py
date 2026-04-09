from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.core import settings, windowing


def test_normalize_optional_value_handles_none_and_text() -> None:
    assert windowing._normalize_optional_value(None) == ""
    assert windowing._normalize_optional_value(" none ") == ""
    assert windowing._normalize_optional_value(" 2026-04-01 ") == "2026-04-01"


def test_month_anchor_and_shift_partition_support_monthly_and_hourly() -> None:
    assert windowing.month_anchor_date("2026-04-19") == "2026-04-01"
    assert windowing.shift_partition("2026-04-19", "hourly", -2) == "2026-04-17"
    assert windowing.shift_partition("2026-04-19", "monthly", 2) == "2026-06-01"


def test_enumerate_partitions_for_hourly_and_monthly() -> None:
    assert windowing.enumerate_partitions("2026-04-01", "2026-04-03", frequency="hourly") == [
        "2026-04-01",
        "2026-04-02",
        "2026-04-03",
    ]
    assert windowing.enumerate_partitions("2026-01-15", "2026-03-20", frequency="monthly") == [
        "2026-01-01",
        "2026-02-01",
        "2026-03-01",
    ]


def test_hourly_and_monthly_chunks_respect_bounds() -> None:
    assert windowing._hourly_chunks("2026-04-01", "2026-04-03", 2) == [
        ("2026-04-01T00", "2026-04-02T23"),
        ("2026-04-03T00", "2026-04-03T23"),
    ]
    assert windowing._monthly_chunks("2026-01-01", "2026-03-01", 2) == [
        ("2026-01", "2026-02"),
        ("2026-03", "2026-03"),
    ]


def test_resolve_ingest_windows_uses_explicit_ranges_and_defaults(monkeypatch) -> None:
    fixed_now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(windowing, "datetime", FixedDateTime)

    hourly_dataset = {"frequency": "hourly", "chunk_days": 2}
    monthly_dataset = {"frequency": "monthly", "chunk_months": 2, "rolling_months": 3}

    assert windowing.resolve_ingest_windows(hourly_dataset, start_date="2026-04-01", end_date="2026-04-03") == [
        ("2026-04-01T00", "2026-04-02T23"),
        ("2026-04-03T00", "2026-04-03T23"),
    ]
    assert windowing.resolve_ingest_windows(monthly_dataset) == [("2026-02", "2026-04")]
    assert windowing.resolve_ingest_windows({"frequency": "hourly"}, rolling_hours="6") == [
        ("2026-04-09T06", "2026-04-09T12")
    ]


def test_processing_and_business_dates_normalize_monthly() -> None:
    assert windowing.resolve_processing_date({"date": " 2026-04-09 "}, "2026-04-08") == "2026-04-09"
    assert windowing.resolve_processing_date(None, "2026-04-08") == "2026-04-08"
    assert windowing.resolve_business_date({"date": "2026-04-19"}, "2026-04-08", frequency="monthly") == "2026-04-01"
    assert windowing.resolve_business_date(None, "2026-04-08", frequency="hourly") == "2026-04-08"


def test_read_optional_float_and_load_settings(monkeypatch) -> None:
    monkeypatch.setenv("ROLLING_HOURS", " 6.5 ")
    monkeypatch.setenv("EIA_API_KEY", "abc")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    monkeypatch.setenv("SNOWFLAKE_APP_SCHEMA", "APP_GOLD")

    assert settings._read_optional_float("ROLLING_HOURS", 2.0) == 6.5
    assert settings.load_eia_settings().default_rolling_hours == 6.5

    snowflake_settings = settings.load_snowflake_settings(schema="RAW_ALT")
    assert snowflake_settings.schema == "RAW_ALT"
    assert snowflake_settings.database == "EIA_PIPELINE"

    app_settings = settings.load_app_snowflake_settings()
    assert app_settings.schema == "APP_GOLD"


def test_read_optional_float_defaults_and_invalid_input(monkeypatch) -> None:
    monkeypatch.delenv("ROLLING_HOURS", raising=False)
    assert settings._read_optional_float("ROLLING_HOURS", 2.0) == 2.0

    monkeypatch.setenv("ROLLING_HOURS", "none")
    assert settings._read_optional_float("ROLLING_HOURS", 2.0) == 2.0

    monkeypatch.setenv("ROLLING_HOURS", "bad")
    with pytest.raises(ValueError):
        settings._read_optional_float("ROLLING_HOURS", 2.0)


def test_load_settings_requires_mandatory_env(monkeypatch) -> None:
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(KeyError):
        settings.load_eia_settings()

    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    with pytest.raises(KeyError):
        settings.load_snowflake_settings()
