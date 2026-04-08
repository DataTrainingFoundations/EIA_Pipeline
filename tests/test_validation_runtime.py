from __future__ import annotations

import json
import os

import pytest

from pipeline.orchestration.validation_runtime import (
    DbtValidationError,
    _artifact_summary,
    _build_dbt_vars,
    _build_profiles_yml,
    _freshness_selector_for_cadence,
    _selector_for_cadence,
)


def test_selector_for_cadence_returns_expected_values() -> None:
    assert _selector_for_cadence("hourly") == "hourly_full_validation"
    assert _selector_for_cadence("monthly") == "monthly_full_validation"


def test_selector_for_cadence_rejects_unknown_values() -> None:
    with pytest.raises(DbtValidationError):
        _selector_for_cadence("weekly")


def test_freshness_selector_for_cadence_returns_expected_values() -> None:
    assert _freshness_selector_for_cadence("hourly") == "freshness_hourly_raw"
    assert _freshness_selector_for_cadence("monthly") == "freshness_monthly_raw"


def test_build_dbt_vars_collects_partition_dates() -> None:
    plan_summary = {
        "cadence_group": "hourly",
        "override_start_date": "2026-04-01",
        "override_end_date": "2026-04-02",
        "selected_dataset": "electricity_generation_hourly",
        "bootstrap_mode": True,
        "dataset_plans": {
            "electricity_generation_hourly": {"planned_partitions": ["2026-04-01", "2026-04-02"]},
            "electricity_demand_hourly": {"planned_partitions": ["2026-04-02"]},
        },
    }

    result = _build_dbt_vars(plan_summary)

    assert result == {
        "cadence_group": "hourly",
        "start_date": "2026-04-01",
        "end_date": "2026-04-02",
        "dataset_id": "electricity_generation_hourly",
        "bootstrap_mode": True,
        "partition_dates": ["2026-04-01", "2026-04-02"],
    }


def test_build_profiles_yml_uses_environment_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBT_SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("DBT_SNOWFLAKE_USER", "user")
    monkeypatch.setenv("DBT_SNOWFLAKE_PASSWORD", "secret")
    monkeypatch.setenv("DBT_SNOWFLAKE_ROLE", "SYSADMIN")
    monkeypatch.setenv("DBT_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    monkeypatch.setenv("DBT_SNOWFLAKE_DATABASE", "EIA_PIPELINE")
    monkeypatch.setenv("DBT_SNOWFLAKE_SCHEMA", "GOLD")
    monkeypatch.setenv("DBT_THREADS", "8")

    profile = _build_profiles_yml("default")

    assert "account: acct" in profile
    assert "threads: 8" in profile


def test_build_profiles_yml_requires_core_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("DBT_SNOWFLAKE_"):
            monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("DBT_SNOWFLAKE_ROLE", "SYSADMIN")
    monkeypatch.setenv("DBT_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    monkeypatch.setenv("DBT_SNOWFLAKE_DATABASE", "EIA_PIPELINE")
    monkeypatch.setenv("DBT_SNOWFLAKE_SCHEMA", "GOLD")

    with pytest.raises(DbtValidationError):
        _build_profiles_yml("default")


def test_artifact_summary_counts_results() -> None:
    artifact = {
        "results": [
            {"unique_id": "test.one", "status": "pass"},
            {"unique_id": "test.two", "status": "warn"},
            {"unique_id": "test.three", "status": "error"},
        ]
    }

    summary = _artifact_summary(artifact, artifact_type="tests")

    assert summary["counts"] == {"pass": 1, "warn": 1, "error": 1}
    assert json.dumps(summary)
