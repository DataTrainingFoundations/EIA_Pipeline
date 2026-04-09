from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.orchestration import bootstrap_runtime, ingest_runtime, transform_runtime, validation_runtime


def test_bootstrap_runtime_helpers(monkeypatch) -> None:
    assert bootstrap_runtime.should_continue_bootstrap({"a": {"has_more_bootstrap_work": True}}, progress_made=True) is True
    assert bootstrap_runtime.should_continue_bootstrap({}, progress_made=True) is False
    assert bootstrap_runtime.should_trigger_matching_transform(
        {
            "trigger_matching_transform": True,
            "dataset_plans": {"d": {"bootstrap_active": True}},
        },
        {"d": {"written": 1}},
        {"d": {}},
    ) is True
    assert bootstrap_runtime.chain_depth_exhausted({"bootstrap_chain_depth": "10"}) is True

    conf = bootstrap_runtime.next_bootstrap_conf({"bootstrap_chain_depth": "2"}, source_dag_id="ingest", source_dag_run_id="run:1")
    assert conf["bootstrap_chain_depth"] == 3
    assert conf["bootstrap_chain_origin"] == "ingest"

    monkeypatch.setattr(bootstrap_runtime, "build_trigger_run_id", lambda *args, **kwargs: "triggered-id")
    monkeypatch.setattr(bootstrap_runtime, "has_active_dag_run", lambda *args, **kwargs: False)
    called = {}
    import sys, types

    trigger_module = types.ModuleType("airflow.api.common.trigger_dag")
    trigger_module.trigger_dag = lambda **kwargs: called.update(kwargs)
    sys.modules["airflow.api.common.trigger_dag"] = trigger_module

    result = bootstrap_runtime.trigger_dag_if_idle("dag", conf={"bootstrap_chain_depth": "2"}, source_run_id="source:run")
    assert result["triggered"] is True
    assert called["run_id"] == "triggered-id"


def test_ingest_runtime_plan_run_and_finalize(monkeypatch) -> None:
    datasets = [
        {"id": "electricity_generation_hourly", "frequency": "hourly"},
        {"id": "electricity_demand_hourly", "frequency": "hourly"},
    ]
    monkeypatch.setattr(ingest_runtime, "iter_scheduled_ingest_cadence_datasets", lambda cadence: list(datasets))
    monkeypatch.setattr(
        ingest_runtime,
        "plan_ingest_windows",
        lambda *args, **kwargs: type(
            "Plan",
            (),
            {
                "dataset_id": args[1]["id"],
                "frequency": "hourly",
                "bootstrap_ingest_complete": False,
                "bootstrap_active": True,
                "bootstrap_strategy": "latest_first",
                "bootstrap_batch_start": "2026-04-08",
                "bootstrap_batch_end": "2026-04-09",
                "remaining_partitions_estimate": 3,
                "latest_target_partition": "2026-04-09",
                "has_more_bootstrap_work": True,
                "ingest_start_date": "2026-04-08",
                "ingest_end_date": "2026-04-09",
                "current_partition": "2026-04-09",
            },
        )(),
    )
    monkeypatch.setattr(ingest_runtime, "ingest_dataset", lambda *args, **kwargs: 5)
    monkeypatch.setattr(ingest_runtime, "_utc_now", lambda: "2026-04-09T12:00:00")
    monkeypatch.setattr(ingest_runtime, "get_pipeline_state", lambda *args, **kwargs: {"bootstrap_ingest_complete": False})
    captured = []
    monkeypatch.setattr(ingest_runtime, "update_ingest_state", lambda *args, **kwargs: captured.append(kwargs))

    plan = ingest_runtime.plan_ingest_cadence(None, "EIA_PIPELINE", "hourly", {"bootstrap_mode": "yes"})
    summary = ingest_runtime.run_ingest_cadence(None, object(), "EIA_PIPELINE", "hourly", plan)
    finalized = ingest_runtime.finalize_ingest_state(None, "EIA_PIPELINE", "hourly", plan, summary)

    assert plan["bootstrap_mode"] is True
    assert summary["electricity_generation_hourly"]["written"] == 5
    assert finalized["electricity_generation_hourly"]["has_more_bootstrap_work"] is True
    assert captured[0]["last_ingest_succeeded_at"] == "2026-04-09T12:00:00"


def test_transform_runtime_plan_build_and_finalize(monkeypatch) -> None:
    datasets = [{"id": "electricity_generation_hourly", "frequency": "hourly"}]
    monkeypatch.setattr(transform_runtime, "iter_scheduled_transform_cadence_datasets", lambda cadence: list(datasets))
    monkeypatch.setattr(
        transform_runtime,
        "plan_transform_partitions",
        lambda *args, **kwargs: type(
            "Plan",
            (),
            {
                "dataset_id": "electricity_generation_hourly",
                "frequency": "hourly",
                "bootstrap_transform_complete": False,
                "bootstrap_active": True,
                "bootstrap_priority": "latest_first",
                "scan_start_date": "2026-04-08",
                "scan_end_date": "2026-04-09",
                "planned_partitions": ["2026-04-09"],
                "pending_partitions": ["2026-04-09"],
                "stale_partitions": [],
                "remaining_pending_count": 0,
                "remaining_stale_count": 0,
                "raw_latest_partition": "2026-04-09",
                "current_partition": "2026-04-09",
                "has_more_bootstrap_work": False,
            },
        )(),
    )
    monkeypatch.setattr(transform_runtime, "run_silver_partitions", lambda *args, **kwargs: [{"partition_date": "2026-04-09"}])
    monkeypatch.setattr(transform_runtime, "run_gold_partitions", lambda *args, **kwargs: {"2026-04-09": {"fact_generation_hourly": 1}})
    monkeypatch.setattr(transform_runtime, "refresh_gold_dimensions", lambda *args, **kwargs: {"dim_fuel_type": 2})
    monkeypatch.setattr(transform_runtime, "_utc_now", lambda: "2026-04-09T12:00:00")
    monkeypatch.setattr(
        transform_runtime,
        "get_pipeline_state",
        lambda *args, **kwargs: {"bootstrap_transform_complete": False, "last_silver_partition": None, "last_gold_partition": None},
    )
    captured = []
    monkeypatch.setattr(transform_runtime, "update_transform_state", lambda *args, **kwargs: captured.append(kwargs))

    plan = transform_runtime.plan_transform_cadence(None, "EIA_PIPELINE", "hourly", {"bootstrap_mode": "yes"})
    silver_summary = transform_runtime.build_silver_for_cadence(None, "EIA_PIPELINE", "hourly", plan)
    gold_summary = transform_runtime.build_gold_for_cadence(None, "EIA_PIPELINE", "hourly", silver_summary)
    dims = transform_runtime.refresh_cadence_dimensions(None, "EIA_PIPELINE", "hourly")
    finalized = transform_runtime.finalize_transform_state(None, "EIA_PIPELINE", "hourly", plan, silver_summary, gold_summary)

    assert silver_summary["gold_partitions"] == ["2026-04-09"]
    assert gold_summary["results"]["2026-04-09"]["fact_generation_hourly"] == 1
    assert dims["dim_fuel_type"] == 2
    assert finalized["electricity_generation_hourly"]["bootstrap_transform_complete"] is True
    assert captured[0]["last_gold_partition"] == "2026-04-09"


def test_validation_runtime_helpers_and_error_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DBT_SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("DBT_SNOWFLAKE_USER", "user")
    monkeypatch.setenv("DBT_SNOWFLAKE_PASSWORD", "pw")
    monkeypatch.setenv("DBT_THREADS", "8")
    monkeypatch.setenv("DBT_PROJECT_DIR", str(tmp_path))

    assert validation_runtime._default_project_dir() == tmp_path.resolve()
    assert validation_runtime._default_target() == "default"
    assert validation_runtime._default_threads() == 8
    assert "threads: 8" in validation_runtime._build_profiles_yml("qa")
    assert validation_runtime._selector_for_cadence("hourly") == "hourly_full_validation"
    assert validation_runtime._freshness_selector_for_cadence("monthly") == "freshness_monthly_raw"
    assert validation_runtime._partitions_for_plan({"dataset_plans": {"a": {"planned_partitions": ["2026-04-09", ""]}}}) == ["2026-04-09"]
    assert validation_runtime._build_dbt_vars({"cadence_group": "hourly", "bootstrap_mode": 1})["bootstrap_mode"] is True
    assert validation_runtime._load_json_artifact(tmp_path / "missing.json") == {}
    assert validation_runtime._artifact_summary({}, artifact_type="tests")["status"] == "missing"

    artifact = {"results": [{"unique_id": "x", "status": "pass", "message": "ok"}]}
    assert validation_runtime._artifact_summary(artifact, artifact_type="tests")["counts"] == {"pass": 1}

    selectors_file = Path("warehouse/dbt/selectors.yml").read_text(encoding="utf-8")
    for selector in (
        "hourly_generation_validation",
        "hourly_demand_validation",
        "monthly_retail_sales_validation",
        "monthly_power_operational_validation",
    ):
        assert selector in selectors_file

    monkeypatch.setattr(validation_runtime, "_default_project_dir", lambda: tmp_path)
    monkeypatch.setattr(validation_runtime, "_resolve_dbt_binary", lambda: "dbt")
    with pytest.raises(validation_runtime.DbtValidationError):
        validation_runtime.run_dbt_validation({"cadence_group": ""})

    with pytest.raises(validation_runtime.DbtValidationError):
        validation_runtime._selector_for_cadence("weekly")


def test_validation_runtime_full_run_with_mocked_dbt(monkeypatch, tmp_path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "sources.json").write_text('{"sources":[{"unique_id":"s","status":"pass"}]}', encoding="utf-8")
    (target_dir / "run_results.json").write_text('{"results":[{"unique_id":"t","status":"pass"}]}', encoding="utf-8")
    monkeypatch.setenv("DBT_SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("DBT_SNOWFLAKE_USER", "user")
    monkeypatch.setenv("DBT_SNOWFLAKE_PASSWORD", "pw")
    monkeypatch.setattr(validation_runtime, "_default_project_dir", lambda: tmp_path)
    monkeypatch.setattr(validation_runtime, "_resolve_dbt_binary", lambda: "dbt")
    calls = []
    monkeypatch.setattr(
        validation_runtime,
        "_run_dbt_command",
        lambda command, **kwargs: calls.append(command) or type("Completed", (), {"returncode": 0})(),
    )

    summary = validation_runtime.run_dbt_validation(
        {
            "cadence_group": "hourly",
            "override_start_date": "2026-04-08",
            "override_end_date": "2026-04-09",
            "selected_dataset": "electricity_generation_hourly",
            "bootstrap_mode": False,
            "dataset_plans": {"electricity_generation_hourly": {"planned_partitions": ["2026-04-09"]}},
        }
    )

    assert summary["freshness"]["counts"] == {"pass": 1}
    assert summary["tests"]["counts"] == {"pass": 1}
    assert [item["name"] for item in summary["commands"]] == ["deps", "source_freshness", "test"]
    assert any("freshness_hourly_generation" in part for command in calls for part in command)
