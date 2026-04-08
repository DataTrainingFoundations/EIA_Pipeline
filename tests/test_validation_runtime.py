from pipeline.orchestration.validation_runtime import (
    _datasets_with_planned_work,
    _freshness_selector_args,
    _selector_args,
    run_dbt_validation,
)


def test_datasets_with_planned_work_filters_empty_plans():
    plan_summary = {
        "dataset_plans": {
            "electricity_generation_hourly": {"planned_partitions": ["2026-04-08"]},
            "electricity_demand_hourly": {"planned_partitions": []},
        }
    }

    assert _datasets_with_planned_work(plan_summary) == ["electricity_generation_hourly"]


def test_selector_args_use_generation_selector_for_generation_only_hourly_run():
    selector_args = _selector_args(
        cadence_group="hourly",
        planned_datasets=["electricity_generation_hourly"],
    )

    assert selector_args == ["--selector", "hourly_generation_validation"]


def test_selector_args_union_multiple_dataset_selectors():
    selector_args = _selector_args(
        cadence_group="hourly",
        planned_datasets=["electricity_generation_hourly", "electricity_demand_hourly"],
    )

    assert selector_args == [
        "--selector",
        "hourly_demand_validation",
        "--selector",
        "hourly_generation_validation",
    ]


def test_freshness_selector_args_match_generation_only_hourly_run():
    selector_args = _freshness_selector_args(
        cadence_group="hourly",
        planned_datasets=["electricity_generation_hourly"],
    )

    assert selector_args == ["--selector", "freshness_hourly_generation"]


def test_run_dbt_validation_skips_when_no_planned_datasets(monkeypatch):
    monkeypatch.setattr(
        "pipeline.orchestration.validation_runtime._default_project_dir",
        lambda: __import__("pathlib").Path("."),
    )
    monkeypatch.setattr(
        "pipeline.orchestration.validation_runtime._resolve_dbt_binary",
        lambda: "dbt",
    )

    summary = run_dbt_validation(
        {
            "cadence_group": "hourly",
            "dataset_plans": {
                "electricity_generation_hourly": {"planned_partitions": []},
                "electricity_demand_hourly": {"planned_partitions": []},
            },
        }
    )

    assert summary["status"] == "skipped"
    assert summary["reason"] == "no_planned_partitions"
