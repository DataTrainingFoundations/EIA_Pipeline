from types import SimpleNamespace

import jobs.gold_region_fuel_serving_hourly as gold_job
from jobs.gold_region_fuel_serving_hourly import parse_args as parse_gold_args
from jobs.platinum_region_demand_daily import parse_args as parse_platinum_args


def test_parse_gold_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["gold_region_fuel_serving_hourly.py"])
    args = parse_gold_args()
    assert args.silver_base_path.startswith("s3a://")
    assert args.region_fact_path.startswith("s3a://gold/facts/")
    assert args.fuel_fact_path.startswith("s3a://gold/facts/")
    assert args.respondent_dim_path.startswith("s3a://gold/dimensions/")
    assert args.fuel_dim_path.startswith("s3a://gold/dimensions/")
    assert args.start is None
    assert args.end is None


def test_parse_platinum_args_defaults(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sys.argv", ["platinum_region_demand_daily.py"])
    args = parse_platinum_args()
    assert args.gold_input_path.startswith("s3a://gold/")
    assert args.platinum_table.startswith("platinum.")
    assert args.stage_table.startswith("platinum.")


def test_gold_main_skips_missing_partitioned_input(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "sys.argv",
        [
            "gold_region_fuel_serving_hourly.py",
            "--dataset",
            "electricity_fuel_type_data",
        ],
    )
    monkeypatch.setattr(gold_job, "configure_logging", lambda: None)
    monkeypatch.setattr(gold_job, "load_spark_app_config", lambda: SimpleNamespace())
    monkeypatch.setattr(
        gold_job, "build_spark_session", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        gold_job, "has_partitioned_parquet_input", lambda *_args, **_kwargs: False
    )
    monkeypatch.setattr(gold_job, "log_job_start", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gold_job, "log_job_complete", lambda *_args, **_kwargs: None)

    read_calls: list[tuple[object, str]] = []
    monkeypatch.setattr(
        gold_job,
        "read_partitioned_parquet",
        lambda spark, path, **_kwargs: read_calls.append((spark, path)),
    )

    gold_job.main()

    assert read_calls == []
