from pipeline_runtime import _floor_to_step, build_spark_submit_command, format_log_fields


def test_build_spark_submit_command_quotes_airflow_templates() -> None:
    command = build_spark_submit_command(
        "silver_clean_transform.py",
        job_name="silver_clean_transform",
        application_args=["--start", "{{ data_interval_start.isoformat() }}"],
    )
    assert "silver_clean_transform.py" in command
    assert '"{{ data_interval_start.isoformat() }}"' in command
    assert "--name silver_clean_transform" in command
    assert "--conf spark.cores.max=4" in command
    assert "--conf spark.executor.cores=2" in command


def test_floor_to_step_rounds_to_day_boundary() -> None:
    from datetime import datetime, timezone

    value = datetime(2026, 3, 11, 14, 52, 10, tzinfo=timezone.utc)
    assert _floor_to_step(value, "day").isoformat() == "2026-03-11T00:00:00+00:00"


def test_format_log_fields_skips_empty_values() -> None:
    assert format_log_fields(dataset_id="electricity_region_data", run_id=None, task_id="ingest") == "dataset_id=electricity_region_data task_id=ingest"
