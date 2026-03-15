from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType


class _DagState:
    current_dag = None


class FakeDAG:
    def __init__(self, dag_id: str, **kwargs) -> None:  # noqa: ANN003
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.task_dict: dict[str, object] = {}

    def __enter__(self):  # noqa: ANN204
        _DagState.current_dag = self
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        _DagState.current_dag = None

    def get_task(self, task_id: str):  # noqa: ANN201
        return self.task_dict[task_id]


class FakeBaseOperator:
    def __init__(self, task_id: str, **kwargs) -> None:  # noqa: ANN003
        self.task_id = task_id
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()
        self.output = {"task_id": task_id}
        for key, value in kwargs.items():
            setattr(self, key, value)
        if _DagState.current_dag is not None:
            _DagState.current_dag.task_dict[task_id] = self

    def set_downstream(self, other):  # noqa: ANN201
        if isinstance(other, list):
            for item in other:
                self.set_downstream(item)
            return other
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def __rshift__(self, other):  # noqa: ANN201
        return self.set_downstream(other)

    def __rrshift__(self, other):  # noqa: ANN201
        if isinstance(other, list):
            for item in other:
                item.set_downstream(self)
            return self
        return other.set_downstream(self)


class FakeBashOperator(FakeBaseOperator):
    pass


class FakePythonOperator(FakeBaseOperator):
    pass


class FakeShortCircuitOperator(FakePythonOperator):
    pass


class FakeExternalTaskSensor(FakeBaseOperator):
    pass


class FakePythonSensor(FakeBaseOperator):
    pass


def _install_fake_airflow(monkeypatch) -> None:  # noqa: ANN001
    airflow_module = ModuleType("airflow")
    airflow_module.DAG = FakeDAG

    operators_module = ModuleType("airflow.operators")
    bash_module = ModuleType("airflow.operators.bash")
    bash_module.BashOperator = FakeBashOperator
    python_module = ModuleType("airflow.operators.python")
    python_module.PythonOperator = FakePythonOperator
    python_module.ShortCircuitOperator = FakeShortCircuitOperator
    python_module.get_current_context = lambda: {}

    sensors_module = ModuleType("airflow.sensors")
    external_task_module = ModuleType("airflow.sensors.external_task")
    external_task_module.ExternalTaskSensor = FakeExternalTaskSensor
    python_sensor_module = ModuleType("airflow.sensors.python")
    python_sensor_module.PythonSensor = FakePythonSensor

    utils_module = ModuleType("airflow.utils")
    trigger_rule_module = ModuleType("airflow.utils.trigger_rule")
    trigger_rule_module.TriggerRule = type("TriggerRule", (), {"ONE_FAILED": "one_failed"})

    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.bash", bash_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python_module)
    monkeypatch.setitem(sys.modules, "airflow.sensors", sensors_module)
    monkeypatch.setitem(sys.modules, "airflow.sensors.external_task", external_task_module)
    monkeypatch.setitem(sys.modules, "airflow.sensors.python", python_sensor_module)
    monkeypatch.setitem(sys.modules, "airflow.utils", utils_module)
    monkeypatch.setitem(sys.modules, "airflow.utils.trigger_rule", trigger_rule_module)


def _load_module(monkeypatch, module_name: str):  # noqa: ANN001, ANN201
    _install_fake_airflow(monkeypatch)
    monkeypatch.setenv("WORKSPACE_ROOT", str(Path(__file__).resolve().parents[2]))
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


REGION_DATASET = {
    "id": "electricity_region_data",
    "topic": "eia_electricity_region_data",
    "bronze_output_path": "s3a://bronze/region",
    "bronze_checkpoint_path": "s3a://bronze/checkpoints/region",
    "platinum_table": "platinum.region_demand_daily",
}

FUEL_DATASET = {
    "id": "electricity_fuel_type_data",
    "topic": "eia_electricity_fuel_type_data",
    "bronze_output_path": "s3a://bronze/fuel",
    "bronze_checkpoint_path": "s3a://bronze/checkpoints/fuel",
}


def test_pipeline_builders_render_expected_commands_and_tasks(monkeypatch) -> None:  # noqa: ANN001
    pipeline_builders = _load_module(monkeypatch, "pipeline_builders")

    fetch_command = pipeline_builders.build_fetch_command("electricity_region_data", "2026-03-10T00", "2026-03-10T01", 20)
    bronze_command = pipeline_builders.build_bronze_command(REGION_DATASET)
    bronze_pool = pipeline_builders.bronze_write_pool("electricity_region_data")
    silver_command = pipeline_builders.build_silver_command(REGION_DATASET, "electricity_region_data", "start", "end", validation_only=True)
    gold_command = pipeline_builders.build_curated_gold_command("electricity_region_data", "start", "end")
    platinum_command = pipeline_builders.build_region_daily_platinum_command("platinum.stage", "start", "end")
    verification_command = pipeline_builders.build_bronze_verification_command("electricity_region_data", REGION_DATASET, "ops.stage")
    merge_task = pipeline_builders.build_merge_task("merge_stage", "target.table", "stage.table", ["id", "value"], ["id"])
    validate_rows = pipeline_builders.build_validate_rows_task("validate_rows", "target.table", description="rows", allow_missing_table=True)
    validate_distinct = pipeline_builders.build_validate_distinct_task("validate_distinct", "target.table", "respondent", description="distinct")
    validate_bounds = pipeline_builders.build_validate_bounds_task("validate_bounds", "target.table", "coverage_ratio", description="bounds", min_value=0.0)
    sensor = pipeline_builders.build_curated_gold_sensor("wait_for_region", "electricity_region_data")
    first_backfill_sensor = pipeline_builders.build_first_backfill_sensor("wait_for_region_backfill", "electricity_region_data")

    assert "python -m src.fetch_eia --dataset electricity_region_data" in fetch_command
    assert "BRONZE_OUTPUT_PATH=s3a://bronze/region" in bronze_command
    assert bronze_pool == "electricity_region_data_bronze_write"
    assert "--validation-only" in silver_command
    assert "gold_region_fuel_serving_hourly.py" in gold_command
    assert "platinum_region_demand_daily.py" in platinum_command
    assert "bronze_hourly_coverage_verify.py" in verification_command
    assert merge_task.op_kwargs["allow_missing_stage"] is True
    assert validate_rows.op_kwargs["allow_missing_table"] is True
    assert validate_distinct.op_kwargs["column_name"] == "respondent"
    assert validate_bounds.op_kwargs["min_value"] == 0.0
    assert sensor.external_dag_id == "electricity_region_data_incremental"
    assert sensor.external_task_id == "spark_curated_gold_batch"
    assert sensor.deferrable is True
    assert first_backfill_sensor.op_kwargs["dataset_id"] == "electricity_region_data"
    assert first_backfill_sensor.mode == "reschedule"


def test_dataset_dag_builders_include_expected_tasks_for_region_and_fuel(monkeypatch) -> None:  # noqa: ANN001
    _load_module(monkeypatch, "pipeline_builders")
    pipeline_dataset_dags = _load_module(monkeypatch, "pipeline_dataset_dags")

    incremental_region = pipeline_dataset_dags.build_incremental_dag("electricity_region_data", REGION_DATASET)
    incremental_fuel = pipeline_dataset_dags.build_incremental_dag("electricity_fuel_type_data", FUEL_DATASET)
    backfill_region = pipeline_dataset_dags.build_backfill_dag("electricity_region_data", REGION_DATASET)
    assert "spark_platinum_stage" in incremental_region.task_dict
    assert "spark_platinum_stage" not in incremental_fuel.task_dict
    assert incremental_region.get_task("spark_curated_gold_batch").downstream_task_ids == {"spark_platinum_stage"}
    assert incremental_region.get_task("spark_bronze_batch").pool == "electricity_region_data_bronze_write"
    assert incremental_fuel.get_task("spark_bronze_batch").pool == "electricity_fuel_type_data_bronze_write"
    assert backfill_region.get_task("ingest_backfill_chunk").pool == "global_backfill_worker"
    assert backfill_region.get_task("spark_bronze_backfill_batch").pool == "global_backfill_worker"
    assert backfill_region.get_task("spark_silver_backfill").pool == "global_backfill_worker"
    assert backfill_region.get_task("spark_curated_gold_backfill").pool == "global_backfill_worker"
    assert incremental_region.get_task("validate_platinum_rows").op_kwargs["allow_empty_result"] is True
    assert incremental_region.get_task("validate_platinum_distinct_respondents").op_kwargs["allow_empty_result"] is True
    assert incremental_region.get_task("validate_platinum_nonnegative_demand").op_kwargs["allow_empty_result"] is True
    assert incremental_region.get_task("trigger_backfill_if_idle").upstream_task_ids == {"validate_platinum_nonnegative_demand"}
    assert incremental_fuel.get_task("trigger_backfill_if_idle").upstream_task_ids == {"spark_curated_gold_batch"}
    assert backfill_region.get_task("mark_backfill_complete").upstream_task_ids == {"validate_backfill_nonnegative_demand"}
    assert backfill_region.get_task("trigger_next_backfill_if_idle").upstream_task_ids == {"mark_backfill_complete"}
    assert backfill_region.get_task("trigger_next_backfill_after_failure_if_idle").upstream_task_ids == {"mark_backfill_failed"}
    assert backfill_region.get_task("validate_backfill_rows").op_kwargs["allow_empty_result"] is True
    assert backfill_region.get_task("validate_backfill_distinct_respondents").op_kwargs["allow_empty_result"] is True
    assert backfill_region.get_task("validate_backfill_nonnegative_demand").op_kwargs["allow_empty_result"] is True


def test_serving_dag_builders_include_validation_chain(monkeypatch) -> None:  # noqa: ANN001
    _load_module(monkeypatch, "pipeline_builders")
    pipeline_serving_dags = _load_module(monkeypatch, "pipeline_serving_dags")

    grid_dag = pipeline_serving_dags.build_grid_operations_dag()
    planning_dag = pipeline_serving_dags.build_resource_planning_dag()

    assert grid_dag.get_task("wait_for_region_first_backfill").upstream_task_ids == set()
    assert grid_dag.get_task("wait_for_fuel_first_backfill").upstream_task_ids == set()
    assert grid_dag.get_task("build_grid_operations_hourly_stage").upstream_task_ids == {
        "wait_for_region_first_backfill",
        "wait_for_fuel_first_backfill",
    }
    assert grid_dag.get_task("validate_grid_operations_renewable_share").upstream_task_ids == {"validate_grid_operations_coverage_ratio"}
    assert grid_dag.get_task("validate_grid_operations_rows").op_kwargs["allow_empty_result"] is True
    assert grid_dag.get_task("validate_grid_operations_respondents").op_kwargs["allow_empty_result"] is True
    assert grid_dag.get_task("validate_grid_operations_coverage_ratio").op_kwargs["allow_empty_result"] is True
    assert grid_dag.get_task("validate_grid_operations_renewable_share").op_kwargs["allow_empty_result"] is True
    assert planning_dag.get_task("validate_resource_planning_carbon_intensity").upstream_task_ids == {
        "validate_resource_planning_renewable_share"
    }
    assert planning_dag.get_task("wait_for_fuel_first_backfill").upstream_task_ids == set()
    assert planning_dag.get_task("build_resource_planning_daily_stage").upstream_task_ids == {
        "wait_for_region_first_backfill",
        "wait_for_fuel_first_backfill",
    }
    assert planning_dag.get_task("validate_resource_planning_rows").op_kwargs["allow_empty_result"] is True
    assert planning_dag.get_task("validate_resource_planning_respondents").op_kwargs["allow_empty_result"] is True
    assert planning_dag.get_task("validate_resource_planning_renewable_share").op_kwargs["allow_empty_result"] is True
    assert planning_dag.get_task("validate_resource_planning_carbon_intensity").op_kwargs["allow_empty_result"] is True


def test_factories_and_support_wrapper_integrate_with_real_registry(monkeypatch) -> None:  # noqa: ANN001
    pipeline_runtime = _load_module(monkeypatch, "pipeline_runtime")
    pipeline_support = _load_module(monkeypatch, "pipeline_support")
    pipeline_factories = _load_module(monkeypatch, "pipeline_factories")

    dags = pipeline_factories.register_all_dags()

    assert set(dags) == {
        "electricity_fuel_type_data_backfill",
        "electricity_fuel_type_data_incremental",
        "electricity_region_data_backfill",
        "electricity_region_data_incremental",
        "platinum_grid_operations_hourly",
        "platinum_resource_planning_daily",
    }
    assert pipeline_support.build_spark_submit_command is pipeline_runtime.build_spark_submit_command
    assert pipeline_support.validate_table_has_rows is not None


def test_wrapper_dag_modules_expose_expected_dag(monkeypatch) -> None:  # noqa: ANN001
    expected = {
        "electricity_region_data_incremental": "electricity_region_data_incremental",
        "electricity_region_data_backfill": "electricity_region_data_backfill",
        "electricity_fuel_type_data_incremental": "electricity_fuel_type_data_incremental",
        "electricity_fuel_type_data_backfill": "electricity_fuel_type_data_backfill",
        "platinum_grid_operations_hourly": "platinum_grid_operations_hourly",
        "platinum_resource_planning_daily": "platinum_resource_planning_daily",
    }

    for module_name, dag_id in expected.items():
        module = _load_module(monkeypatch, module_name)
        assert module.dag.dag_id == dag_id
