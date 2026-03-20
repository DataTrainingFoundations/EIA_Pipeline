from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType


class FakeDAG:
    def __init__(self, dag_id: str, **kwargs) -> None:  # noqa: ANN003
        self.dag_id = dag_id
        self.kwargs = kwargs


def _install_fake_airflow(monkeypatch) -> None:  # noqa: ANN001
    airflow_module = ModuleType("airflow")
    airflow_module.DAG = FakeDAG
    operators_module = ModuleType("airflow.operators")
    bash_module = ModuleType("airflow.operators.bash")
    python_module = ModuleType("airflow.operators.python")
    sensors_module = ModuleType("airflow.sensors")
    external_task_module = ModuleType("airflow.sensors.external_task")
    python_sensor_module = ModuleType("airflow.sensors.python")
    utils_module = ModuleType("airflow.utils")
    trigger_rule_module = ModuleType("airflow.utils.trigger_rule")
    operator_type = type("FakeOperator", (), {"__init__": lambda self, task_id, **kwargs: setattr(self, "task_id", task_id)})
    bash_module.BashOperator = operator_type
    python_module.PythonOperator = operator_type
    python_module.ShortCircuitOperator = operator_type
    python_module.get_current_context = lambda: {}
    external_task_module.ExternalTaskSensor = operator_type
    python_sensor_module.PythonSensor = operator_type
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


def test_register_all_dags_returns_expected_keys(monkeypatch) -> None:  # noqa: ANN001
    pipeline_factories = _load_module(monkeypatch, "pipeline_factories")

    dataset = {"topic": "eia_electricity_region_data"}
    monkeypatch.setattr(pipeline_factories, "load_dataset_registry", lambda: {"electricity_region_data": dataset})
    monkeypatch.setattr(
        pipeline_factories,
        "build_incremental_dag",
        lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_incremental"},
    )
    monkeypatch.setattr(
        pipeline_factories,
        "build_backfill_dag",
        lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_backfill"},
    )
    monkeypatch.setattr(pipeline_factories, "build_grid_operations_dag", lambda: {"dag_id": "platinum_grid_operations_hourly"})
    monkeypatch.setattr(
        pipeline_factories,
        "build_resource_planning_dag",
        lambda: {"dag_id": "platinum_resource_planning_daily"},
    )

    dags = pipeline_factories.register_all_dags()

    assert sorted(dags) == [
        "electricity_region_data_backfill",
        "electricity_region_data_incremental",
        "platinum_grid_operations_hourly",
        "platinum_resource_planning_daily",
    ]
