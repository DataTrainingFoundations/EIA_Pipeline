from __future__ import annotations

import sys
import types

from pipeline.core import registry
from pipeline.orchestration import bootstrap_runtime


def test_registry_filters_and_lookup(monkeypatch) -> None:
    datasets = [
        {
            "id": "electricity_generation_hourly",
            "frequency": "hourly",
            "cadence_group": "hourly",
            "transform_enabled": True,
            "scheduled_ingest": True,
            "scheduled_transform": True,
            "snowflake_table": "GEN_RAW",
        },
        {
            "id": "electricity_retail_sales_monthly",
            "frequency": "monthly",
            "cadence_group": "monthly",
            "transform_enabled": True,
            "scheduled_ingest": False,
            "scheduled_transform": False,
            "snowflake_table": "SALES_RAW",
        },
    ]
    monkeypatch.setattr(registry, "load_registry", lambda: list(datasets))

    assert registry.normalize_dataset_id("electricity_generation_hourly") == "electricity_generation"
    assert registry.iter_datasets() == datasets
    assert registry.iter_ingest_datasets() == datasets
    assert registry.iter_transform_datasets() == datasets
    assert registry.iter_cadence_datasets("hourly") == [datasets[0]]
    assert registry.iter_scheduled_ingest_datasets() == [datasets[0]]
    assert registry.iter_scheduled_transform_datasets() == [datasets[0]]
    assert registry.iter_scheduled_cadence_datasets("hourly") == [datasets[0]]
    assert registry.iter_scheduled_ingest_cadence_datasets("hourly") == [datasets[0]]
    assert registry.iter_scheduled_transform_cadence_datasets("hourly") == [datasets[0]]
    assert registry.iter_monthly_ingest_datasets() == [datasets[1]]
    assert registry.iter_monthly_transform_datasets() == [datasets[1]]
    assert registry.get_dataset("electricity_generation_hourly") == datasets[0]
    assert registry.get_raw_table_name("electricity_generation_hourly") == "GEN_RAW"
    assert registry.get_silver_table_name("electricity_generation_hourly") == "SILVER_ELECTRICITY_GENERATION"


def test_bootstrap_active_run_helpers(monkeypatch) -> None:
    class FakeField:
        def __eq__(self, other):
            return ("eq", other)

        def in_(self, values):
            return ("in", tuple(values))

        def __ne__(self, other):
            return ("ne", other)

    class DagRun:
        dag_id = FakeField()
        state = FakeField()
        run_id = FakeField()

    class Query:
        def __init__(self):
            self.has_row = True

        def filter(self, *args, **kwargs):
            return self

        def limit(self, _n):
            return self

        def first(self):
            return object() if self.has_row else None

    class FakeSession:
        def __init__(self):
            self.query_obj = Query()
            self.closed = False

        def query(self, model):
            return self.query_obj

        def close(self):
            self.closed = True

    session = FakeSession()
    airflow_models = types.ModuleType("airflow.models")
    airflow_models.DagRun = DagRun
    airflow_settings = types.ModuleType("airflow.settings")
    airflow_settings.Session = lambda: session
    sys.modules["airflow.models"] = airflow_models
    sys.modules["airflow.settings"] = airflow_settings

    assert bootstrap_runtime.has_active_dag_run("dag") is True
    session.query_obj.has_row = False
    assert bootstrap_runtime.has_active_dag_run("dag", exclude_run_id="run-1") is False

    monkeypatch.setattr(bootstrap_runtime, "has_active_dag_run", lambda *args, **kwargs: True)
    result = bootstrap_runtime.trigger_dag_if_idle("dag", conf={"bootstrap_chain_depth": "1"}, source_run_id="run")
    assert result == {"triggered": False, "reason": "active_run_exists", "dag_id": "dag"}
