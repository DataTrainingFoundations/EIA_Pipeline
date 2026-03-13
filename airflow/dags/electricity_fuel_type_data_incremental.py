from __future__ import annotations

from airflow import DAG  # noqa: F401

from pipeline_dataset_dags import build_incremental_dag
from pipeline_support import get_dataset

dag = build_incremental_dag("electricity_fuel_type_data", get_dataset("electricity_fuel_type_data"))
