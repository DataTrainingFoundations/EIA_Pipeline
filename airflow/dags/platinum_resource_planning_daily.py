from __future__ import annotations

from airflow import DAG  # noqa: F401

from pipeline_serving_dags import build_resource_planning_dag

dag = build_resource_planning_dag()
