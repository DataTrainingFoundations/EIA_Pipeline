from __future__ import annotations

from pipeline_serving_dags import build_resource_planning_dag

from airflow import DAG  # noqa: F401

dag = build_resource_planning_dag()
