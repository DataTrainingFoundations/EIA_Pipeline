# Airflow DAGs

## Purpose
This folder contains every Airflow DAG module plus the shared orchestration helpers those DAGs call.

## Important Files
- `pipeline_factories.py`: compatibility facade and bulk DAG registration.
- `pipeline_dataset_dags.py`: incremental, backfill, verification, and repair DAG builders.
- `pipeline_serving_dags.py`: platinum-serving DAG builders.
- `pipeline_builders.py`: task-construction helpers reused across DAGs.
- `pipeline_support.py`: compatibility wrapper for older imports.

## Add Something New
1. Add dataset DAGs in `pipeline_dataset_dags.py` when the work belongs to one ingestion dataset.
2. Add business-serving DAGs in `pipeline_serving_dags.py` when the work builds stable marts.
3. Reuse `pipeline_builders.py` to generate BashOperator and validation tasks.
4. If the new DAG needs queue state, put that logic in `pipeline_backfill.py` or `pipeline_repair.py`.
5. Add a thin top-level DAG file only when the scheduler needs a file-based entrypoint.

## Follow This Function Next
- `pipeline_factories.register_all_dags`
- `pipeline_dataset_dags.build_incremental_dag`
- `pipeline_builders.build_fetch_command`
- `pipeline_validation.merge_stage_into_target`

## Relevant Tests
- `airflow/tests/*`

## Common Mistakes
- Embedding SQL or queue logic directly inside a DAG definition.
- Creating task names that do not explain source, destination, or validation intent.
- Adding logs that omit `dataset_id`, `stage_table`, or the active time window.
