# Airflow

## Purpose
This folder owns orchestration: hourly incrementals, historical backfills, disabled Bronze verification/repair code paths, and the platinum-serving DAG schedules.

## Important Files
- `dags/pipeline_factories.py`: DAG registration facade.
- `dags/pipeline_dataset_dags.py`: dataset DAG builders.
- `dags/pipeline_serving_dags.py`: business-serving DAG builders.
- `dags/pipeline_runtime.py`: registry/path/DB/Spark-submit helpers.
- `dags/pipeline_backfill.py`: backfill queue helpers.
- `dags/pipeline_repair.py`: inactive Bronze repair queue helpers kept on disk.
- `dags/pipeline_validation.py`: merge and validation helpers.

## Add Something New
1. Decide whether the new work is dataset orchestration, serving orchestration, or shared helper logic.
2. Add DAG composition in `dags/`, not in `README.md` or `scripts/`.
3. Reuse `pipeline_builders.py` for task creation before writing custom operators.
4. Keep PythonOperator callables in the helper modules, not inside the DAG file body.
5. Add tests under `airflow/tests`.

## Follow This Function Next
- Start with `pipeline_factories.register_all_dags`.
- Then follow the specific DAG builder in `pipeline_dataset_dags.py` or `pipeline_serving_dags.py`.
- For active queue state, follow into `pipeline_backfill.py`.

## Relevant Tests
- `airflow/tests/test_pipeline_runtime.py`
- `airflow/tests/test_pipeline_backfill.py`
- `airflow/tests/test_pipeline_repair.py`
- `airflow/tests/test_pipeline_validation.py`
- `airflow/tests/test_pipeline_factories.py`

## Common Mistakes
- Putting database mutation logic directly into DAG files.
- Logging task failures without the dataset id, chunk range, or target table.
- Creating new DAG files without reusing the shared builders first.
