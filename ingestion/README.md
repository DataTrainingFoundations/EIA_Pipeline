# Ingestion

## Purpose
This folder owns the EIA API fetch flow and Kafka publishing step at the very start of the pipeline.

## Important Files
- `src/fetch_eia.py`: stable CLI entrypoint used by Airflow and local runs.
- `src/registry.py`: dataset registry loading and validation.
- `src/eia_api.py`: query parameter building and pagination.
- `src/event_factory.py`: Bronze event envelope construction.
- `src/publish_kafka.py`: Kafka producer creation and publish boundary.

## Add Something New
1. Add the dataset entry in `src/dataset_registry.yml`.
2. Choose a new Kafka topic name in that registry entry.
3. If the EIA endpoint needs special facets or columns, add them in the registry instead of hard-coding them in Python.
4. Run `python -m src.fetch_eia --dataset <dataset_id> --dry-run` to inspect the fetch result without publishing.
5. Add or update tests in `tests/test_fetch_eia.py` and `tests/test_publish_kafka.py`.
6. Follow the DAG wiring in [README.md](/C:/revature/EIA_Pipeline/airflow/dags/README.md) after the ingestion layer is correct.

## Follow This Function Next
- Start at `src/fetch_eia.py:main`.
- Then follow `load_dataset_registry`, `fetch_dataset_rows`, `build_event`, and `publish_events`.

## Relevant Tests
- `ingestion/tests/test_fetch_eia.py`
- `ingestion/tests/test_publish_kafka.py`

## Common Mistakes
- Creating topic names in code instead of the dataset registry.
- Adding endpoint-specific logic in `fetch_eia.py` instead of a helper module or registry config.
- Using `print` for operational logs instead of the shared structured logging pattern.
