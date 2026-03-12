# Ingestion Source Map

## Purpose
This folder contains the step-by-step code used to fetch EIA rows and turn them into Bronze-ready Kafka events.

## Important Files
- `fetch_eia.py`: CLI orchestration only.
- `registry.py`: registry lookup and validation.
- `eia_api.py`: HTTP request construction and pagination.
- `event_factory.py`: deterministic event ids and event metadata.
- `publish_kafka.py`: Kafka publish boundary.
- `dataset_registry.yml`: source-of-truth dataset definitions.

## Add Something New
1. Add or update a dataset in `dataset_registry.yml`.
2. Keep fetch orchestration in `fetch_eia.py` thin.
3. Put endpoint-specific query logic in `eia_api.py` only if the registry cannot express it.
4. Put envelope or metadata changes in `event_factory.py`.
5. Add regression tests before changing the publish contract.

## Follow This Function Next
- `fetch_eia.main`
- `eia_api.fetch_dataset_rows`
- `event_factory.build_event`
- `publish_kafka.publish_events`

## Relevant Tests
- `ingestion/tests/test_fetch_eia.py`
- `ingestion/tests/test_publish_kafka.py`

## Common Mistakes
- Hiding dataset behavior in conditionals instead of the registry.
- Changing the event envelope without updating Bronze expectations.
- Forgetting to log source window, dataset id, and topic together.
