# Warehouse

## Purpose
This folder contains schema migrations for the serving and metadata tables used by Airflow, Spark, and the business app.

## Important Files
- `migrations/`: ordered SQL migrations for metadata and serving tables.

## Add Something New
1. Add a new numbered migration in `migrations/`.
2. Keep serving-table DDL and metadata-table DDL easy to review.
3. If the table feeds Airflow validation, update the corresponding validation helpers or DAG wiring.
4. Apply the SQL with the scripts in [README.md](/C:/revature/EIA_Pipeline/scripts/README.md).

## Follow This Function Next
- Start with the newest migration file.
- Then follow the Spark job or Airflow validation helper that reads or writes the table.

## Relevant Tests
- `airflow/tests/test_pipeline_validation.py`
- app data-access tests when a new serving table is exposed to Streamlit

## Common Mistakes
- Changing a serving schema without updating the merge column list in Airflow.
- Adding a table that is never validated or surfaced.
- Mixing historical cleanup SQL with new forward-only schema changes.
