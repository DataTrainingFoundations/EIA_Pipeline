param(
    [ValidateSet("fast", "app-airflow", "spark", "all", "coverage")]
    [string]$Mode = "all"
)

switch ($Mode) {
    "fast" {
        pytest -q ingestion/tests airflow/tests app/tests spark/tests/test_config.py spark/tests/test_bronze_cli.py spark/tests/test_gold_platinum_cli.py spark/tests/test_persona_cli.py spark/tests/test_silver_cli.py spark/tests/test_logging_utils.py
    }
    "app-airflow" {
        pytest -q airflow/tests app/tests
    }
    "spark" {
        pytest -q spark/tests
    }
    "all" {
        pytest -q
    }
    "coverage" {
        pytest `
            --cov=ingestion/src `
            --cov=airflow/dags `
            --cov=app `
            --cov=admin_app `
            --cov=spark/common `
            --cov=spark/jobs `
            --cov-config=.coveragerc `
            --cov-report=term-missing `
            --cov-report=html
    }
}
