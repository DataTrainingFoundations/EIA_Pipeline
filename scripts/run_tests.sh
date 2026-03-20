#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR="$REPO_ROOT/test-results"
JUNIT_PATH="$REPORT_DIR/junit.xml"
HTML_PATH="$REPORT_DIR/pytest-report.html"
COVERAGE_PATH="$REPO_ROOT/htmlcov/index.html"
QUALITY_TARGETS=(ingestion airflow app spark)

case "$MODE" in
  lint)
    ruff check "${QUALITY_TARGETS[@]}"
    ;;
  format)
    black "${QUALITY_TARGETS[@]}"
    ruff check --fix "${QUALITY_TARGETS[@]}"
    ;;
  format-check)
    black --check "${QUALITY_TARGETS[@]}"
    ;;
  quality)
    ruff check "${QUALITY_TARGETS[@]}"
    black --check "${QUALITY_TARGETS[@]}"
    pytest -q
    ;;
  fast)
    pytest -q ingestion/tests airflow/tests app/tests spark/tests/test_config.py spark/tests/test_bronze_cli.py spark/tests/test_gold_platinum_cli.py spark/tests/test_persona_cli.py spark/tests/test_silver_cli.py spark/tests/test_logging_utils.py
    ;;
  app-airflow)
    pytest -q airflow/tests app/tests
    ;;
  spark)
    pytest -q spark/tests
    ;;
  all)
    pytest -q
    ;;
  coverage)
    mkdir -p "$REPORT_DIR"
    pytest \
      --cov=ingestion/src \
      --cov=airflow/dags \
      --cov=app \
      --cov=admin_app \
      --cov=spark/common \
      --cov=spark/jobs \
      --cov-config=.coveragerc \
      --cov-report=term-missing \
      --cov-report=html \
      --cov-fail-under=70 \
      --junitxml="$JUNIT_PATH" \
      --html="$HTML_PATH" \
      --self-contained-html
    python - "$JUNIT_PATH" <<'PY'
import sys
import xml.etree.ElementTree as ET

junit_path = sys.argv[1]
root = ET.parse(junit_path).getroot()
failures = 0
errors = 0
skipped = 0
for suite in root.iter("testsuite"):
    failures += int(suite.attrib.get("failures", 0))
    errors += int(suite.attrib.get("errors", 0))
    skipped += int(suite.attrib.get("skipped", 0))
if failures or errors or skipped:
    raise SystemExit(
        f"Unexpected nonzero JUnit counts: failures={failures} errors={errors} skipped={skipped}"
    )
PY
    echo "Coverage HTML: $COVERAGE_PATH"
    echo "Pytest HTML: $HTML_PATH"
    echo "JUnit XML: $JUNIT_PATH"
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    echo "Use one of: lint, format, format-check, quality, fast, app-airflow, spark, all, coverage" >&2
    exit 1
    ;;
esac
