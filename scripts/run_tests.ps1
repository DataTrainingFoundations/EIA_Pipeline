param(
    [ValidateSet("lint", "format", "format-check", "quality", "fast", "app-airflow", "spark", "all", "coverage")]
    [string]$Mode = "all"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$reportDir = Join-Path $repoRoot "test-results"
$junitPath = Join-Path $reportDir "junit.xml"
$htmlPath = Join-Path $reportDir "pytest-report.html"
$coveragePath = Join-Path $repoRoot "htmlcov"
$coverageIndexPath = Join-Path $coveragePath "index.html"
$qualityTargets = @("ingestion", "airflow", "app", "spark")

switch ($Mode) {
    "lint" {
        python -m ruff check @qualityTargets
    }
    "format" {
        python -m black @qualityTargets
        python -m ruff check --fix @qualityTargets
    }
    "format-check" {
        python -m black --check @qualityTargets
    }
    "quality" {
        python -m ruff check @qualityTargets
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
        python -m black --check @qualityTargets
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
        pytest -q
    }
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
        New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
        pytest `
            --cov=ingestion/src `
            --cov=airflow/dags `
            --cov=app `
            --cov=admin_app `
            --cov=spark/common `
            --cov=spark/jobs `
            --cov-config=.coveragerc `
            --cov-report=term-missing `
            --cov-report=html `
            --cov-fail-under=70 `
            --junitxml=$junitPath `
            --html=$htmlPath `
            --self-contained-html

        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        [xml]$junit = Get-Content $junitPath
        $failures = 0
        $errors = 0
        $skipped = 0
        foreach ($suite in $junit.SelectNodes("//testsuite")) {
            $failures += [int]$suite.failures
            $errors += [int]$suite.errors
            $skipped += [int]$suite.skipped
        }
        if ($failures -ne 0 -or $errors -ne 0 -or $skipped -ne 0) {
            Write-Error "Unexpected nonzero JUnit counts: failures=$failures errors=$errors skipped=$skipped"
            exit 1
        }

        Write-Host "Coverage HTML: $coverageIndexPath"
        Write-Host "Pytest HTML: $htmlPath"
        Write-Host "JUnit XML: $junitPath"
    }
}
