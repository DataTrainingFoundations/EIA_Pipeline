param(
    [string]$RunDate = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss')
)

$ErrorActionPreference = 'Stop'

function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

Write-Host "[1/4] Starting containers..."
Invoke-Checked { docker compose up -d --build }

Write-Host "[2/4] Applying warehouse SQL on the running Postgres instance..."
Invoke-Checked { powershell -ExecutionPolicy Bypass -File scripts/apply_warehouse_sql.ps1 }

Write-Host "[3/4] Running Airflow DAG test for $RunDate ..."
Invoke-Checked { docker compose exec --user airflow airflow airflow dags test electricity_region_data_incremental $RunDate }

Write-Host "[4/4] Validating rows in platinum.region_demand_daily ..."
Invoke-Checked { docker compose exec postgres psql -U platform -d platform -c "select min(date) as min_date, max(date) as max_date, count(*) as rows from platinum.region_demand_daily;" }
