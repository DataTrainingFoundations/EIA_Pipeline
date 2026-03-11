param(
    [int]$Iterations = 24
)

$ErrorActionPreference = 'Stop'

function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

function Wait-BackfillRun {
    param([string]$RunId)
    while ($true) {
        $state = docker compose exec -T airflow airflow dags state electricity_region_data_backfill $RunId
        $state = $state.Trim()
        Write-Host "run $RunId state=$state"
        if ($state -eq 'success') { return }
        if ($state -eq 'failed') { throw "Backfill run $RunId failed" }
        Start-Sleep -Seconds 15
    }
}

Write-Host "[1/3] Starting containers..."
Invoke-Checked { docker compose up -d --build }

Write-Host "[2/3] Applying warehouse SQL on the running Postgres instance..."
Invoke-Checked { powershell -ExecutionPolicy Bypass -File scripts/apply_warehouse_sql.ps1 }

Write-Host "[3/3] Triggering $Iterations backfill run(s)..."
for ($iteration = 1; $iteration -le $Iterations; $iteration++) {
    Write-Host "Backfill iteration $iteration"
    $triggerOutput = docker compose exec -T airflow airflow dags trigger electricity_region_data_backfill
    $runIdLine = ($triggerOutput | Where-Object { $_ -match 'electricity_region_data_backfill' })
    if (-not $runIdLine) {
        throw "Could not determine run_id from trigger output"
    }
    $columns = $runIdLine -split '\|'
    $runId = $columns[2].Trim()
    Wait-BackfillRun -RunId $runId
}
