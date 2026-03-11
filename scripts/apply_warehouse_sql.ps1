$ErrorActionPreference = 'Stop'

function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

$user = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { 'platform' }
$db = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { 'platform' }

Get-ChildItem 'warehouse/migrations/*.sql' | Sort-Object Name | ForEach-Object {
    $containerPath = "/docker-entrypoint-initdb.d/$($_.Name)"
    Write-Host "Applying warehouse/migrations/$($_.Name)..."
    Invoke-Checked { docker compose exec -T postgres psql -U $user -d $db -f $containerPath }
}