$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    & docker compose --profile orchestration up -d job-worker airflow
    if ($LASTEXITCODE -ne 0) { throw 'Scheduler startup failed' }
    Write-Host 'Airflow: http://localhost:8088 | local demo login: admin / demo-admin'
    Write-Host 'DAG retailrocket_daily_retrain runs daily at 02:00 Asia/Ho_Chi_Minh.'
} finally { Pop-Location }
