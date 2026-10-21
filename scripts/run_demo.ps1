param([string]$RunId = ('demo-' + (Get-Date -Format 'yyyyMMdd-HHmmss')), [switch]$FullGrid)
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    & docker version --format '{{.Server.Version}}'
    if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop Linux engine first' }
    & docker build -t retailrocket-phase17:1.0 .
    if ($LASTEXITCODE -ne 0) { throw 'Image build failed' }
    & docker compose up -d --wait kafka namenode datanode mongodb redis
    if ($LASTEXITCODE -ne 0) { throw 'Infrastructure failed health checks' }
    $arguments=@('compose','run','--rm','--no-deps','app','python','-m','pipeline.runner','demo','--run-id',$RunId)
    if ($FullGrid) { $arguments+='--full-grid' }
    & docker @arguments
    if ($LASTEXITCODE -ne 0) { throw "Pipeline failed. Read outputs/runs/$RunId/metrics/run_report.json" }
    & docker compose up -d --no-deps api
    if ($LASTEXITCODE -ne 0) { throw 'API startup failed' }
    & docker compose run --rm --no-deps app python -m scripts.verify_e2e --base-url http://api:8000 --run-id $RunId
    if ($LASTEXITCODE -ne 0) { throw 'End-to-end verification failed' }
    Write-Host "Dashboard: http://localhost:8000 | OpenAPI: http://localhost:8000/docs | Run: $RunId"
} finally { Pop-Location }
