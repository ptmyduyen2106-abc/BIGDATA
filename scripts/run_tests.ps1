param([switch]$Integration)
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    & docker build -t retailrocket-phase17:1.0 .
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    $arguments=@('compose','run','--rm','--no-deps')
    if ($Integration) { $arguments+=@('-e','RUN_DELTA_TESTS=1','-e','RUN_ONLINE_TESTS=1','-e','BIGDATA_RUN_SPARK_ML_TESTS=1') }
    $arguments+=@('app','python','-m','scripts.test_all')
    & docker @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Tests failed' }
} finally { Pop-Location }
