$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
$topic='phase17_cli_' + [guid]::NewGuid().ToString('N')
$payload='9000001|{"synthetic":true,"purpose":"phase17-cli-roundtrip"}'
$created=$false
try {
    & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic $topic --partitions 1 --replication-factor 1 --config retention.ms=60000
    if ($LASTEXITCODE -ne 0) { throw 'Cannot create diagnostic topic' }
    $created=$true
    $payload | & docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $topic --property parse.key=true --property 'key.separator=|' --producer-property acks=all
    if ($LASTEXITCODE -ne 0) { throw 'CLI producer failed' }
    $actual=& docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $topic --from-beginning --max-messages 1 --timeout-ms 20000 --property print.key=true --property 'key.separator=|'
    if ($LASTEXITCODE -ne 0 -or ($actual -join "`n").Trim() -ne $payload) { throw 'CLI roundtrip mismatch or timeout' }
    $report=@{status='passed'; topic=$topic; key='9000001'; exact_payload_match=$true; checked_at_utc=[DateTime]::UtcNow.ToString('o')}
    New-Item -ItemType Directory -Force outputs/evidence | Out-Null
    $report | ConvertTo-Json | Set-Content -LiteralPath outputs/evidence/kafka-cli.json -Encoding utf8
    Write-Host 'PASS: Kafka console producer and consumer exact keyed roundtrip'
} finally {
    if ($created) { & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic $topic }
    Pop-Location
}
