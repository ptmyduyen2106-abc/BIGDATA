#!/usr/bin/env bash
# Real console-producer -> console-consumer roundtrip in an isolated temporary topic.
set -euo pipefail
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
RUN_ID="cli-smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TOPIC="phase2_cli_smoke_${RUN_ID}"
KEY="9000001"
PAYLOAD="{\"run_id\":\"${RUN_ID}\",\"message\":\"Kafka CLI roundtrip\",\"synthetic\":true}"
CREATED=false
cleanup() {
  if [[ "$CREATED" == true ]]; then
    docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-topics.sh" \
      --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 --config retention.ms=60000
CREATED=true
printf '%s|%s\n' "$KEY" "$PAYLOAD" | \
  docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --property parse.key=true --property 'key.separator=|' --producer-property acks=all
ACTUAL="$(docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-console-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --from-beginning \
  --max-messages 1 --timeout-ms 15000 \
  --property print.key=true --property 'key.separator=|')"
ACTUAL="${ACTUAL//$'\r'/}"
EXPECTED="${KEY}|${PAYLOAD}"
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
  printf 'FAIL: CLI content mismatch. Expected: %s\nReceived: %s\n' "$EXPECTED" "$ACTUAL" >&2
  exit 1
fi
printf 'PASS: Kafka CLI producer/consumer roundtrip; run_id=%s; topic=%s\n' "$RUN_ID" "$TOPIC"
printf '%s\n' "$ACTUAL"
