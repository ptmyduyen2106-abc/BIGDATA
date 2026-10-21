#!/usr/bin/env bash
# Run from the project root; Apache Kafka 3.9.1 container is named by compose service.
set -euo pipefail
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
for topic in page_view add_to_cart purchase review; do
  docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 \
    --config retention.ms=604800000
done
docker compose exec -T "$KAFKA_SERVICE" "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" --describe
