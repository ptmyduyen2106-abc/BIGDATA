import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from phase2.contract import TOPICS


def ensure_topics(bootstrap_servers=None):
    admin = KafkaAdminClient(bootstrap_servers=(bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")).split(","), request_timeout_ms=30000)
    try:
        existing = set(admin.list_topics())
        for name in TOPICS:
            if name not in existing:
                try:
                    admin.create_topics([NewTopic(name, 3, 1, topic_configs={"retention.ms": "604800000"})])
                except TopicAlreadyExistsError:
                    pass
        deadline = time.monotonic() + 45
        while True:
            details = admin.describe_topics(list(TOPICS))
            ready = len(details) == len(TOPICS) and all(
                entry.get("error_code", 0) == 0 and len(entry.get("partitions", [])) >= 1
                and all(partition.get("leader", -1) >= 0 for partition in entry["partitions"])
                for entry in details)
            if ready:
                return [{"topic": entry["topic"], "partitions": len(entry["partitions"])} for entry in details]
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Kafka topic metadata did not become ready: {details}")
            time.sleep(1)
    finally:
        admin.close()


if __name__ == "__main__":
    print(ensure_topics())
