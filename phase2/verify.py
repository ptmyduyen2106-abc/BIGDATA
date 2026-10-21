"""Finite Kafka consumer verification scoped to a single producer run_id."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

from .contract import TOPICS, ContractError, validate_event, utc_now


def inspect_record(record, run_id: str) -> dict | None:
    try:
        event = json.loads(record.value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, AttributeError) as exc:
        raise ContractError(f"Invalid UTF-8 JSON at {record.topic}:{record.partition}:{record.offset}") from exc
    if not isinstance(event, dict):
        raise ContractError("Kafka event must be a JSON object")
    if event.get("run_id") != run_id:
        return None
    validate_event(event, topic=record.topic)
    if record.key != str(event["visitorid"]).encode("utf-8"):
        raise ContractError("Kafka key must be visitorid encoded as UTF-8")
    return event


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True)
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--expected-total", type=int, default=1, help="Minimum matching records to observe")
    p.add_argument("--require-all-topics", action="store_true", help="Only for the four-topic synthetic fixture")
    p.add_argument("--timeout", type=float, default=45.0, help="Overall poll deadline in seconds")
    p.add_argument("--report", help="Optional JSON report path")
    args = p.parse_args(argv)
    if args.expected_total < 1 or not math.isfinite(args.timeout) or args.timeout <= 0 or not args.run_id.strip():
        p.error("expected-total and timeout must be positive; run-id must be nonempty")
    report = {"run_id": args.run_id, "status": "failed", "started_at": utc_now(),
              "expected_total": args.expected_total, "required_topics": list(TOPICS) if args.require_all_topics else [],
              "matched": 0, "unique_event_ids": 0, "duplicate_event_ids": 0,
              "topic_counts": {}, "skipped_other_runs": 0, "errors": []}
    consumer = None
    counts = Counter()
    event_ids = set()
    started = time.monotonic()
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(*TOPICS,
            bootstrap_servers=args.bootstrap_servers.split(","),
            group_id=f"phase2-verify-{uuid4().hex}",
            auto_offset_reset="earliest", enable_auto_commit=False,
            request_timeout_ms=15000, api_version_auto_timeout_ms=10000,
        )
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            remaining_ms = max(1, min(1000, int((deadline - time.monotonic()) * 1000)))
            batches = consumer.poll(timeout_ms=remaining_ms, max_records=500)
            for records in batches.values():
                for record in records:
                    event = inspect_record(record, args.run_id)
                    if event is None:
                        report["skipped_other_runs"] += 1
                        continue
                    counts[record.topic] += 1
                    report["matched"] += 1
                    if event["event_id"] in event_ids:
                        report["duplicate_event_ids"] += 1
                    event_ids.add(event["event_id"])
            enough_topics = not args.require_all_topics or set(TOPICS).issubset(counts)
            if report["matched"] >= args.expected_total and enough_topics:
                report["status"] = "success"
                break
        if report["status"] != "success":
            report["errors"].append("Deadline reached before required records/topics were observed")
    except Exception as exc:
        report["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        if consumer:
            consumer.close()
        report["topic_counts"] = dict(counts)
        report["unique_event_ids"] = len(event_ids)
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["finished_at"] = utc_now()
        if args.report:
            report_path = Path(args.report)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
