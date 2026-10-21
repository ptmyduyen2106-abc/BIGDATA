"""CSV -> four Kafka topics; bounded acknowledgements and offline JSONL mode."""
from __future__ import annotations

import argparse
from collections import Counter, deque
import csv
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Callable, Iterable

from .contract import ContractError, RAW_FIELDS, build_event, serialize_event, utc_now


def read_events(path: str | Path, run_id: str, limit: int = 0) -> Iterable[dict]:
    with Path(path).open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames or not set(RAW_FIELDS).issubset(reader.fieldnames):
            raise ContractError(f"CSV requires columns {', '.join(RAW_FIELDS)}")
        for index, row in enumerate(reader, 1):
            if limit and index > limit:
                break
            try:
                yield build_event(row, run_id)
            except ContractError as exc:
                raise ContractError(f"CSV data row {index}: {exc}") from exc


def publish(events: Iterable[dict], send: Callable, *, rate: float = 100.0,
            max_pending: int = 256, ack_timeout: float = 30.0,
            pace_from_timestamps: bool = False, time_scale: float = 1.0,
            progress: dict | None = None) -> dict:
    """Publish with bounded in-flight futures; count only acknowledged records."""
    if (not all(math.isfinite(v) for v in (rate, ack_timeout, time_scale))
            or rate < 0 or max_pending < 1 or ack_timeout <= 0 or time_scale <= 0):
        raise ValueError("Invalid rate, pending limit, acknowledgement timeout or time scale")
    pending = deque()
    counts, acknowledged_counts = Counter(), Counter()
    started = time.monotonic()
    sent = acknowledged = 0
    first_timestamp = previous_timestamp = None

    def record_progress():
        if progress is not None:
            progress.update(sent=sent, acknowledged=acknowledged, topic_counts=dict(counts),
                            acknowledged_topic_counts=dict(acknowledged_counts))

    record_progress()

    def acknowledge_one():
        nonlocal acknowledged
        future, topic = pending.popleft()
        future.get(timeout=ack_timeout)
        acknowledged += 1
        acknowledged_counts[topic] += 1
        record_progress()

    for event in events:
        if first_timestamp is None:
            first_timestamp = event["timestamp"]
        if pace_from_timestamps and previous_timestamp is not None and event["timestamp"] < previous_timestamp:
            raise ContractError("Timestamp pacing requires CSV sorted by timestamp; source event timestamps were preserved")
        target_elapsed = sent / rate if rate else 0.0
        if pace_from_timestamps:
            target_elapsed = max(target_elapsed, (event["timestamp"] - first_timestamp) / 1000 / time_scale)
        delay = target_elapsed - (time.monotonic() - started)
        if delay > 0:
            time.sleep(delay)
        topic = event["event_type"]
        pending.append((send(topic, str(event["visitorid"]).encode("utf-8"), serialize_event(event)), topic))
        sent += 1
        counts[topic] += 1
        record_progress()
        previous_timestamp = event["timestamp"]
        if len(pending) >= max_pending:
            acknowledge_one()
    while pending:
        acknowledge_one()
    if not sent:
        raise ContractError("Input contains no events")
    elapsed = time.monotonic() - started
    return {"sent": sent, "acknowledged": acknowledged, "topic_counts": dict(counts),
            "acknowledged_topic_counts": dict(acknowledged_counts),
            "elapsed_seconds": round(elapsed, 3),
            "events_per_second": round(acknowledged / elapsed, 3) if elapsed else None}


class _Written:
    def get(self, timeout=None):
        return None


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="RetailRocket CSV or labeled synthetic fixture")
    p.add_argument("--run-id", required=True, help="Trace label; excluded from stable event_id")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--limit", type=int, default=0, help="Maximum records; 0 means all")
    p.add_argument("--rate", type=float, default=100.0, help="Maximum events/second; 0 means unrestricted")
    p.add_argument("--max-pending", type=int, default=256)
    p.add_argument("--ack-timeout", type=float, default=30.0)
    p.add_argument("--pace-from-timestamps", action="store_true", help="Requires time-sorted CSV")
    p.add_argument("--time-scale", type=float, default=1.0, help="10 replays source time ten times faster")
    p.add_argument("--dry-run", action="store_true", help="Write JSONL without Kafka")
    p.add_argument("--output", help="Dry-run JSONL path (default: outputs/<run-id>/events.jsonl)")
    p.add_argument("--report", help="Metrics JSON path (default: outputs/<run-id>/producer.json)")
    return p


def main(argv=None) -> int:
    p = parser()
    args = p.parse_args(argv)
    if (not all(math.isfinite(v) for v in (args.rate, args.ack_timeout, args.time_scale))
            or args.limit < 0 or args.rate < 0 or args.max_pending < 1 or args.ack_timeout <= 0 or args.time_scale <= 0):
        p.error("limit/rate must be non-negative; max-pending, ack-timeout, time-scale must be positive")
    if not args.run_id.strip():
        p.error("run-id cannot be empty")
    # run_id is a label, not an unrestricted filesystem path.
    safe_run = "".join(c if c.isalnum() or c in "-_" else "_" for c in args.run_id)
    report_path = Path(args.report or f"outputs/{safe_run}/producer.json")
    if report_path.resolve() == Path(args.input).resolve():
        p.error("report must differ from input CSV")
    report = {"run_id": args.run_id, "input": str(Path(args.input)), "started_at": utc_now(),
              "mode": "dry_run" if args.dry_run else "kafka", "status": "failed"}
    producer = sink = None
    try:
        if args.dry_run:
            output_path = Path(args.output or f"outputs/{safe_run}/events.jsonl")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.resolve() == Path(args.input).resolve():
                raise ValueError("Dry-run output must differ from input CSV")
            if output_path.resolve() == report_path.resolve():
                raise ValueError("Dry-run output and report must use different paths")
            sink = output_path.open("w", encoding="utf-8", newline="\n")
            def send(topic, key, value):
                sink.write(value.decode("utf-8") + "\n")
                return _Written()
            report["output"] = str(output_path)
        else:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers.split(","),
                acks="all", enable_idempotence=True, retries=5,
                max_in_flight_requests_per_connection=1,
                linger_ms=10, request_timeout_ms=30000, max_block_ms=30000,
            )
            def send(topic, key, value):
                return producer.send(topic, key=key, value=value)
        metrics = publish(read_events(args.input, args.run_id, args.limit), send,
                          rate=args.rate, max_pending=args.max_pending, ack_timeout=args.ack_timeout,
                          pace_from_timestamps=args.pace_from_timestamps, time_scale=args.time_scale,
                          progress=report)
        if sink:
            sink.flush()
        report.update(metrics)
        report["status"] = "success"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if sink:
            sink.close()
        if producer:
            try:
                producer.close(timeout=args.ack_timeout)
            except Exception as exc:
                report["status"] = "failed"
                report.setdefault("error", f"Producer close failed: {exc}")
        report["finished_at"] = utc_now()
        # A filesystem write is not a Kafka acknowledgement, including partial failures.
        if args.dry_run and "acknowledged" in report:
            report["written"] = report.pop("acknowledged")
            report["written_topic_counts"] = report.pop("acknowledged_topic_counts")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
