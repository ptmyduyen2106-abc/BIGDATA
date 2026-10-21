"""Mongo-backed idempotent live user/session projections with Redis serving keys.

Connectors are imported only on use. The reference implementation recomputes an
affected user's/item's full observed history, suitable for a bounded course demo.
It does not claim constant-memory production scalability.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable, Mapping

from pipeline.storage import EVENT_WEIGHTS, _save_json, utc_millis

EVENT_NAMES = {"view": "page_view", "addtocart": "add_to_cart", "transaction": "purchase", "review": "review"}
SESSION_GAP_MS = 30 * 60000


def _counts(events: list[dict]) -> dict:
    counts = Counter(event["event"] for event in events)
    result = {f"{name}_count": counts[raw] for raw, name in EVENT_NAMES.items()}
    result.update(event_count=len(events), unique_items=len({int(event["itemid"]) for event in events}),
                  interaction_score=sum(EVENT_WEIGHTS[event["event"]] for event in events))
    result["purchase_per_view"] = (counts["transaction"] / counts["view"] if counts["view"] else None)
    return result


def project_user(events: Iterable[Mapping], gap_ms: int = SESSION_GAP_MS) -> dict:
    """Pure projection of accepted events; retry/order/duplicate independent.

    Sessions are provisional observed-event sessions. Late events can bridge two
    sessions; callers replace the complete projected set and delete obsolete IDs.
    The latest 5m window ends at the next UTC minute after the latest user event.
    """
    unique = {str(event["event_id"]): dict(event) for event in events}
    ordered = sorted(unique.values(), key=lambda event: (int(event["timestamp"]), str(event["event_id"])))
    if not ordered:
        raise ValueError("Cannot project a user with no events")
    visitorid = int(ordered[0]["visitorid"])
    if any(int(event["visitorid"]) != visitorid for event in ordered):
        raise ValueError("project_user requires exactly one visitor")
    if gap_ms <= 0:
        raise ValueError("Session gap must be positive")
    sessions_events: list[list[dict]] = []
    for event in ordered:
        if not sessions_events or int(event["timestamp"]) - int(sessions_events[-1][-1]["timestamp"]) > gap_ms:
            sessions_events.append([event])
        else:
            sessions_events[-1].append(event)
    sessions = []
    for group in sessions_events:
        first_ms, last_ms = int(group[0]["timestamp"]), int(group[-1]["timestamp"])
        sessions.append({
            "visitorid": visitorid,
            "session_id": hashlib.sha256(f"{visitorid}:{first_ms}".encode()).hexdigest(),
            "session_start_ms": first_ms, "session_end_ms": last_ms + gap_ms,
            "first_event_time_utc": utc_millis(first_ms), "last_event_time_utc": utc_millis(last_ms),
            "active_duration_seconds": (last_ms - first_ms) / 1000,
            "provisional": True, "projection_source": "accepted_clean_events",
            **_counts(group),
        })
    latest_ms = int(ordered[-1]["timestamp"])
    window_end = (latest_ms // 60000 + 1) * 60000
    window_start = window_end - 5 * 60000
    recent = [event for event in ordered if window_start <= int(event["timestamp"]) < window_end]
    return {
        "visitorid": visitorid, "seen_items": sorted({int(event["itemid"]) for event in ordered}),
        "first_event_ms": int(ordered[0]["timestamp"]), "last_event_ms": latest_ms,
        **_counts(ordered),
        "latest_window": {"window_start_ms": window_start, "window_end_ms": window_end,
                          "anchor": "latest_observed_user_event", "provisional": True, **_counts(recent)},
        "sessions": sessions, "current_session": sessions[-1],
    }


def _event_document(event: Mapping, run_id: str) -> dict:
    document = {name: event.get(name) for name in (
        "event_id", "schema_version", "timestamp", "visitorid", "itemid", "event", "event_type",
        "transactionid", "ingested_at", "review_text", "rating", "synthetic",
    ) if event.get(name) is not None}
    document.update(run_id=run_id, source_run_id=event.get("run_id"))
    # Avoid collecting TimestampType into host-local naive Python datetimes.
    for name in ("timestamp", "visitorid", "itemid"):
        document[name] = int(document[name])
    return document


class OnlinePublisher:
    """One writer per run; Mongo durable facts precede replaceable projections."""
    def __init__(self, mongo_client, redis_client, run_id: str):
        self.mongo_client, self.redis = mongo_client, redis_client
        self.run_id = run_id
        self.db = mongo_client["bigdata"]
        self.events = self.db["events"]
        self.users = self.db["user_features"]
        self.sessions = self.db["sessions"]
        self.items = self.db["item_features"]
        self.events.create_index([("run_id", 1), ("event_id", 1)], unique=True)
        self.events.create_index([("run_id", 1), ("visitorid", 1), ("timestamp", 1)])
        self.events.create_index([("run_id", 1), ("itemid", 1)])
        self.users.create_index([("run_id", 1), ("visitorid", 1)], unique=True)
        self.sessions.create_index([("run_id", 1), ("visitorid", 1), ("session_id", 1)], unique=True)
        self.items.create_index([("run_id", 1), ("itemid", 1)], unique=True)

    def consume(self, events: Iterable[Mapping]) -> dict:
        touched_users, touched_items = set(), set()
        received = 0
        for event in events:
            received += 1
            document = _event_document(event, self.run_id)
            self.events.update_one({"run_id": self.run_id, "event_id": document["event_id"]},
                                   {"$setOnInsert": document}, upsert=True)
            touched_users.add(document["visitorid"])
            touched_items.add(document["itemid"])
        for visitorid in sorted(touched_users):
            selector = {"run_id": self.run_id, "visitorid": visitorid}
            projection = project_user(self.events.find(selector, {"_id": 0}))
            sessions = projection.pop("sessions")
            projection.update(run_id=self.run_id)
            for session in sessions:
                session.update(run_id=self.run_id)
            self.users.update_one(selector, {"$set": projection}, upsert=True)
            session_ids = []
            for session in sessions:
                session_ids.append(session["session_id"])
                self.sessions.update_one({**selector, "session_id": session["session_id"]},
                                         {"$set": session}, upsert=True)
            # A late bridge can merge previously separate sessions. Remove the
            # obsolete projection IDs, while immutable event facts remain intact.
            self.sessions.delete_many({**selector, "session_id": {"$nin": session_ids}})
            prefix = f"bigdata:{self.run_id}"
            pipe = self.redis.pipeline(transaction=True)
            seen_key = f"{prefix}:seen:{visitorid}"
            pipe.delete(seen_key)
            if projection["seen_items"]:
                pipe.sadd(seen_key, *map(str, projection["seen_items"]))
            for suffix, value in ((f"features:{visitorid}", projection),
                                  (f"window:{visitorid}", projection["latest_window"]),
                                  (f"session:{visitorid}", projection["current_session"])):
                pipe.set(f"{prefix}:{suffix}", json.dumps(value, ensure_ascii=False, sort_keys=True))
            pipe.execute()
        for itemid in sorted(touched_items):
            selector = {"run_id": self.run_id, "itemid": itemid}
            history = list(self.events.find(selector, {"_id": 0}))
            projection = {**selector, **_counts(history),
                          "unique_visitors": len({event["visitorid"] for event in history}),
                          "last_event_ms": max(int(event["timestamp"]) for event in history)}
            self.items.update_one(selector, {"$set": projection}, upsert=True)
        return {"received_rows": received, "affected_users": len(touched_users),
                "affected_items": len(touched_items),
                "durable_events": self.events.count_documents({"run_id": self.run_id}),
                "users": self.users.count_documents({"run_id": self.run_id}),
                "sessions": self.sessions.count_documents({"run_id": self.run_id})}


def _connect(run_id: str, mongo_uri: str, redis_url: str) -> OnlinePublisher:
    from pymongo import MongoClient
    import redis
    mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    cache = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=10,
                                 socket_connect_timeout=10)
    try:
        mongo.admin.command("ping")
        cache.ping()
        return OnlinePublisher(mongo, cache, run_id)
    except Exception:
        mongo.close()
        cache.close()
        raise


def _publish_stream_sinks(spark, root: Path, publisher: OnlinePublisher) -> dict:
    """Cache actual Phase 3 output separately from provisional live projections."""
    from pyspark.sql import functions as F, Window
    report = {}
    for name, sort_column, redis_kind in (("windows", "window_end", "stream_window"),
                                           ("sessions", "session_end", "final_session")):
        path = root / name
        if (path / "_delta_log").exists():
            from delta.tables import DeltaTable
            if not DeltaTable.isDeltaTable(spark, path.as_posix()):
                report[name] = {"status": "initial_commit_pending", "rows": 0}
                continue
            frame = spark.read.format("delta").load(path.as_posix())
        elif path.exists() and any(path.glob("*.parquet")):
            frame = spark.read.parquet(path.as_posix())
        else:
            report[name] = {"status": "not_available", "rows": 0}
            continue
        latest = (frame.withColumn("_rank", F.row_number().over(
            Window.partitionBy("visitorid").orderBy(F.col(sort_column).desc())))
                  .filter("_rank = 1").drop("_rank"))
        rows = 0
        for text in latest.toJSON().toLocalIterator():
            document = json.loads(text)
            visitorid = int(document["visitorid"])
            document.update(run_id=publisher.run_id, projection_source=f"phase3_{name}")
            publisher.redis.set(f"bigdata:{publisher.run_id}:{redis_kind}:{visitorid}",
                                json.dumps(document, ensure_ascii=False, sort_keys=True))
            if name == "windows":
                publisher.users.update_one({"run_id": publisher.run_id, "visitorid": visitorid},
                                           {"$set": {"stream_window": document}}, upsert=True)
            rows += 1
        report[name] = {"status": "published", "latest_user_rows": rows}
    return report


def publish_features(spark, run_root: str, run_id: str,
                     mongo_uri: str = "mongodb://mongodb:27017",
                     redis_url: str = "redis://redis:6379/0") -> dict:
    from phase3.schema import CLEAN_SCHEMA
    root = Path(run_root)
    publisher = _connect(run_id, mongo_uri, redis_url)
    try:
        clean = spark.read.schema(CLEAN_SCHEMA).parquet((root / "clean").as_posix())
        report = publisher.consume(row.asDict(recursive=True) for row in clean.toLocalIterator())
        report.update(status="completed", run_id=run_id,
                      stream_sinks=_publish_stream_sinks(spark, root, publisher),
                      created_at=datetime.now(timezone.utc).isoformat())
        _save_json(root, "online_publish.json", report)
        return report
    finally:
        publisher.mongo_client.close()
        publisher.redis.close()


def main() -> None:
    from phase3.schema import CLEAN_SCHEMA
    from phase3.streaming import StreamConfig, await_queries, create_spark
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default="outputs")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", os.getenv("MONGODB_URI", "mongodb://mongodb:27017")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis:6379/0"))
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--duration-seconds", type=float, default=0)
    parser.add_argument("--processing-interval", default="5 seconds")
    parser.add_argument("--master")
    parser.add_argument("--no-download-jars", action="store_true")
    args = parser.parse_args()
    config = StreamConfig(args.run_id, output_root=(Path(args.output_root) / "runs").as_posix())
    config.prepare()
    # The continuous clean Parquet path uses no Delta reader or Kafka JAR.
    spark = create_spark(args.master, enable_delta=not args.continuous,
                         download_jars=not args.no_download_jars and not args.continuous)
    publisher = None
    try:
        if not args.continuous:
            print(json.dumps(publish_features(spark, config.root.as_posix(), config.run_id,
                                              args.mongo_uri, args.redis_url), indent=2))
            return
        publisher = _connect(config.run_id, args.mongo_uri, args.redis_url)

        def write_batch(batch, batch_id):
            # foreachBatch is at-least-once. Mongo immutable event upserts plus
            # recomputed SET projections make retries converge without $inc.
            report = publisher.consume(row.asDict(recursive=True) for row in batch.toLocalIterator())
            report.update(batch_id=batch_id, status="completed", run_id=config.run_id,
                          created_at=datetime.now(timezone.utc).isoformat())
            _save_json(config.root, f"online-batch-{batch_id:06d}.json", report)

        clean = spark.readStream.schema(CLEAN_SCHEMA).parquet(config.path("clean"))
        query = (clean.writeStream.foreachBatch(write_batch).outputMode("append")
                 .queryName(f"{config.run_id}_online")
                 .option("checkpointLocation", config.checkpoint("online"))
                 .trigger(processingTime=args.processing_interval).start())
        await_queries([query], args.duration_seconds)
    finally:
        if publisher is not None:
            publisher.mongo_client.close()
            publisher.redis.close()
        spark.stop()


if __name__ == "__main__":
    main()
