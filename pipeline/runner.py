"""One integrated execution: Kafka -> Spark -> Delta/HDFS -> NoSQL -> ALS -> serving."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import time

from pipeline.config import atomic_json, latest_run, pipeline_lock, resolve_data_run_id, run_path, utc_now, validate_run_id


def source_inventory(path, limit=0):
    from phase2.contract import build_event
    identifiers = set()
    counts = {}
    previous = None
    with Path(path).open(newline="", encoding="utf-8-sig") as stream:
        for index, row in enumerate(csv.DictReader(stream)):
            if limit and index >= limit:
                break
            event = build_event(row, "inventory")
            if previous is not None and event["timestamp"] < previous:
                raise ValueError("Replay CSV must be sorted by timestamp. Use scripts/sort_events.py first, or choose batch-history mode.")
            previous = event["timestamp"]
            identifiers.add(event["event_id"])
            counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1
    total = sum(counts.values())
    if not total:
        raise ValueError("Input dataset is empty")
    return {"rows": total, "unique_event_ids": len(identifiers), "duplicate_input_rows": total - len(identifiers), "topic_counts": counts, "input": str(path)}


def publish_model_registry(root, run_id, mongo_uri, redis_url):
    from pymongo import MongoClient
    import redis
    bundle = json.loads((root / "serving/model_bundle.json").read_text(encoding="utf-8"))
    registry = {key: bundle.get(key) for key in ("run_id", "model_version", "trained_at_utc", "algorithm", "metrics")}
    with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
        collection = client.bigdata.model_registry
        collection.create_index([("run_id", 1), ("model_version", 1)], unique=True)
        collection.update_one({"run_id": run_id, "model_version": registry["model_version"]}, {"$set": registry}, upsert=True)
    cache = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=5)
    try:
        cache.set(f"bigdata:{run_id}:active_model", json.dumps(registry))
    finally:
        cache.close()
    return registry


def execute(run_id, output_root="outputs", input_csv="data/sample/events_10000.csv", limit=0,
            quick=True, source_run_id=None, run_acid=True, batch_history=False):
    validate_run_id(run_id)
    root = run_path(run_id, output_root)
    if root.exists():
        raise FileExistsError(f"Run already exists: {run_id}; use a new run_id")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    started = time.perf_counter()
    report = {"run_id": run_id, "started_at_utc": utc_now(), "status": "running", "stages": {}, "source_run_id": source_run_id}
    spark = None
    with pipeline_lock(output_root):
        (root / "metrics").mkdir(parents=True)
        def note(stage):
            report["current_stage"] = stage
            atomic_json(root / "metrics/run_report.json", report)
            print(f"[{utc_now()}] {stage}", flush=True)
        try:
            note("source_and_ingestion")
            if source_run_id:
                report["source_data_run_id"] = resolve_data_run_id(source_run_id, output_root)
            if source_run_id is None and not batch_history:
                from pipeline.topics import ensure_topics
                from phase2.producer import main as produce
                from phase2.verify import main as verify
                inventory = source_inventory(input_csv, limit)
                report["source"] = inventory
                report["stages"]["topics"] = ensure_topics(bootstrap)
                producer_report = root / "metrics/producer.json"
                arguments = ["--input", input_csv, "--run-id", run_id, "--rate", "0", "--limit", str(limit), "--report", str(producer_report)]
                if produce(arguments) != 0:
                    raise RuntimeError("Kafka producer failed; see producer.json")
                if verify(["--run-id", run_id, "--bootstrap-servers", bootstrap, "--expected-total", str(inventory["rows"]), "--timeout", "90", "--report", str(root / "metrics/kafka_verify.json")]) != 0:
                    raise RuntimeError("Kafka verification failed")
            from phase3.streaming import create_spark, StreamConfig, start_ingest, start_features, await_queries, ProgressRecorder
            from phase3.snapshot import snapshot
            from phase3.schema import CLEAN_SCHEMA
            note("spark_startup_and_ingestion")
            spark = create_spark("local[2]", enable_delta=True, kafka=source_run_id is None and not batch_history, download_jars=True)
            spark.streams.addListener(ProgressRecorder(str(root / "metrics/streaming")))
            config = StreamConfig(run_id, output_root=str(Path(output_root) / "runs"), bootstrap_servers=bootstrap,
                                  max_offsets_per_trigger=max(report.get("source", {}).get("rows", 10000) * 5, 10000),
                                  feature_format="delta", trigger="available-now", filter_run_id=True)
            if batch_history:
                from pipeline.history import import_history
                report["stages"]["history_import"] = import_history(spark, input_csv, str(root), run_id)
            elif source_run_id:
                original = run_path(report["source_data_run_id"], output_root)
                if not (original / "clean").exists():
                    raise ValueError("Source run has no committed clean data")
                (spark.read.schema(CLEAN_SCHEMA).parquet(str(original / "clean"))
                 .write.mode("errorifexists").parquet(str(root / "clean")))
            else:
                await_queries(start_ingest(spark, config))
            accepted = spark.read.schema(CLEAN_SCHEMA).parquet(str(root / "clean")).count()
            if source_run_id is None and not batch_history and accepted != report["source"]["unique_event_ids"]:
                raise AssertionError(f"Accepted {accepted}, expected {report['source']['unique_event_ids']} distinct events. See watermark/progress.")
            report["clean_event_count"] = accepted
            note("windows_sessions_and_snapshot")
            await_queries(start_features(spark, config))
            report["stages"]["snapshot"] = snapshot(spark, config)
            from pipeline.storage import build_lake
            note("delta_hdfs_lake")
            report["stages"]["lake"] = build_lake(spark, str(root))
            from pipeline.online import publish_features
            note("online_mongodb_redis_features")
            report["stages"]["nosql"] = publish_features(spark, str(root), run_id, mongo_uri, redis_url)
            from pipeline.train import train_model
            note("als_temporal_cross_validation_and_training")
            report["stages"]["training"] = train_model(spark, str(root), run_id, quick=quick)
            if run_acid:
                from pipeline.acid_check import verify_acid
                note("delta_acid_time_travel")
                report["stages"]["acid"] = verify_acid(spark, str(root))
            note("publish_model_registry")
            report["stages"]["model_registry"] = publish_model_registry(root, run_id, mongo_uri, redis_url)
            # Activate only after all required outputs and registry publication succeeded.
            report["status"] = "completed"
            report["finished_at_utc"] = utc_now()
            report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
            atomic_json(root / "metrics/run_report.json", report)
            atomic_json(Path(output_root) / "latest_run.json", {"run_id": run_id, "activated_at_utc": utc_now(), "model_version": report["stages"]["model_registry"]["model_version"]})
        except Exception as error:
            report.update(status="failed", error=f"{type(error).__name__}: {error}", finished_at_utc=utc_now(), elapsed_seconds=round(time.perf_counter() - started, 3))
            atomic_json(root / "metrics/run_report.json", report)
            raise
        finally:
            if spark:
                spark.stop()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["demo", "retrain", "batch"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "outputs"))
    parser.add_argument("--input", default="data/sample/events_10000.csv")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--full-grid", action="store_true")
    parser.add_argument("--source-run-id")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("limit must be nonnegative")
    source = (args.source_run_id or latest_run(args.output_root)) if args.command == "retrain" else None
    print(json.dumps(execute(args.run_id, args.output_root, args.input, args.limit, not args.full_grid, source, batch_history=args.command == "batch"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
