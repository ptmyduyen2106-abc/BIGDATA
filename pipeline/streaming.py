"""Live Kafka -> Spark -> Delta/Mongo/Redis, using independent checkpoints.

Only one process may write lake/online projections for a run. Retraining is an
independent explicit operation; streaming does not silently replace an ML model.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path

from pipeline.online import publish_features
from pipeline.storage import _save_json, build_lake


def main() -> None:
    from phase3.streaming import (
        ProgressRecorder, StreamConfig, await_queries, create_spark,
        read_clean, start_features, start_ingest,
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default="outputs",
                        help="Unified output root; run data is stored at ROOT/runs/RUN_ID")
    parser.add_argument("--source", choices=["kafka", "files"], default="kafka")
    parser.add_argument("--input-path")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", os.getenv("MONGODB_URI", "mongodb://mongodb:27017")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis:6379/0"))
    parser.add_argument("--duration-seconds", type=float, default=0)
    parser.add_argument("--trigger", choices=["processing", "available-now"], default="processing",
                        help="available-now drains committed input and finishes callbacks without a timed cancellation")
    parser.add_argument("--processing-interval", default="5 seconds")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    parser.add_argument("--filter-run-id", dest="filter_run_id", action="store_true", default=True)
    parser.add_argument("--all-source-runs", dest="filter_run_id", action="store_false")
    parser.add_argument("--master")
    parser.add_argument("--no-download-jars", action="store_true")
    args = parser.parse_args()
    config = StreamConfig(
        args.run_id, output_root=(Path(args.output_root) / "runs").as_posix(),
        source=args.source, input_path=args.input_path, bootstrap_servers=args.bootstrap_servers,
        max_offsets_per_trigger=args.max_offsets_per_trigger, feature_format="delta",
        trigger=args.trigger, processing_interval=args.processing_interval,
        filter_run_id=args.filter_run_id,
    )
    config.prepare()
    spark = create_spark(args.master, enable_delta=True, kafka=args.source == "kafka",
                         download_jars=not args.no_download_jars)
    spark.streams.addListener(ProgressRecorder(config.path("metrics")))
    queries = []
    result = {"run_id": args.run_id, "status": "running",
              "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        queries += start_ingest(spark, config)
        queries += start_features(spark, config)

        def lake_and_online(batch, batch_id):
            report = {"batch_id": batch_id, "started_at": datetime.now(timezone.utc).isoformat()}
            try:
                report["input_rows"] = batch.count()
                if report["input_rows"]:
                    # Source clean is already a durable boundary. Rebuilding
                    # deterministic snapshots and SET projections converges on
                    # retry, even if failure occurs between these external stores.
                    report["lake"] = build_lake(spark, config.root.as_posix())
                    report["online"] = publish_features(
                        spark, config.root.as_posix(), args.run_id, args.mongo_uri, args.redis_url)
                report["status"] = "completed"
            except Exception as error:
                report.update(status="failed", error=f"{type(error).__name__}: {error}")
                raise
            finally:
                report["finished_at"] = datetime.now(timezone.utc).isoformat()
                _save_json(config.root, f"lake-online-batch-{batch_id:06d}.json", report)

        sink = (read_clean(spark, config).writeStream.foreachBatch(lake_and_online)
                        .outputMode("append").queryName(f"{args.run_id}_lake_online")
                        .option("checkpointLocation", config.checkpoint("lake_online")))
        sink = sink.trigger(availableNow=True) if args.trigger == "available-now" else sink.trigger(processingTime=args.processing_interval)
        queries.append(sink.start())
        await_queries(queries, args.duration_seconds)
        result["status"] = "completed"
        result["termination"] = "normal_or_requested_duration; inspect source offsets for backlog"
    except Exception as error:
        result.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        _save_json(config.root, "live_pipeline.json", result)
        spark.stop()


if __name__ == "__main__":
    main()
