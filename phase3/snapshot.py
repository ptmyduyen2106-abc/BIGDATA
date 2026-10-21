"""Explicit, reproducible batch snapshot for a stopped finite replay.

This includes open session/window tails without injecting fake heartbeat events.
The snapshot is provisional if ingestion will resume. Streaming outputs retain
their actual watermark-finalization semantics.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json

from pyspark.sql import functions as F

from phase3.schema import CLEAN_SCHEMA, RAW_COLUMNS
from phase3.streaming import StreamConfig, create_spark
from phase3.transforms import session_features, window_features


def snapshot(spark, config: StreamConfig) -> dict:
    clean = (spark.read.schema(CLEAN_SCHEMA).parquet(config.path("clean"))
             .dropDuplicates(["event_id"]).cache())
    try:
        clean_count = clean.count()
        if clean_count == 0:
            raise ValueError("No clean events. Run ingestion successfully before snapshot.")
        # Collect epoch milliseconds, not TimestampType: Python's conversion of
        # TimestampType is a naive host-local datetime even with Spark in UTC.
        max_time_ms = clean.agg(F.max("timestamp").alias("value")).first()["value"]
        max_time_utc = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=max_time_ms)
        watermark = config.watermark
        windows = window_features(clean, watermark, config.window_duration, config.window_slide)
        sessions = session_features(clean, watermark, config.session_gap)
        counts = {}
        for name, frame in (("windows", windows), ("sessions", sessions)):
            frame = (frame.withColumn("snapshot_max_event_time", F.expr(f"timestamp_millis({max_time_ms})"))
                     .withColumn("is_finite_snapshot", F.lit(True)))
            frame.write.format(config.feature_format).mode("overwrite").save(
                config.path(f"snapshots/{name}"))
            counts[name] = frame.count()
        # Phase 4 spark_batch.py expects exactly this original RetailRocket CSV.
        # Folder contains part-*.csv files and a _SUCCESS marker, not a single file.
        (clean.select(*RAW_COLUMNS).orderBy("timestamp", "visitorid", "itemid", "event")
         .coalesce(1).write.option("header", "true").mode("overwrite")
         .csv(config.path("phase4_raw_csv")))
        summary = {
            "run_id": config.run_id, "clean_event_count": clean_count,
            "window_rows": counts["windows"], "session_rows": counts["sessions"],
            "max_event_time_utc": max_time_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "format": config.feature_format,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "semantics": "Batch snapshot of observed clean events; open tails included. "
                         "Provisional if ingestion resumes; not a stream-finalization claim.",
            "phase4_csv_columns": list(RAW_COLUMNS),
            "review_caveat": "Original Phase 4 silver filters out review; extend it explicitly "
                             "if synthetic review events should be retained downstream.",
        }
        (config.root / "metrics" / "snapshot-summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
    finally:
        clean.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default="outputs/runs")
    parser.add_argument("--format", dest="feature_format", choices=["delta", "parquet"], default="delta")
    parser.add_argument("--window-duration", default="5 minutes")
    parser.add_argument("--window-slide", default="1 minute")
    parser.add_argument("--session-gap", default="30 minutes")
    parser.add_argument("--master")
    parser.add_argument("--download-jars", action="store_true", default=True)
    parser.add_argument("--no-download-jars", dest="download_jars", action="store_false")
    args = parser.parse_args()
    config = StreamConfig(**{
        name: value for name, value in vars(args).items()
        if name in StreamConfig.__dataclass_fields__
    })
    config.prepare()
    spark = create_spark(args.master, config.feature_format == "delta",
                         download_jars=args.download_jars)
    try:
        print(json.dumps(snapshot(spark, config), ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
