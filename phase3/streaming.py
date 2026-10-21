"""Kafka -> validated/deduplicated Parquet -> separate event-time feature jobs.

Run ``python -m phase3.streaming --help``. Every sink has its own checkpoint.
Never chain deduplication and aggregation in one Spark 3.5 streaming query.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from phase3.schema import CLEAN_SCHEMA, TOPICS
from phase3.transforms import (
    deduplicate, parse_events, quarantined_events, session_features,
    valid_events, window_features,
)


@dataclass
class StreamConfig:
    run_id: str
    output_root: str = "outputs/runs"
    source: str = "kafka"
    input_path: str | None = None
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 10000
    max_files_per_trigger: int = 1
    watermark: str = "10 minutes"
    window_duration: str = "5 minutes"
    window_slide: str = "1 minute"
    session_gap: str = "30 minutes"
    feature_format: str = "delta"
    trigger: str = "available-now"
    processing_interval: str = "5 seconds"
    filter_run_id: bool = False

    @property
    def root(self) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", self.run_id):
            raise ValueError("run_id must start with a letter/digit and use letters/digits/_.-")
        return Path(self.output_root) / self.run_id

    def path(self, name: str) -> str:
        return (self.root / name).as_posix()

    def checkpoint(self, name: str) -> str:
        return self.path(f"checkpoints/{name}")

    def prepare(self) -> None:
        if self.source == "files" and not self.input_path:
            raise ValueError("--input-path is required for --source files")
        for name in ("clean", "quarantine", "checkpoints", "metrics"):
            (self.root / name).mkdir(parents=True, exist_ok=True)


def create_spark(master: str | None = None, enable_delta: bool = False,
                 kafka: bool = False, download_jars: bool = False) -> SparkSession:
    builder = (SparkSession.builder.appName("BigData-Phase3-Streaming")
               .config("spark.hadoop.fs.defaultFS", "file:///")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.streaming.noDataMicroBatches.enabled", "true"))
    if master:
        builder = builder.master(master)
    if enable_delta:
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    if download_jars:
        connectors = (["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"]
                      if kafka else [])
        if enable_delta:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder, extra_packages=connectors)
        elif connectors:
            builder = builder.config("spark.jars.packages", ",".join(connectors))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_source(spark: SparkSession, config: StreamConfig) -> DataFrame:
    if config.source == "kafka":
        source = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", config.bootstrap_servers)
                  .option("subscribe", ",".join(TOPICS))
                  .option("startingOffsets", config.starting_offsets)
                  .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
                  .option("failOnDataLoss", "true").load())
        return source.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.lit(None).cast("string").alias("source_file"),
        )
    if config.source == "files":
        return (spark.readStream.format("text")
                .option("maxFilesPerTrigger", config.max_files_per_trigger)
                .load(config.input_path).select(
                    F.col("value").alias("raw_value"),
                    F.lit(None).cast("string").alias("kafka_topic"),
                    F.lit(None).cast("int").alias("kafka_partition"),
                    F.lit(None).cast("long").alias("kafka_offset"),
                    F.lit(None).cast("timestamp").alias("kafka_timestamp"),
                    F.input_file_name().alias("source_file"),
                ))
    raise ValueError(f"Unsupported source: {config.source}")


def read_clean(spark: SparkSession, config: StreamConfig) -> DataFrame:
    # Read all currently committed part files together. Throttling this source by
    # one file can split one ingest micro-batch across triggers; a random part
    # containing newer timestamps could advance the feature watermark before
    # equally valid records in another part of that same batch are read.
    return spark.readStream.schema(CLEAN_SCHEMA).parquet(config.path("clean"))


def _trigger(writer: Any, config: StreamConfig) -> Any:
    if config.trigger == "available-now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=config.processing_interval)


def start_ingest(spark: SparkSession, config: StreamConfig) -> list[StreamingQuery]:
    config.prepare()
    parsed = parse_events(read_source(spark, config),
                          config.run_id if config.filter_run_id else None)
    clean = deduplicate(valid_events(parsed), config.watermark)
    queries = []
    try:
        for name, frame in (("quarantine", quarantined_events(parsed)), ("clean", clean)):
            writer = (frame.writeStream.format("parquet").outputMode("append")
                      .queryName(f"{config.run_id}_{name}")
                      .option("checkpointLocation", config.checkpoint(name))
                      .option("path", config.path(name)))
            queries.append(_trigger(writer, config).start())
    except Exception:
        for query in queries:
            query.stop()
        raise
    return queries


def delta_upsert(path: str, keys: list[str]):
    """Idempotent SET aggregates keyed by business key, not additive increments."""
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable
        batch.persist()
        try:
            # Consume the complete stateful output even on retries and empty batches.
            if batch.count() == 0:
                return
            if DeltaTable.isDeltaTable(batch.sparkSession, path):
                predicate = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
                (DeltaTable.forPath(batch.sparkSession, path).alias("t")
                 .merge(batch.alias("s"), predicate)
                 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            else:
                batch.write.format("delta").mode("errorifexists").save(path)
        finally:
            batch.unpersist()
    return write_batch


def start_features(spark: SparkSession, config: StreamConfig,
                   stages: tuple[str, ...] = ("windows", "sessions")) -> list[StreamingQuery]:
    config.prepare()
    queries = []
    try:
        for stage in stages:
            clean = read_clean(spark, config)
            if stage == "windows":
                features = window_features(clean, config.watermark,
                                           config.window_duration, config.window_slide)
                keys = ["visitorid", "window_start", "window_end"]
            elif stage == "sessions":
                features = session_features(clean, config.watermark, config.session_gap)
                keys = ["visitorid", "session_start", "session_end"]
            else:
                raise ValueError(f"Unknown feature stage: {stage}")
            writer = (features.writeStream.queryName(f"{config.run_id}_{stage}")
                      .option("checkpointLocation", config.checkpoint(stage)))
            if config.feature_format == "delta":
                # Sessions can merge when late data bridges two sessions, so Spark
                # supports only append after the event-time watermark finalizes them.
                mode = "update" if stage == "windows" else "append"
                writer = writer.outputMode(mode).foreachBatch(
                    delta_upsert(config.path(stage), keys))
            else:
                writer = (writer.outputMode("append").format("parquet")
                          .option("path", config.path(stage)))
            queries.append(_trigger(writer, config).start())
    except Exception:
        for query in queries:
            query.stop()
        raise
    return queries


class ProgressRecorder(StreamingQueryListener):
    """Actual Spark batch progress, not synthetic success/evidence counters."""
    def __init__(self, directory: str):
        super().__init__()
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.names: dict[str, str] = {}

    def onQueryStarted(self, event) -> None:
        self.names[str(event.id)] = event.name or str(event.id)

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        name = progress.get("name") or self.names.get(str(progress["id"]), "query")
        # The batch id in the filename makes repeated recording harmless.
        target = self.directory / f"{name}-batch-{progress['batchId']:06d}.json"
        target.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")

    def onQueryTerminated(self, event) -> None:
        target = self.directory / f"terminated-{event.id}-{event.runId}.json"
        target.write_text(json.dumps({
            "query_id": str(event.id), "query_run_id": str(event.runId),
            "exception": event.exception,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }, ensure_ascii=False, indent=2), encoding="utf-8")

    def onQueryIdle(self, event) -> None:
        pass


def await_queries(queries: list[StreamingQuery], duration_seconds: float = 0) -> None:
    deadline = time.monotonic() + duration_seconds if duration_seconds > 0 else None
    try:
        while any(query.isActive for query in queries):
            for query in queries:
                error = query.exception()
                if error is not None:
                    raise error
            if deadline is not None and time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        # A failed query may have terminated before entering the polling loop.
        for query in queries:
            error = query.exception()
            if error is not None:
                raise error
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def parser() -> argparse.ArgumentParser:
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--stage", choices=["ingest", "windows", "sessions", "features", "all"], default="all")
    args.add_argument("--run-id", required=True)
    args.add_argument("--output-root", default="outputs/runs")
    args.add_argument("--source", choices=["kafka", "files"], default="kafka")
    args.add_argument("--input-path")
    args.add_argument("--bootstrap-servers",
                      default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    args.add_argument("--starting-offsets", default="earliest")
    args.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    args.add_argument("--max-files-per-trigger", type=int, default=1)
    args.add_argument("--watermark", default="10 minutes")
    args.add_argument("--window-duration", default="5 minutes")
    args.add_argument("--window-slide", default="1 minute")
    args.add_argument("--session-gap", default="30 minutes")
    args.add_argument("--format", dest="feature_format", choices=["delta", "parquet"], default="delta")
    args.add_argument("--trigger", choices=["available-now", "processing"], default="available-now")
    args.add_argument("--processing-interval", default="5 seconds")
    args.add_argument("--duration-seconds", type=float, default=0,
                      help="0 waits indefinitely for a processing trigger")
    args.add_argument("--filter-run-id", action="store_true")
    args.add_argument("--master", help="e.g. local[2], omit for spark-submit master")
    args.add_argument("--download-jars", action="store_true", default=True,
                      help="resolve pinned Kafka/Delta Maven JARs (default)")
    args.add_argument("--no-download-jars", dest="download_jars", action="store_false",
                      help="use preinstalled connector JARs without accessing Maven")
    return args


def main() -> None:
    args = parser().parse_args()
    fields = StreamConfig.__dataclass_fields__
    config = StreamConfig(**{key: value for key, value in vars(args).items() if key in fields})
    config.prepare()
    spark = create_spark(args.master, config.feature_format == "delta",
                         config.source == "kafka", args.download_jars)
    recorder = ProgressRecorder(config.path("metrics"))
    spark.streams.addListener(recorder)
    queries: list[StreamingQuery] = []
    result = {"run_id": config.run_id, "stage": args.stage, "status": "running",
              "trigger": config.trigger, "feature_format": config.feature_format,
              "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        if args.stage in {"ingest", "all"}:
            queries = start_ingest(spark, config)
            if config.trigger == "available-now" or args.stage == "ingest":
                await_queries(queries, args.duration_seconds)
                queries = []
        stages = (("windows", "sessions") if args.stage in {"all", "features"}
                  else (args.stage,) if args.stage in {"windows", "sessions"} else ())
        if stages:
            queries += start_features(spark, config, stages)
        if queries:
            await_queries(queries, args.duration_seconds)
        result["status"] = "completed"
        result["tail_semantics"] = (
            "Event-time watermark advances only with input. Session tail may remain in state. "
            "Use phase3.snapshot for an explicit finite-input batch snapshot."
        )
    except Exception as error:
        result.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        (config.root / "metrics" / f"{args.stage}-result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
