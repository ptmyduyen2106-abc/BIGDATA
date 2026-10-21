"""Real local Spark tests with a file source; no Kafka/Delta JARs are required.

Run: python -m unittest discover -s tests -p 'test_phase3*.py' -v
The Docker runtime is recommended on Windows (native Hadoop DLLs are not bundled).
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
BASE = 1704110400123  # 2024-01-01 12:00:00.123 UTC


def event(timestamp=BASE, visitorid=1, itemid=10, raw="view", transactionid=None):
    event_type = {"view": "page_view", "addtocart": "add_to_cart",
                  "transaction": "purchase", "review": "review"}[raw]
    canonical = json.dumps([timestamp, visitorid, raw, itemid, transactionid],
                           separators=(",", ":"))
    return {
        "schema_version": 1, "event_id": hashlib.sha256(canonical.encode()).hexdigest(),
        "run_id": "fixture", "timestamp": timestamp, "visitorid": visitorid,
        "itemid": itemid, "event": raw, "event_type": event_type,
        "transactionid": transactionid,
        "event_time": datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat(),
        "ingested_at": "2024-01-01T12:00:00Z", "synthetic": True,
    }


@unittest.skipUnless(HAS_PYSPARK, "PySpark not installed; run inside Spark Docker runtime")
class Phase3SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from phase3.streaming import create_spark
        cls.spark = create_spark("local[2]")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw_frame(self, payloads, topic=None):
        from pyspark.sql import functions as F
        # Construct source rows inside the JVM. Production sources are Kafka/file
        # readers, so these tests should not require an unrelated Python worker.
        values = F.array(*[F.lit(value) for value in payloads])
        return (self.spark.range(len(payloads)).select(
                    F.element_at(values, F.col("id").cast("int") + F.lit(1)).alias("raw_value"))
                .withColumn("kafka_topic", F.lit(topic).cast("string"))
                .withColumn("kafka_partition", F.lit(0).cast("int"))
                .withColumn("kafka_offset", F.lit(0).cast("long"))
                .withColumn("kafka_timestamp", F.lit(None).cast("timestamp"))
                .withColumn("source_file", F.lit(None).cast("string")))

    def epoch_columns(self, frame, *names):
        from pyspark.sql import functions as F
        # TimestampType collected into Python is a naive host-local datetime.
        # Compare JVM-computed epoch milliseconds instead of the host timezone.
        for name in names:
            frame = frame.withColumn(f"{name}_ms", F.expr(f"unix_millis(`{name}`)"))
        return frame

    def test_parse_validation_and_exact_epoch_milliseconds(self):
        from phase3.transforms import parse_events, valid_events
        valid = event()
        wrong_hash = dict(valid, event_id="a" * 64)
        wrong_type = dict(valid, timestamp="not-an-integer")
        missing_purchase_id = event(raw="transaction")
        bad_schema = dict(valid, schema_version=2)
        bad_ingestion_time = dict(valid, ingested_at="2024-01-01T12:00:00")
        bad_rating = dict(event(raw="review"), rating=5.5)
        rows = self.epoch_columns(parse_events(self.raw_frame([
            json.dumps(valid), "{", json.dumps(wrong_hash), json.dumps(wrong_type),
            json.dumps(missing_purchase_id), json.dumps(bad_schema),
            json.dumps(bad_ingestion_time), json.dumps(bad_rating),
        ])), "event_time").collect()
        self.assertIsNone(rows[0].validation_error)
        self.assertEqual(rows[0].event_time_ms, BASE)
        self.assertEqual(rows[1].validation_error, "malformed_json_or_type")
        self.assertEqual(rows[2].validation_error, "event_id_mismatch")
        self.assertIsNotNone(rows[3].validation_error)
        self.assertEqual(rows[4].validation_error, "purchase_without_transactionid")
        self.assertEqual(rows[5].validation_error, "unsupported_schema_version")
        self.assertEqual(rows[6].validation_error, "invalid_ingested_at")
        self.assertEqual(rows[7].validation_error, "invalid_rating")
        mismatch = parse_events(self.raw_frame([json.dumps(valid)], topic="purchase")).first()
        self.assertEqual(mismatch.validation_error, "topic_mismatch")
        self.assertEqual(valid_events(parse_events(self.raw_frame([json.dumps(valid)]))).count(), 1)

    def test_batch_window_session_features_and_boundaries(self):
        from phase3.transforms import parse_events, session_features, valid_events, window_features
        # Five real fixture records, not a fake clock event. Visitor 2 has a
        # purchase without a preceding view, so purchase/view is undefined.
        payloads = [
            event(),
            event(BASE + 60000, raw="addtocart", itemid=11),
            event(BASE + 30000, raw="transaction", itemid=11, transactionid=7),
            event(BASE + 10 * 60000, visitorid=2, itemid=20,
                  raw="transaction", transactionid=8),
            event(BASE + 40 * 60000 - 123, itemid=12),  # Exactly 12:40:00.000 UTC.
        ]
        clean = valid_events(parse_events(self.raw_frame([json.dumps(value) for value in payloads])))
        windows = self.epoch_columns(
            window_features(clean, duration="5 minutes", slide="1 minute"),
            "window_start", "window_end", "first_event_time").collect()
        early = next(row for row in windows if row.visitorid == 1
                     and row.window_start_ms == BASE - 123)
        self.assertEqual(early.event_count, 3)
        self.assertEqual((early.page_view_count, early.add_to_cart_count, early.purchase_count), (1, 1, 1))
        self.assertEqual(early.interaction_score, 9)
        self.assertEqual(early.purchase_per_view, 1.0)
        self.assertEqual(early.window_end_ms - early.window_start_ms, 5 * 60000)
        self.assertEqual(early.first_event_time_ms, BASE)
        self.assertTrue(all(row.purchase_per_view is None for row in windows if row.visitorid == 2))
        # Sliding intervals are [start,end): the exact 12:40 event must not be
        # counted in 12:35-12:40, but must appear in 12:40-12:45 and four overlaps.
        final_windows = [row for row in windows if row.visitorid == 1
                         and row.first_event_time_ms == BASE + 40 * 60000 - 123]
        self.assertEqual(len(final_windows), 5)
        self.assertEqual(sorted(row.window_start_ms for row in final_windows),
                         [BASE - 123 + minute * 60000 for minute in (36, 37, 38, 39, 40)])
        self.assertTrue(all(row.event_count == 1 for row in final_windows))

        sessions = self.epoch_columns(session_features(clean, gap="30 minutes"),
                                      "session_start", "session_end", "last_event_time").collect()
        visitor1_sessions = sorted((row for row in sessions if row.visitorid == 1),
                                   key=lambda row: row.session_start_ms)
        self.assertEqual(len(visitor1_sessions), 2)
        first, second = visitor1_sessions
        self.assertEqual(first.event_count, 3)
        self.assertEqual(first.interaction_score, 9)
        self.assertEqual(first.active_duration_seconds, 60.0)
        self.assertEqual(first.session_end_ms - first.last_event_time_ms, 30 * 60000)
        self.assertGreater(second.session_start_ms - first.last_event_time_ms, 30 * 60000)
        self.assertNotEqual(first.session_id, second.session_id)
        visitor2_session = next(row for row in sessions if row.visitorid == 2)
        self.assertEqual(visitor2_session.event_count, 1)
        self.assertEqual(visitor2_session.interaction_score, 5)
        self.assertIsNone(visitor2_session.purchase_per_view)

    def test_streaming_dedup_lateness_features_and_checkpoint_restart(self):
        from phase3.schema import CLEAN_SCHEMA, RAW_COLUMNS
        from phase3.snapshot import snapshot
        from phase3.streaming import StreamConfig, start_features, start_ingest
        with tempfile.TemporaryDirectory(prefix="phase3-") as temporary:
            root = Path(temporary)
            inputs = root / "input"
            inputs.mkdir()
            config = StreamConfig(
                "fixture", output_root=(root / "outputs").as_posix(),
                source="files", input_path=inputs.as_posix(), feature_format="parquet",
                trigger="processing", processing_interval="200 milliseconds",
                window_slide="5 minutes", max_files_per_trigger=1,
            )
            batch_number = 0

            def push(payloads):
                nonlocal batch_number
                batch_number += 1
                temporary_file = root / "pending.jsonl"
                temporary_file.write_text("\n".join(
                    value if isinstance(value, str) else json.dumps(value)
                    for value in payloads) + "\n", encoding="utf-8")
                os.replace(temporary_file, inputs / f"batch-{batch_number:03d}.jsonl")

            def drain(queries):
                for query in queries:
                    query.processAllAvailable()

            view = event()
            cart = event(BASE + 60000, raw="addtocart", itemid=11)
            purchase = event(BASE + 30000, raw="transaction", itemid=11, transactionid=7)
            future = event(BASE + 80 * 60000, visitorid=2, itemid=20)
            future2 = event(BASE + 81 * 60000, visitorid=2, itemid=21)
            too_late = event(BASE + 90000, itemid=12)
            queries = start_ingest(self.spark, config)
            try:
                push([view, view, cart, "{"])
                drain(queries)
                push([purchase, cart])  # Out of order but within the watermark.
                drain(queries)
                push([future])
                drain(queries)
                push([too_late])  # Distinct ID, but now behind event-time watermark.
                drain(queries)
                progress = [entry for query in queries for entry in query.recentProgress]
                dropped = sum(operator.get("numRowsDroppedByWatermark", 0)
                              for entry in progress for operator in entry.get("stateOperators", []))
                self.assertGreaterEqual(dropped, 1)
            finally:
                for query in queries:
                    query.stop()

            clean = self.spark.read.schema(CLEAN_SCHEMA).parquet(config.path("clean"))
            self.assertEqual(clean.count(), 4)
            self.assertEqual(clean.select("event_id").distinct().count(), 4)
            quarantined = self.spark.read.parquet(config.path("quarantine"))
            self.assertEqual(quarantined.count(), 1)
            self.assertIn("raw_value", quarantined.columns)

            # Resume both source offsets and dedup state from the same checkpoints.
            queries = start_ingest(self.spark, config)
            try:
                push([cart, future2])
                drain(queries)
            finally:
                for query in queries:
                    query.stop()
            clean = self.spark.read.schema(CLEAN_SCHEMA).parquet(config.path("clean"))
            self.assertEqual(clean.count(), 5)
            self.assertEqual(clean.filter(f"event_id = '{cart['event_id']}'").count(), 1)

            feature_queries = start_features(self.spark, config)
            try:
                drain(feature_queries)
            finally:
                for query in feature_queries:
                    query.stop()
            windows = self.spark.read.parquet(config.path("windows"))
            early_window = windows.filter("visitorid = 1").collect()
            self.assertEqual(len(early_window), 1)
            self.assertEqual(early_window[0].event_count, 3)
            self.assertEqual(early_window[0].page_view_count, 1)
            self.assertEqual(early_window[0].add_to_cart_count, 1)
            self.assertEqual(early_window[0].purchase_count, 1)
            self.assertEqual(early_window[0].interaction_score, 9)
            sessions = self.spark.read.parquet(config.path("sessions"))
            early_session = self.epoch_columns(sessions.filter("visitorid = 1"),
                                               "session_start", "session_end").first()
            self.assertEqual(early_session.event_count, 3)
            self.assertEqual(early_session.active_duration_seconds, 60.0)
            self.assertEqual(early_session.session_start_ms, BASE)
            self.assertEqual(early_session.session_end_ms, BASE + 31 * 60000)
            # Visitor 2's most recent session cannot be finalized without later data.
            self.assertEqual(sessions.filter("visitorid = 2").count(), 0)

            # Restart feature queries with no new data: file sink must not duplicate rows.
            before_window_count, before_session_count = windows.count(), sessions.count()
            feature_queries = start_features(self.spark, config)
            try:
                drain(feature_queries)
            finally:
                for query in feature_queries:
                    query.stop()
            self.assertEqual(self.spark.read.parquet(config.path("windows")).count(), before_window_count)
            self.assertEqual(self.spark.read.parquet(config.path("sessions")).count(), before_session_count)

            summary = snapshot(self.spark, config)
            self.assertEqual(summary["clean_event_count"], 5)
            self.assertEqual(summary["session_rows"], 2)
            self.assertEqual(summary["window_rows"], 2)
            raw_csv = (self.spark.read.option("header", "true")
                       .csv(config.path("phase4_raw_csv")))
            self.assertEqual(raw_csv.columns, list(RAW_COLUMNS))
            self.assertEqual(raw_csv.count(), 5)


if __name__ == "__main__":
    unittest.main()
