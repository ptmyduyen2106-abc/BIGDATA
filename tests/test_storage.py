"""Storage/projection contract tests. Delta integration is opt-in in its runtime."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from pipeline.online import OnlinePublisher, project_user
from pipeline.storage import utc_millis


BASE = 1704110400123


def fixture_event(identifier, minute=0, raw="view", visitor=1, item=10):
    return {"event_id": identifier, "timestamp": BASE + minute * 60000,
            "visitorid": visitor, "itemid": item, "event": raw, "run_id": "source-fixture",
            "event_type": {"view": "page_view", "addtocart": "add_to_cart",
                           "transaction": "purchase", "review": "review"}[raw]}


def _matches(document, selector):
    for key, expected in selector.items():
        if isinstance(expected, dict) and "$nin" in expected:
            if document.get(key) in expected["$nin"]:
                return False
        elif document.get(key) != expected:
            return False
    return True


class FakeCollection:
    def __init__(self):
        self.documents = []
        self.indexes = []

    def create_index(self, keys, **options):
        self.indexes.append((keys, options))

    def update_one(self, selector, changes, upsert=False):
        found = next((document for document in self.documents if _matches(document, selector)), None)
        if found is None:
            if not upsert:
                return
            found = dict(selector)
            found.update(copy.deepcopy(changes.get("$setOnInsert", {})))
            self.documents.append(found)
        found.update(copy.deepcopy(changes.get("$set", {})))

    def find(self, selector, projection=None):
        return [copy.deepcopy(document) for document in self.documents if _matches(document, selector)]

    def delete_many(self, selector):
        self.documents = [document for document in self.documents if not _matches(document, selector)]

    def count_documents(self, selector):
        return len(self.find(selector))


class FakeDatabase(dict):
    def __missing__(self, key):
        self[key] = FakeCollection()
        return self[key]


class FakeMongo(dict):
    def __missing__(self, key):
        self[key] = FakeDatabase()
        return self[key]


class FakeRedis:
    def __init__(self):
        self.values, self.sets = {}, {}

    def pipeline(self, transaction=True):
        assert transaction
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, target):
        self.target, self.commands = target, []

    def delete(self, key):
        self.commands.append(("delete", key))
        return self

    def set(self, key, value):
        self.commands.append(("set", key, value))
        return self

    def sadd(self, key, *values):
        self.commands.append(("sadd", key, values))
        return self

    def execute(self):
        for command in self.commands:
            kind, key, *arguments = command
            if kind == "delete":
                self.target.values.pop(key, None)
                self.target.sets.pop(key, None)
            elif kind == "set":
                self.target.values[key] = arguments[0]
            else:
                self.target.sets.setdefault(key, set()).update(arguments[0])
        self.commands.clear()


class ProjectionTests(unittest.TestCase):
    def test_dedup_order_window_and_late_session_bridge(self):
        first = fixture_event("a")
        later = fixture_event("b", 31, "addtocart", item=11)
        bridge = fixture_event("c", 29, "transaction", item=11)
        self.assertEqual(len(project_user([first, later])["sessions"]), 2)
        projection = project_user([later, bridge, first, bridge])
        self.assertEqual(projection, project_user([first, bridge, later]))
        self.assertEqual(projection["event_count"], 3)
        self.assertEqual(projection["interaction_score"], 9)
        self.assertEqual(projection["seen_items"], [10, 11])
        self.assertEqual(len(projection["sessions"]), 1)
        self.assertEqual(projection["current_session"]["active_duration_seconds"], 31 * 60)
        self.assertTrue(projection["current_session"]["provisional"])
        self.assertEqual(projection["latest_window"]["event_count"], 2)
        self.assertEqual(projection["latest_window"]["interaction_score"], 8)
        self.assertIsNone(projection["latest_window"]["purchase_per_view"])
        with self.assertRaises(ValueError):
            project_user([first, fixture_event("other", visitor=2)])
        with self.assertRaises(ValueError):
            project_user([])

    def test_retry_replaces_projections_removes_stale_sessions_and_isolates_runs(self):
        mongo, cache = FakeMongo(), FakeRedis()
        publisher = OnlinePublisher(mongo, cache, "runA")
        first, later = fixture_event("a"), fixture_event("b", 31, "addtocart", item=11)
        bridge = fixture_event("c", 29, "transaction", item=12)
        report = publisher.consume([first, later])
        self.assertEqual(report["sessions"], 2)
        publisher.consume([bridge, bridge])
        report = publisher.consume([later, first, bridge])  # Retry full source.
        self.assertEqual(report["durable_events"], 3)
        self.assertEqual(report["sessions"], 1)  # Old bridge-obsolete session removed.
        user = mongo["bigdata"]["user_features"].find({"run_id": "runA"})[0]
        self.assertEqual(user["event_count"], 3)
        self.assertEqual(user["interaction_score"], 9)
        self.assertEqual(cache.sets["bigdata:runA:seen:1"], {"10", "11", "12"})
        self.assertEqual(json.loads(cache.values["bigdata:runA:session:1"])["event_count"], 3)
        second = OnlinePublisher(mongo, cache, "runB")
        second.consume([first])
        self.assertEqual(publisher.events.count_documents({"run_id": "runA"}), 3)
        self.assertEqual(publisher.events.count_documents({"run_id": "runB"}), 1)
        self.assertEqual(cache.sets["bigdata:runB:seen:1"], {"10"})
        self.assertTrue(any(options.get("unique") for _, options in publisher.events.indexes))

    def test_utc_milliseconds_and_missing_metrics(self):
        self.assertEqual(utc_millis(BASE), "2024-01-01T12:00:00.123Z")
        self.assertIsNone(utc_millis(None))
        purchase_only = project_user([fixture_event("x", raw="transaction")])
        self.assertIsNone(purchase_only["purchase_per_view"])
        self.assertEqual(purchase_only["interaction_score"], 5)


@unittest.skipUnless(os.getenv("RUN_ONLINE_TESTS") == "1", "Set RUN_ONLINE_TESTS=1 with MongoDB/Redis running")
class OnlineIntegrationTests(unittest.TestCase):
    def test_real_mongo_redis_retry_session_bridge_and_seen_set(self):
        from pipeline.online import _connect
        run_id = "test-online-" + uuid4().hex
        publisher = _connect(run_id, os.getenv("MONGO_URI", os.getenv("MONGODB_URI", "mongodb://mongodb:27017")),
                             os.getenv("REDIS_URL", "redis://redis:6379/0"))
        try:
            first, later = fixture_event("a"), fixture_event("b", 31, "addtocart", item=11)
            bridge = fixture_event("c", 29, "transaction", item=12)
            self.assertEqual(publisher.consume([first, later])["sessions"], 2)
            self.assertEqual(publisher.consume([bridge, bridge])["sessions"], 1)
            report = publisher.consume([later, first, bridge])
            self.assertEqual(report["durable_events"], 3)
            self.assertEqual(publisher.redis.smembers(f"bigdata:{run_id}:seen:1"), {"10", "11", "12"})
            user = publisher.users.find_one({"run_id": run_id, "visitorid": 1})
            self.assertEqual(user["event_count"], 3)
            self.assertEqual(user["interaction_score"], 9)
            self.assertEqual(json.loads(publisher.redis.get(f"bigdata:{run_id}:session:1"))["event_count"], 3)
        finally:
            # Delete only this test's random namespace, never application data.
            for collection in (publisher.events, publisher.users, publisher.sessions, publisher.items):
                collection.delete_many({"run_id": run_id})
            keys = list(publisher.redis.scan_iter(match=f"bigdata:{run_id}:*"))
            if keys:
                publisher.redis.delete(*keys)
            publisher.mongo_client.close()
            publisher.redis.close()


@unittest.skipUnless(os.getenv("RUN_DELTA_TESTS") == "1", "Set RUN_DELTA_TESTS=1 in the Java/Delta runtime")
class DeltaIntegrationTests(unittest.TestCase):
    def test_build_retry_and_actual_acid_time_travel(self):
        from pyspark.sql import functions as F
        from phase2.contract import build_event
        from phase3.streaming import create_spark
        from phase3.transforms import parse_events, valid_events
        from pipeline.acid_check import verify_acid
        from pipeline.storage import build_lake
        spark = create_spark("local[2]", enable_delta=True, download_jars=True)
        try:
            with tempfile.TemporaryDirectory(prefix="delta-course-test-") as directory:
                root = Path(directory)
                records = [build_event({"timestamp": BASE + index * 1000, "visitorid": 1 if index < 3 else 2,
                                        "event": kind, "itemid": 10 if index < 3 else 20,
                                        "transactionid": 7 if kind == "transaction" else None}, "fixture")
                           for index, kind in enumerate(("view", "addtocart", "transaction", "review"))]
                payloads = [json.dumps(value) for value in records + [records[0]]]
                values = F.array(*map(F.lit, payloads))
                raw = spark.range(len(payloads)).select(
                    F.element_at(values, F.col("id").cast("int") + 1).alias("raw_value"))
                for name, dtype in (("kafka_topic", "string"), ("kafka_partition", "int"),
                                    ("kafka_offset", "long"), ("kafka_timestamp", "timestamp"),
                                    ("source_file", "string")):
                    raw = raw.withColumn(name, F.lit(None).cast(dtype))
                valid_events(parse_events(raw)).write.parquet((root / "clean").as_posix())
                first, second = build_lake(spark, directory), build_lake(spark, directory)
                self.assertEqual((first["silver_events"], second["bronze_events"]), (4, 4))
                gold = spark.read.format("delta").load(second["paths"]["gold_interactions"]).first()
                self.assertEqual((gold.interaction_score, gold["count"]), (9.0, 3))
                self.assertIsNone(second["ctr"])
                self.assertIsNone(second["revenue"])
                self.assertEqual(verify_acid(spark, directory)["status"], "passed")
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
