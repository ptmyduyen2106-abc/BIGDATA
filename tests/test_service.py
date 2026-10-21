"""Serving regression tests using real FastAPI routing and temporary artifacts.

Redis/Mongo are optional; mocked transports exercise version and history rules
without claiming an external service was started.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from fastapi.testclient import TestClient

from service.app import create_app
from service.backends import Backends
from service.store import Settings

ROOT = Path(__file__).resolve().parents[1]


def bundle(run="run-a", version="als-v1"):
    return {"run_id": run, "model_version": version,
            "trained_at_utc": "2026-10-21T08:00:00+00:00", "algorithm": "als_implicit",
            "recommendations": {"7": [{"itemid": 2, "score": 0.9}, {"itemid": 3, "score": 0.8},
                                      {"itemid": 4, "score": 0.7}]},
            "seen_items": {"7": [1, 2]},
            "popular_items": [{"itemid": 1, "score": 10}, {"itemid": 2, "score": 9},
                              {"itemid": 5, "score": 8}, {"itemid": 6, "score": 7}],
            "metrics": {"heldout_test": {"precision_at_k": 0.12, "recall_at_k": 0.2,
                                        "ndcg_at_k": 0.18}}}


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.sets = {}
        self.fail = False
        self.ttls = []

    def check(self):
        if self.fail:
            raise ConnectionError("fixture Redis unavailable")

    def get(self, key):
        self.check()
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.check()
        self.ttls.append(ttl)
        self.values[key] = value

    def smembers(self, key):
        self.check()
        return self.sets.get(key, set())

    def ping(self):
        self.check()
        return True

    def close(self):
        pass


class ServiceTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / "outputs" / "test-tmp"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch, prefix="service-")
        self.outputs = Path(self.temporary.name)
        self.settings = Settings(output_root=self.outputs)
        self.online = Backends(self.settings)
        self.application = create_app(self.settings, self.online)
        self.client = TestClient(self.application)

    def tearDown(self):
        self.client.close()
        self.online.close()
        self.temporary.cleanup()

    def write(self, data, select=True):
        serving = self.outputs / "runs" / data["run_id"] / "serving"
        serving.mkdir(parents=True, exist_ok=True)
        target = serving / "model_bundle.json"
        temporary = serving / "next.json"
        temporary.write_text(json.dumps(data), encoding="utf-8")
        os.replace(temporary, target)
        if select:
            (self.outputs / "latest_run.json").write_text(json.dumps({"run_id": data["run_id"]}), encoding="utf-8")
        return target

    def enable_redis(self):
        settings = Settings(output_root=self.outputs, redis_url="redis://fixture-only")
        self.online.settings = settings
        self.redis = FakeRedis()
        self.online.redis = self.redis
        return self.redis

    def test_not_ready_until_usable_bundle_and_liveness_independent(self):
        self.assertEqual(self.client.get("/health/live").status_code, 200)
        result = self.client.get("/health/ready")
        self.assertEqual(result.status_code, 503)
        self.assertFalse(result.json()["model"]["ready"])
        self.assertEqual(self.client.get("/api/recommendations/7").status_code, 503)
        self.write(bundle())
        self.assertEqual(self.client.get("/health/ready").status_code, 200)

    def test_personalized_seen_filter_and_popularity_fill(self):
        self.write(bundle())
        result = self.client.get("/api/recommendations/7?k=3").json()
        self.assertEqual([row["itemid"] for row in result["items"]], [3, 4, 5])
        self.assertEqual(result["source"], "als_bundle")
        self.assertEqual(result["seen_items_excluded"], 2)
        self.assertFalse(result["cache_hit"])
        self.assertGreaterEqual(result["request_ms"], 0)

    def test_unknown_user_uses_explicit_popular_fallback(self):
        self.write(bundle())
        result = self.client.get("/api/recommendations/0?k=2").json()
        self.assertEqual(result["source"], "popular_fallback")
        self.assertEqual([row["itemid"] for row in result["items"]], [1, 2])
        self.assertEqual(result["run_id"], "run-a")

    def test_strict_id_and_k_bounds(self):
        self.write(bundle())
        for path in ("/api/recommendations/-1", "/api/recommendations/2147483648",
                     "/api/recommendations/7.0", "/api/recommendations/true",
                     "/api/recommendations/7?k=0", "/api/recommendations/7?k=101",
                     "/api/recommendations/7?k=1.0", "/api/users?limit=0", "/api/users?limit=201"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 422)
        self.assertEqual(self.client.get("/api/recommendations/2147483647?k=100").status_code, 200)

    def test_invalid_schema_rejected_without_fake_ready(self):
        for changes in ({"algorithm": "fake"}, {"popular_items": []},
                        {"trained_at_utc": "not-a-date"}, {"model_version": ""},
                        {"recommendations": {"7": [{"itemid": -1, "score": 1}]}},
                        {"recommendations": {"7": [{"itemid": 4, "score": float("nan")}]}},
                        {"seen_items": {}}):
            with self.subTest(changes=changes):
                # Each test starts with an unactivated store, so an invalid first
                # model cannot borrow a previously valid same-run bundle.
                client = TestClient(create_app(self.settings))
                value = bundle()
                value.update(changes)
                self.write(value)
                self.assertEqual(client.get("/health/ready").status_code, 503)
                client.close()

    def test_same_run_failed_reload_retains_traceable_last_valid(self):
        target = self.write(bundle())
        self.assertEqual(self.client.get("/health/ready").status_code, 200)
        target.write_text("{unfinished", encoding="utf-8")
        response = self.client.get("/api/recommendations/7").json()
        self.assertTrue(response["model_stale"])
        self.assertEqual(response["model_version"], "als-v1")
        self.assertIsNotNone(response["reload_error"])
        self.assertTrue(self.client.get("/health/ready").json()["model"]["stale"])

    def test_pointer_switch_never_leaks_previous_run(self):
        self.write(bundle())
        self.assertEqual(self.client.get("/api/recommendations/7").status_code, 200)
        (self.outputs / "latest_run.json").write_text('{"run_id":"run-b"}', encoding="utf-8")
        result = self.client.get("/health/ready")
        self.assertEqual(result.status_code, 503)
        self.assertEqual(result.json()["model"]["run_id"], "run-b")
        self.assertEqual(self.client.get("/api/users").status_code, 503)
        self.write(bundle("run-b", "als-v2"))
        self.assertEqual(self.client.get("/api/recommendations/7").json()["run_id"], "run-b")

    def test_atomic_version_reload_ignores_old_cache(self):
        redis = self.enable_redis()
        self.write(bundle())
        first = self.client.get("/api/recommendations/7?k=2").json()
        self.assertFalse(first["cache_hit"])
        self.assertTrue(self.client.get("/api/recommendations/7?k=2").json()["cache_hit"])
        changed = bundle(version="als-v2-longer")
        changed["recommendations"]["7"] = [{"itemid": 99, "score": 0.95}]
        self.write(changed)
        result = self.client.get("/api/recommendations/7?k=2").json()
        self.assertEqual(result["model_version"], "als-v2-longer")
        self.assertEqual(result["items"][0]["itemid"], 99)
        self.assertFalse(result["cache_hit"])
        self.assertEqual(redis.ttls[-1], 60)

    def test_cache_hit_rechecks_live_seen_history(self):
        redis = self.enable_redis()
        self.write(bundle())
        self.client.get("/api/recommendations/7?k=2")
        redis.sets["bigdata:run-a:seen:7"] = {"3"}
        result = self.client.get("/api/recommendations/7?k=2").json()
        self.assertTrue(result["cache_hit"])
        self.assertEqual([row["itemid"] for row in result["items"]], [4, 5])
        self.assertIn("redis", result["history_sources"])
        redis.sets["bigdata:other-run:seen:7"] = {"5"}
        self.assertIn(5, [row["itemid"] for row in self.client.get("/api/recommendations/7?k=2").json()["items"]])

    def test_retrained_model_filters_canonical_data_history_even_on_cache_hit(self):
        redis = self.enable_redis()
        self.write(bundle("model-retrain", "als-retrained"))
        directory = self.outputs / "runs" / "model-retrain" / "metrics"
        directory.mkdir()
        (directory / "run_report.json").write_text(json.dumps({
            "run_id": "model-retrain", "source_run_id": "run-ingest",
        }), encoding="utf-8")
        initial = self.client.get("/api/recommendations/7?k=2").json()
        self.assertEqual([row["itemid"] for row in initial["items"]], [3, 4])
        # The writer keeps appending to the original ingestion namespace after
        # Airflow activates a new model. Both scoped histories must be applied.
        redis.sets["bigdata:run-ingest:seen:7"] = {"3"}
        redis.sets["bigdata:model-retrain:seen:7"] = {"4"}
        redis.sets["bigdata:unrelated-run:seen:7"] = {"5", "6"}
        result = self.client.get("/api/recommendations/7?k=2").json()
        self.assertTrue(result["cache_hit"])
        self.assertEqual([row["itemid"] for row in result["items"]], [5, 6])
        self.assertEqual(result["run_id"], "model-retrain")
        self.assertEqual(result["data_run_id"], "run-ingest")
        self.assertEqual(result["model_version"], "als-retrained")
        self.assertEqual(result["history_runs"], ["model-retrain", "run-ingest"])
        self.assertEqual(result["history_sources"].count("redis"), 1)
        self.assertIn(Backends.cache_key("model-retrain", "als-retrained", 7, 2), redis.values)
        self.assertNotIn(Backends.cache_key("run-ingest", "als-retrained", 7, 2), redis.values)
        self.assertEqual(self.client.get("/api/metrics").json()["data_run_id"], "run-ingest")
        self.assertEqual(self.client.get("/health/ready").json()["model"]["data_run_id"], "run-ingest")
        redis.fail = True
        degraded = self.client.get("/api/recommendations/7?k=2").json()
        self.assertEqual(len(degraded["warnings"]), 1)

    def test_invalid_lineage_fails_closed_without_unrelated_history(self):
        self.write(bundle())
        self.assertEqual(self.client.get("/api/recommendations/7").status_code, 200)
        directory = self.outputs / "runs" / "run-a" / "metrics"
        directory.mkdir()
        path = directory / "run_report.json"
        for contents in (json.dumps({"run_id": "run-a", "source_run_id": "../../escape"}),
                         json.dumps({"run_id": "run-a", "source_run_id": "run-a"}), "{unfinished"):
            with self.subTest(contents=contents):
                path.write_text(contents, encoding="utf-8")
                result = self.client.get("/api/recommendations/7")
                self.assertEqual(result.status_code, 503)
                self.assertIn("lineage", result.json()["detail"]["message"].lower())
                self.assertEqual(self.client.get("/api/metrics").status_code, 503)
                self.assertEqual(self.client.get("/health/ready").status_code, 503)

    def test_cache_payload_version_mismatch_is_ignored(self):
        redis = self.enable_redis()
        self.write(bundle())
        key = Backends.cache_key("run-a", "als-v1", 7, 2)
        redis.values[key] = json.dumps({"run_id": "other", "model_version": "als-v1",
                                       "visitorid": 7, "k": 2, "source": "als_bundle",
                                       "items": [{"itemid": 999, "score": 999}]})
        result = self.client.get("/api/recommendations/7?k=2").json()
        self.assertFalse(result["cache_hit"])
        self.assertNotIn(999, [row["itemid"] for row in result["items"]])

    def test_optional_redis_failure_falls_back_with_explicit_warning(self):
        redis = self.enable_redis()
        redis.fail = True
        self.write(bundle())
        result = self.client.get("/api/recommendations/7?k=2")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["source"], "als_bundle")
        self.assertTrue(result.json()["warnings"])
        ready = self.client.get("/health/ready")
        self.assertEqual(ready.status_code, 200)
        self.assertEqual(ready.json()["dependencies"]["redis"]["status"], "unavailable")

    def test_sqlite_filters_omitted_user_and_is_required_when_declared(self):
        value = bundle()
        value["seen_items_store"] = "seen_items.sqlite"
        target = self.write(value)
        self.assertEqual(self.client.get("/health/ready").status_code, 503)
        sidecar = target.parent / "seen_items.sqlite"
        with sqlite3.connect(sidecar) as db:
            db.execute("CREATE TABLE seen_items(visitorid INTEGER,itemid INTEGER,PRIMARY KEY(visitorid,itemid))")
            db.execute("INSERT INTO seen_items VALUES(888,1)")
        db.close()
        result = self.client.get("/api/recommendations/888?k=2").json()
        self.assertEqual(result["source"], "popular_fallback")
        self.assertEqual([row["itemid"] for row in result["items"]], [2, 5])
        self.assertIn("sqlite", result["history_sources"])
        sidecar.unlink()
        self.assertEqual(self.client.get("/health/ready").status_code, 503)
        self.assertEqual(self.client.get("/api/recommendations/888").status_code, 503)

    def test_pointer_and_sidecar_path_traversal_rejected(self):
        (self.outputs / "latest_run.json").write_text('{"run_id":"../../escape"}', encoding="utf-8")
        self.assertEqual(self.client.get("/health/ready").status_code, 503)
        value = bundle()
        value["seen_items_store"] = "../../history.sqlite"
        self.write(value)
        self.assertEqual(self.client.get("/health/ready").status_code, 503)

    def test_metrics_users_and_local_dashboard(self):
        self.write(bundle())
        directory = self.outputs / "runs" / "run-a" / "metrics"
        directory.mkdir()
        (directory / "kpis.json").write_text('{"run_id":"run-a","total_events":100}', encoding="utf-8")
        result = self.client.get("/api/metrics").json()
        self.assertEqual(result["kpis"]["total_events"], 100)
        self.assertEqual(result["model_metrics"]["heldout_test"]["precision_at_k"], 0.12)
        self.assertIn("revenue", result["unavailable_metrics"])
        users = self.client.get("/api/users?limit=1").json()
        self.assertEqual(users["user_ids"], [7])
        self.assertEqual(users["run_id"], "run-a")
        (directory / "kpis.json").write_text('{"run_id":"wrong","total_events":999}', encoding="utf-8")
        self.assertIsNone(self.client.get("/api/metrics").json()["kpis"])
        page = self.client.get("/")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Retail Signal", page.text)
        for name in ("app.js", "style.css"):
            asset = self.client.get(f"/static/{name}")
            self.assertEqual(asset.status_code, 200)
            self.assertNotIn("https://", asset.text)


if __name__ == "__main__":
    unittest.main()
