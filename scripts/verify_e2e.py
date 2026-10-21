"""Assert published artifacts, real stores and HTTP serving agree on one completed run."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request

from pipeline.config import atomic_json, run_path, utc_now


def get_json(url):
    with urllib.request.urlopen(url, timeout=20) as response:
        return json.load(response)


def verify(base_url, run_id, output_root):
    root = run_path(run_id, output_root)
    report = {"run_id": run_id, "checked_at_utc": utc_now(), "status": "failed", "checks": {}}
    try:
        result = json.loads((root / "metrics/run_report.json").read_text(encoding="utf-8"))
        assert result["status"] == "completed", "Pipeline is not completed"
        report["checks"]["pipeline_completed"] = True
        bundle = json.loads((root / "serving/model_bundle.json").read_text(encoding="utf-8"))
        assert bundle["run_id"] == run_id and bundle["algorithm"] == "als_implicit"
        deadline = time.monotonic() + 60
        while True:
            try:
                ready = get_json(base_url.rstrip("/") + "/health/ready")
                break
            except (urllib.error.URLError, OSError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        assert ready["status"] == "ready", ready
        metrics = get_json(base_url.rstrip("/") + "/api/metrics")
        assert metrics["run_id"] == run_id and metrics["model_version"] == bundle["model_version"]
        assert metrics["kpis"]["silver_events"] >= result["clean_event_count"]
        assert metrics["kpis"]["ctr"] is None and metrics["kpis"]["revenue"] is None
        report["checks"]["api_model_and_kpis"] = True
        users = get_json(base_url.rstrip("/") + "/api/users?limit=3")["user_ids"]
        assert users, "No users exported by ALS serving bundle"
        tested = []
        with sqlite3.connect(root / "serving/seen_items.sqlite") as connection:
            omitted = next((int(row[0]) for row in connection.execute("SELECT DISTINCT visitorid FROM seen_items ORDER BY visitorid")
                            if str(row[0]) not in bundle["recommendations"]), None)
            visitors = users + ([omitted] if omitted is not None else []) + [2147483647]
            for visitor in visitors:
                response = get_json(base_url.rstrip("/") + f"/api/recommendations/{visitor}?k=10")
                assert response["run_id"] == run_id and response["model_version"] == bundle["model_version"]
                items = [row["itemid"] for row in response["items"]]
                assert len(items) == len(set(items)) and len(items) <= 10
                seen = {row[0] for row in connection.execute("SELECT itemid FROM seen_items WHERE visitorid=?", (visitor,))}
                assert not seen.intersection(items), "API returned a previously seen item"
                tested.append({"visitorid": visitor, "returned": len(items), "source": response["source"]})
        report["checks"]["known_and_cold_users_no_seen_items"] = tested
        from pymongo import MongoClient
        import redis
        with MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"), serverSelectionTimeoutMS=5000) as mongo:
            count = mongo.bigdata.events.count_documents({"run_id": run_id})
            assert count == metrics["kpis"]["silver_events"], (count, metrics["kpis"])
            assert mongo.bigdata.model_registry.count_documents({"run_id": run_id, "model_version": bundle["model_version"]}) == 1
            report["checks"]["mongo_event_count_and_model_registry"] = count
        cache = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
        try:
            assert json.loads(cache.get(f"bigdata:{run_id}:active_model"))["model_version"] == bundle["model_version"]
            assert cache.exists(f"bigdata:{run_id}:seen:{users[0]}")
            report["checks"]["redis_features_and_model_registry"] = True
        finally:
            cache.close()
        assert result["stages"]["acid"]["status"] == "passed"
        report["checks"]["delta_time_travel_and_atomic_rejection"] = True
        report["status"] = "passed"
        return report
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        atomic_json(root / "metrics/e2e_verification.json", report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--base-url", default="http://api:8000")
    parser.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "outputs"))
    args = parser.parse_args()
    print(json.dumps(verify(args.base_url, args.run_id, args.output_root), indent=2))
