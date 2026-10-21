"""Read-only state/registry proof; one GET recommendation exercises API reload/cache."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
from urllib.request import urlopen

from pymongo import MongoClient
import redis


ROOT = Path(os.getenv("OUTPUT_ROOT", "/app/outputs"))
JOB_ID = "64af6b3f57f5c1763b303ac8"
EXPECTED_RUN = "retrain-" + JOB_ID
EXPECTED_VERSION = EXPECTED_RUN + ":als-serving-v1"


def get_json(url):
    with urlopen(url, timeout=15) as response:
        return json.load(response)


def registry_summary(value):
    return {key: value.get(key) for key in ("run_id", "model_version", "trained_at_utc", "algorithm")} if value else None


job = json.loads((ROOT / "jobs" / f"{JOB_ID}.json").read_text())
latest = json.loads((ROOT / "latest_run.json").read_text())
dag = json.loads((ROOT / "evidence" / "airflow_dag_state.json").read_text())
ready = get_json("http://api:8000/health/ready")
api_metrics = get_json("http://api:8000/api/metrics")
users = get_json("http://api:8000/api/users?limit=1")
recommendation = get_json("http://api:8000/api/recommendations/" + str(users["user_ids"][0]) + "?k=5")
with MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"), serverSelectionTimeoutMS=5000) as client:
    mongo_registry = client.bigdata.model_registry.find_one({"run_id": EXPECTED_RUN}, {"_id": 0})
cache = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True, socket_timeout=5)
try:
    redis_raw = cache.get(f"bigdata:{EXPECTED_RUN}:active_model")
finally:
    cache.close()
redis_registry = json.loads(redis_raw) if redis_raw else None
connection = sqlite3.connect(f"file:{ROOT / 'runs' / EXPECTED_RUN / 'serving' / 'seen_items.sqlite'}?mode=ro", uri=True)
try:
    seen = {row[0] for row in connection.execute("SELECT itemid FROM seen_items WHERE visitorid=?", (recommendation["visitorid"],))}
finally:
    connection.close()
checks = {
    "job_completed": job["status"] == "completed",
    "airflow_dag_success": len(dag["dag_runs"]) == 1 and dag["dag_runs"][0]["state"] == "success",
    "airflow_all_tasks_success": len(dag["tasks"]) == 2 and all(task["state"] == "success" for task in dag["tasks"]),
    "latest_pointer_matches": latest.get("run_id") == EXPECTED_RUN and latest.get("model_version") == EXPECTED_VERSION,
    "api_ready": ready["status"] == "ready",
    "api_reloaded_new_model": api_metrics["run_id"] == EXPECTED_RUN and api_metrics["model_version"] == EXPECTED_VERSION and not api_metrics["model"]["stale"],
    "mongo_registry_matches": bool(mongo_registry) and mongo_registry["model_version"] == EXPECTED_VERSION,
    "redis_registry_matches": bool(redis_registry) and redis_registry["run_id"] == EXPECTED_RUN and redis_registry["model_version"] == EXPECTED_VERSION,
    "recommendation_version_matches": recommendation["run_id"] == EXPECTED_RUN and recommendation["model_version"] == EXPECTED_VERSION,
    "recommendation_excludes_observed_seen": not (seen & {item["itemid"] for item in recommendation["items"]}),
}
proof = {
    "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    "status": "passed" if all(checks.values()) else "failed",
    "checks": checks, "job": job, "airflow": dag, "latest_pointer": latest,
    "api_readiness": ready, "api_model": {key: api_metrics.get(key) for key in ("run_id", "model_version", "trained_at_utc", "metric_model_version", "algorithm")},
    "heldout_metrics": api_metrics["model_metrics"]["heldout_test"],
    "mongo_registry": registry_summary(mongo_registry), "redis_registry": registry_summary(redis_registry),
    "api_recommendation": recommendation,
    "note": "Manual Airflow run completed genuine retraining; ranking scores are retained as observed, including zero accuracy on sparse sample. No retrigger performed by verifier.",
}
destination = ROOT / "evidence" / "airflow_retrain_proof.json"
destination.write_text(json.dumps(proof, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
print(json.dumps(proof, indent=2, ensure_ascii=False, allow_nan=False))
raise SystemExit(0 if proof["status"] == "passed" else 1)
