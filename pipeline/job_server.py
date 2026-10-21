"""Internal, bounded retraining worker for Airflow; no Docker socket or shell API."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pipeline.config import atomic_json, latest_run, utc_now

app = FastAPI(title="Internal retraining worker")
executor = ThreadPoolExecutor(max_workers=1)
lock = Lock()
active_job = None


class RetrainRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    quick: bool = True


def job_file(job_id):
    return Path(os.getenv("OUTPUT_ROOT", "outputs")) / "jobs" / f"{job_id}.json"


def run_job(job):
    global active_job
    job.update(status="running", started_at_utc=utc_now())
    atomic_json(job_file(job["job_id"]), job)
    try:
        from pipeline.runner import execute
        report = execute(job["run_id"], os.getenv("OUTPUT_ROOT", "outputs"), quick=job["quick"], source_run_id=job["source_run_id"])
        job.update(status="completed", result={"run_id": report["run_id"], "elapsed_seconds": report["elapsed_seconds"]})
    except Exception as error:
        job.update(status="failed", error=f"{type(error).__name__}: {error}")
    finally:
        job["finished_at_utc"] = utc_now()
        atomic_json(job_file(job["job_id"]), job)
        with lock:
            active_job = None


@app.get("/health")
def health():
    return {"status": "alive", "active_job": active_job}


@app.post("/jobs/retrain", status_code=202)
def retrain(request: RetrainRequest):
    global active_job
    identifier = hashlib.sha256(request.request_id.encode()).hexdigest()[:24]
    with lock:
        path = job_file(identifier)
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing["status"] in {"queued", "running"} and active_job != identifier:
                existing.update(status="failed", error="Worker restarted during job; trigger a new request ID after reviewing pipeline lock", finished_at_utc=utc_now())
                atomic_json(path, existing)
            return existing
        if active_job is not None:
            raise HTTPException(409, "One retraining job is already active")
        try:
            source = latest_run()
        except (OSError, ValueError, KeyError):
            raise HTTPException(409, "Run the initial data pipeline before scheduling retraining")
        job = {"job_id": identifier, "request_id": request.request_id, "source_run_id": source,
               "run_id": f"retrain-{identifier}", "quick": request.quick, "status": "queued", "created_at_utc": utc_now()}
        active_job = identifier
        atomic_json(path, job)
        executor.submit(run_job, job)
        return job


@app.get("/jobs/{job_id}")
def status(job_id: str):
    import re
    if not re.fullmatch(r"[0-9a-f]{24}", job_id):
        raise HTTPException(404, "Unknown job")
    path = job_file(job_id)
    if not path.exists():
        raise HTTPException(404, "Unknown job")
    return json.loads(path.read_text(encoding="utf-8"))
