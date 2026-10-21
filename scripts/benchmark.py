"""Measure actual HTTP latency; no invented load/SLO or historical event-time latency."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import platform
import statistics
import time
import urllib.request

from pipeline.config import atomic_json, latest_run, run_path, utc_now


def percentile(values, fraction):
    values = sorted(values)
    index = (len(values) - 1) * fraction
    lower = math.floor(index)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (index - lower)


def benchmark(base_url, requests=100, concurrency=4):
    if not 1 <= requests <= 10000 or not 1 <= concurrency <= 32:
        raise ValueError("requests 1..10000 and concurrency 1..32")
    with urllib.request.urlopen(base_url.rstrip("/") + "/api/users?limit=1", timeout=20) as response:
        user = json.load(response)["user_ids"][0]
    url = base_url.rstrip("/") + f"/api/recommendations/{user}?k=10"
    def request_one(_):
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = json.load(response)
            assert payload["visitorid"] == user
            return (time.perf_counter() - start) * 1000, None
        except Exception as error:
            return (time.perf_counter() - start) * 1000, f"{type(error).__name__}: {error}"
    for index in range(5):
        duration, error = request_one(index)
        if error:
            raise RuntimeError(error)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(request_one, range(requests)))
    elapsed = time.perf_counter() - start
    good = [value for value, error in results if error is None]
    errors = [error for _, error in results if error is not None]
    return {"measured_at_utc": utc_now(), "requests": requests, "concurrency": concurrency,
            "successful": len(good), "errors": errors[:10], "elapsed_seconds": elapsed,
            "throughput_requests_per_second": len(good) / elapsed,
            "latency_ms": {"mean": statistics.mean(good), "p50": percentile(good, .50),
                           "p95": percentile(good, .95), "p99": percentile(good, .99)} if good else {},
            "environment": platform.platform(), "status": "passed" if not errors else "failed",
            "scope": "HTTP recommendation API, five warmup requests, selected user and configured concurrency. No production scale/SLO claim."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://api:8000")
    parser.add_argument("--run-id")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    run_id = args.run_id or latest_run()
    result = benchmark(args.base_url, args.requests, args.concurrency)
    result["run_id"] = run_id
    atomic_json(run_path(run_id) / "metrics/benchmark.json", result)
    print(json.dumps(result, indent=2))
    if result["status"] != "passed":
        raise SystemExit(1)
