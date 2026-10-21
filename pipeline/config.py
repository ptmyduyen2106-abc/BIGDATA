from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import uuid


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def validate_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Invalid run_id: use 1-128 letters, digits, underscore, dot or hyphen")
    return value


def run_path(run_id, output_root=None):
    return Path(output_root or os.getenv("OUTPUT_ROOT", "outputs")) / "runs" / validate_run_id(run_id)


def resolve_data_run_id(run_id, output_root=None):
    """Follow model snapshots back to the ingestion run still receiving events.

    Model activation changes the serving version, not the Kafka producer's run
    ID. Both online history filtering and the next retrain must retain this
    lineage, including through several consecutive retraining generations.
    """
    current = validate_run_id(run_id)
    visited = set()
    for _ in range(64):
        if current in visited:
            raise ValueError("Cycle in source run lineage")
        visited.add(current)
        path = run_path(current, output_root) / "metrics/run_report.json"
        if not path.exists():
            return current
        if path.stat().st_size > 4 * 1024 * 1024:
            raise ValueError("Source run report exceeds 4 MiB")
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as error:
            raise ValueError(f"Cannot read source run lineage for {current}") from error
        if not isinstance(report, dict) or report.get("run_id", current) != current:
            raise ValueError("Source run report has invalid identity")
        source = report.get("source_data_run_id")
        if source is None:
            source = report.get("source_run_id")
        if source is None:
            return current
        if not isinstance(source, str):
            raise ValueError("Source run ID must be a string")
        current = validate_run_id(source)
    raise ValueError("Source run lineage exceeds 64 generations")


def lake_path(run_root, table):
    if not re.fullmatch(r"[a-z_]+", table):
        raise ValueError("Invalid lake table name")
    base = os.getenv("LAKE_BASE_URI", "").rstrip("/")
    if base:
        return f"{base}/{validate_run_id(Path(run_root).name)}/delta/{table}"
    return (Path(run_root) / "delta" / table).as_posix()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def latest_run(output_root=None):
    root = Path(output_root or os.getenv("OUTPUT_ROOT", "outputs"))
    data = json.loads((root / "latest_run.json").read_text(encoding="utf-8"))
    return validate_run_id(data["run_id"])


@contextmanager
def pipeline_lock(output_root):
    path = Path(output_root) / ".pipeline.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = path.open("x", encoding="utf-8")
    except FileExistsError as exc:
        raise RuntimeError("Another pipeline is active, or a crashed run left outputs/.pipeline.lock. Inspect it before manual recovery.") from exc
    try:
        handle.write(json.dumps({"pid": os.getpid(), "started_at": utc_now()}))
        handle.close()
        yield
    finally:
        if not handle.closed:
            handle.close()
        path.unlink(missing_ok=True)
