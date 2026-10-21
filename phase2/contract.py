"""Shared, dependency-free event contract; timestamps remain dataset event time."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping

SCHEMA_VERSION = 1
RAW_FIELDS = ("timestamp", "visitorid", "event", "itemid", "transactionid")
EVENT_TO_TOPIC = {
    "view": "page_view",
    "addtocart": "add_to_cart",
    "transaction": "purchase",
    "review": "review",
}
TOPICS = tuple(EVENT_TO_TOPIC.values())


class ContractError(ValueError):
    """A source row or Kafka message does not satisfy schema version 1."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _integer(value: Any, field: str, nullable: bool = False) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if nullable:
            return None
        raise ContractError(f"{field} is required")
    if isinstance(value, bool) or isinstance(value, float):
        raise ContractError(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError(f"{field} must be an integer: {value!r}") from exc
    if parsed < 0:
        raise ContractError(f"{field} must be non-negative")
    return parsed


def normalize_raw(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize CSV fields without inventing identifiers or transaction values."""
    missing = set(RAW_FIELDS) - row.keys()
    if missing:
        raise ContractError(f"Missing CSV columns: {', '.join(sorted(missing))}")
    raw_event = str(row["event"]).strip()
    if raw_event not in EVENT_TO_TOPIC:
        raise ContractError(f"Unknown event: {raw_event!r}")
    result = {
        "timestamp": _integer(row["timestamp"], "timestamp"),
        "visitorid": _integer(row["visitorid"], "visitorid"),
        "event": raw_event,
        "itemid": _integer(row["itemid"], "itemid"),
        "transactionid": _integer(row["transactionid"], "transactionid", nullable=True),
    }
    if not 0 < result["timestamp"] <= 253402300799999:
        raise ContractError("timestamp must be positive UTC epoch milliseconds before year 10000")
    if raw_event == "transaction" and result["transactionid"] is None:
        raise ContractError("transaction events require transactionid")
    return result


def event_id_for(raw: Mapping[str, Any]) -> str:
    """Hash the normalized five source fields: same source row => same ID on replay."""
    normalized = normalize_raw(raw)
    canonical = json.dumps([normalized[name] for name in RAW_FIELDS],
                           ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_event(row: Mapping[str, Any], run_id: str, *, ingested_at: str | None = None) -> dict[str, Any]:
    if not isinstance(run_id, str) or not run_id.strip():
        raise ContractError("run_id must be a non-empty string")
    raw = normalize_raw(row)
    result = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id_for(raw),
        "run_id": run_id,
        **raw,
        "event_type": EVENT_TO_TOPIC[raw["event"]],
        "event_time": datetime.fromtimestamp(raw["timestamp"] / 1000, timezone.utc).isoformat(timespec="milliseconds"),
        "ingested_at": ingested_at or utc_now(),
    }
    if raw["event"] == "review":
        if row.get("review_text"):
            result["review_text"] = str(row["review_text"])
        if row.get("rating") not in (None, ""):
            rating = _integer(row["rating"], "rating")
            if not 1 <= rating <= 5:
                raise ContractError("rating must be an integer from 1 to 5")
            result["rating"] = rating
    synthetic = row.get("synthetic")
    if synthetic in (True, "true", "True", "1", 1):
        result["synthetic"] = True
    elif synthetic not in (None, "", False, "false", "False", "0", 0):
        raise ContractError("synthetic must be true or false")
    validate_event(result)
    return result


def validate_event(event: Mapping[str, Any], *, topic: str | None = None) -> None:
    """Validate the envelope and source content; used by producer and verifier."""
    raw = normalize_raw(event)
    if event.get("event") != raw["event"]:
        raise ContractError("event must be a normalized raw event name")
    if type(event.get("schema_version")) is not int or event["schema_version"] != SCHEMA_VERSION:
        raise ContractError("schema_version must equal integer 1")
    for name in ("timestamp", "visitorid", "itemid"):
        if type(event.get(name)) is not int:
            raise ContractError(f"{name} must be a JSON integer")
    if event.get("transactionid") is not None and type(event["transactionid"]) is not int:
        raise ContractError("transactionid must be a JSON integer or null")
    if not isinstance(event.get("run_id"), str) or not event["run_id"].strip():
        raise ContractError("run_id must be a non-empty string")
    expected_type = EVENT_TO_TOPIC[raw["event"]]
    if event.get("event_type") != expected_type:
        raise ContractError("event_type does not match event")
    if topic is not None and topic != expected_type:
        raise ContractError(f"Wrong Kafka topic {topic!r}; expected {expected_type!r}")
    if event.get("event_id") != event_id_for(raw):
        raise ContractError("event_id does not match normalized source content")
    expected_time = datetime.fromtimestamp(raw["timestamp"] / 1000, timezone.utc).isoformat(timespec="milliseconds")
    if event.get("event_time") != expected_time:
        raise ContractError("event_time must equal timestamp expressed as UTC ISO 8601")
    try:
        ingestion_time = datetime.fromisoformat(event["ingested_at"].replace("Z", "+00:00"))
        if ingestion_time.utcoffset() is None:
            raise ValueError("timezone required")
    except (ValueError, TypeError, AttributeError, KeyError) as exc:
        raise ContractError("ingested_at must be a timezone-aware ISO 8601 datetime") from exc
    if "rating" in event and (type(event["rating"]) is not int or not 1 <= event["rating"] <= 5):
        raise ContractError("rating must be an integer from 1 to 5")
    if "synthetic" in event and type(event["synthetic"]) is not bool:
        raise ContractError("synthetic must be a JSON boolean")


def serialize_event(event: Mapping[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
