"""Validate supplied dataset hashes and profile source CSV without external downloads."""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

from pipeline.config import atomic_json, utc_now


def inspect_sources(data_root="data"):
    root = Path(data_root)
    manifest = json.loads((root / "metadata/manifest.json").read_text(encoding="utf-8"))
    files = {}
    for name in ("events.csv", "category_tree.csv"):
        path = root / "raw" / name
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        assert digest == manifest[name]["sha256"], f"Source checksum mismatch: {name}"
        files[name] = {"bytes": path.stat().st_size, "sha256": digest, "source": manifest[name]["source_page"]}
    counts, missing_tx, previous, inversions = Counter(), 0, None, 0
    minimum = maximum = None
    with (root / "raw/events.csv").open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == ["timestamp", "visitorid", "event", "itemid", "transactionid"]
        for row in reader:
            timestamp, user, item = int(row["timestamp"]), int(row["visitorid"]), int(row["itemid"])
            assert timestamp > 0 and user >= 0 and item >= 0
            assert row["event"] in {"view", "addtocart", "transaction"}
            transaction = int(row["transactionid"]) if row["transactionid"] else None
            assert transaction is None or transaction >= 0
            missing_tx += row["event"] == "transaction" and transaction is None
            counts[row["event"]] += 1
            inversions += previous is not None and timestamp < previous
            previous = timestamp
            minimum = timestamp if minimum is None else min(minimum, timestamp)
            maximum = timestamp if maximum is None else max(maximum, timestamp)
    assert missing_tx == 0
    return {"status": "passed", "checked_at_utc": utc_now(), "files": files, "rows": sum(counts.values()),
            "event_counts": dict(counts), "missing_transaction_ids_on_purchase": missing_tx,
            "timestamp_decreases": inversions, "minimum_epoch_ms": minimum, "maximum_epoch_ms": maximum,
            "limitations": ["No real reviews", "No product prices or impression denominator", "No item-to-category mapping"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--report", default="outputs/evidence/dataset_profile.json")
    args = parser.parse_args()
    result = inspect_sources(args.data_root)
    atomic_json(args.report, result)
    print(json.dumps(result, indent=2))
