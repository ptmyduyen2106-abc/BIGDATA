"""Execute Delta update, historical-read and atomic constraint-rejection checks."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pipeline.storage import _save_json


def verify_acid(spark, run_root: str) -> dict:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pipeline.config import lake_path
    root = Path(run_root)
    # Unique namespace preserves evidence and never alters business tables.
    path = lake_path(run_root, "acid_checks").rstrip("/") + "/" + uuid4().hex
    spark.range(2).selectExpr("id", "id * 10 AS value").write.format("delta").save(path)
    table = DeltaTable.forPath(spark, path)
    before_version = int(table.history(1).select("version").first()["version"])
    old_rows = [(int(row.id), int(row.value)) for row in spark.read.format("delta").load(path).orderBy("id").collect()]
    table.update(condition=F.col("id") == 1, set={"value": F.lit(99)})
    after_version = int(table.history(1).select("version").first()["version"])
    assert after_version > before_version, "Update did not create a new committed version"
    historical = [(int(row.id), int(row.value)) for row in spark.read.format("delta")
                  .option("versionAsOf", before_version).load(path).orderBy("id").collect()]
    assert historical == old_rows, "Historical version changed after update"
    assert spark.read.format("delta").load(path).filter("id = 1 AND value = 99").count() == 1
    escaped_path = path.replace("`", "``")
    spark.sql(f"ALTER TABLE delta.`{escaped_path}` ADD CONSTRAINT value_nonnegative CHECK (value >= 0)")
    rollback_version = int(table.history(1).select("version").first()["version"])
    rejected = False
    rejection_type = None
    rejection_message = ""
    try:
        spark.range(2, 4).selectExpr("id", "CAST(CASE WHEN id = 2 THEN 50 ELSE -1 END AS BIGINT) AS value") \
            .write.format("delta").mode("append").save(path)
    except Exception as error:
        rejected = True
        rejection_type = type(error).__name__
        rejection_message = str(error)
    assert rejected, "Invalid row unexpectedly passed Delta CHECK constraint"
    assert "value_nonnegative" in rejection_message, "Append failed for an unrelated reason, not the CHECK constraint"
    assert int(table.history(1).select("version").first()["version"]) == rollback_version
    assert spark.read.format("delta").load(path).count() == 2, "Rejected append partially committed"
    report = {
        "status": "passed", "path": path, "before_version": before_version,
        "after_update_version": after_version, "rollback_baseline_version": rollback_version,
        "checks": {"update_commit": True, "time_travel": True,
                   "constraint_rejection": True, "no_partial_append": True},
        "rejection_exception_type": rejection_type,
        "scope": "Actual per-table Delta operations; no concurrent-writer or distributed-transaction claim",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_json(root, "acid_check.json", report)
    return report


def main() -> None:
    import argparse
    import json
    from phase3.streaming import StreamConfig, create_spark
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default="outputs")
    parser.add_argument("--master")
    parser.add_argument("--no-download-jars", action="store_true")
    args = parser.parse_args()
    config = StreamConfig(args.run_id, output_root=(Path(args.output_root) / "runs").as_posix())
    spark = create_spark(args.master, enable_delta=True, download_jars=not args.no_download_jars)
    try:
        print(json.dumps(verify_acid(spark, config.root.as_posix()), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
