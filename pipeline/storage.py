"""Phase 4 Delta Bronze/Silver/Gold derived from accepted Phase 3 events."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

EVENT_WEIGHTS = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0, "review": 0.0}


def utc_millis(value: int | None) -> str | None:
    if value is None:
        return None
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=int(value))).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def _save_json(root: Path, name: str, value: dict) -> None:
    from pipeline.config import atomic_json
    target = root / "metrics" / name
    atomic_json(target, value)


def build_lake(spark, run_root: str) -> dict:
    """Idempotent by event ID; individual Delta commits are atomic per table.

    Bronze is insert-only MERGE. Silver/Gold are deterministic batch snapshots.
    A failed multi-table build can be retried; the three tables are not one
    distributed transaction. Input is accepted clean data, not rejected/late data.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from phase3.schema import CLEAN_SCHEMA
    from pipeline.config import lake_path
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    root = Path(run_root)
    clean = (spark.read.schema(CLEAN_SCHEMA).parquet((root / "clean").as_posix())
             .dropDuplicates(["event_id"]).cache())
    try:
        if clean.count() == 0:
            raise ValueError("No accepted clean events; ingestion must finish before build_lake")
        bronze_path = lake_path(run_root, "bronze")
        if DeltaTable.isDeltaTable(spark, bronze_path):
            (DeltaTable.forPath(spark, bronze_path).alias("t")
             .merge(clean.alias("s"), "t.event_id = s.event_id")
             .whenNotMatchedInsertAll().execute())
        else:
            clean.write.format("delta").mode("errorifexists").save(bronze_path)
    finally:
        clean.unpersist()
    bronze = spark.read.format("delta").load(bronze_path)
    score = F.create_map(*[F.lit(value) for pair in EVENT_WEIGHTS.items() for value in pair])
    silver = (bronze.filter(F.col("event_id").isNotNull() & F.col("visitorid").isNotNull()
                            & F.col("itemid").isNotNull() & F.col("event").isin(*EVENT_WEIGHTS))
              .dropDuplicates(["event_id"])
              .withColumn("event_time", F.expr("timestamp_millis(timestamp)"))
              .withColumn("event_date", F.to_date("event_time"))
              .withColumn("interaction_score", score[F.col("event")].cast("float")))
    silver_path = lake_path(run_root, "silver")
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    silver = spark.read.format("delta").load(silver_path).cache()
    try:
        gold = (silver.filter(F.col("interaction_score") > 0)
                .groupBy("visitorid", "itemid")
                .agg(F.sum("interaction_score").cast("float").alias("interaction_score"),
                     F.count("*").cast("long").alias("count")))
        gold_path = lake_path(run_root, "gold_interactions")
        gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
        totals = silver.agg(
            F.count("*").alias("events"), F.countDistinct("visitorid").alias("visitors"),
            F.countDistinct("itemid").alias("items"),
            F.countDistinct(F.when(F.col("event") == "transaction", F.col("visitorid"))).alias("purchasers"),
            F.countDistinct(F.when(F.col("event") == "transaction", F.col("transactionid"))).alias("transactions"),
            F.min("timestamp").alias("first_ms"), F.max("timestamp").alias("last_ms"),
        ).first().asDict()
        distribution = {row["event"]: row["count"] for row in silver.groupBy("event").count().collect()}
        report = {
            "run_id": root.name, "bronze_events": bronze.count(), "silver_events": totals["events"],
            "gold_user_item_pairs": gold.count(), "visitors": totals["visitors"], "items": totals["items"],
            "event_counts": distribution, "purchasing_visitors": totals["purchasers"],
            "distinct_transaction_ids": totals["transactions"],
            "visitor_purchase_conversion": (totals["purchasers"] / totals["visitors"] if totals["visitors"] else None),
            "conversion_definition": "Distinct visitors with >=1 purchase event / all distinct observed visitors",
            "ctr": None, "revenue": None, "average_order_value": None,
            "unavailable_metrics_reason": "Source has no ad impressions/click attribution, price, currency or order totals",
            "first_event_time_utc": utc_millis(totals["first_ms"]),
            "last_event_time_utc": utc_millis(totals["last_ms"]),
            "weights": EVENT_WEIGHTS, "status": "completed",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "paths": {"bronze": bronze_path, "silver": silver_path, "gold_interactions": gold_path},
        }
        _save_json(root, "kpis.json", report)
        return report
    finally:
        silver.unpersist()


def main() -> None:
    import argparse
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
        print(json.dumps(build_lake(spark, config.root.as_posix()), ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
