"""Nạp & làm sạch sự kiện (Silver), dựng Gold, kiểm tra nhất quán Gold <-> rating ALS.

Quy tắc làm sạch trùng với Phase 4 (`spark_silver.py`):
  - bỏ dòng thiếu visitorid/itemid
  - chỉ giữ event in (view, addtocart, transaction)
  - dropDuplicates theo 5 cột gốc (hoặc theo event_id nếu bảng Silver của Phase 3-4 có sẵn)
Trọng số tương tác trùng với Phase 4 Gold: view=1, addtocart=3, transaction=5.
"""
from __future__ import annotations

EVENT_TYPES = ("view", "addtocart", "transaction")
EVENT_WEIGHTS = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}
MAX_INT_ID = 2147483647
DEDUP_COLUMNS = ["timestamp", "visitorid", "event", "itemid", "transactionid"]


def load_events(spark, source: str, path: str):
    """Trả về (events_df, stats). source: csv | parquet | delta."""
    from pyspark.sql import functions as F, types as T

    if source == "csv":
        schema = T.StructType([
            T.StructField("timestamp", T.LongType()), T.StructField("visitorid", T.LongType()),
            T.StructField("event", T.StringType()), T.StructField("itemid", T.LongType()),
            T.StructField("transactionid", T.LongType())])
        raw = spark.read.schema(schema).option("header", True).csv(path)
    elif source in ("parquet", "delta"):
        raw = spark.read.format(source).load(path)
    else:
        raise ValueError("source phải là csv, parquet hoặc delta")

    missing = {"timestamp", "visitorid", "itemid", "event"} - set(raw.columns)
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc: {sorted(missing)}")
    raw_rows = raw.count()

    cleaned = (raw.filter(F.col("visitorid").isNotNull() & F.col("itemid").isNotNull()
                          & F.col("timestamp").isNotNull() & (F.col("timestamp") > 0)
                          & F.col("event").isin(*EVENT_TYPES)))
    if "event_id" in raw.columns:
        cleaned = cleaned.dropDuplicates(["event_id"])
    else:
        keys = [c for c in DEDUP_COLUMNS if c in raw.columns]
        cleaned = cleaned.dropDuplicates(keys)

    bad_ids = cleaned.filter((F.col("visitorid") < 0) | (F.col("visitorid") > MAX_INT_ID)
                             | (F.col("itemid") < 0) | (F.col("itemid") > MAX_INT_ID))
    if bad_ids.limit(1).count():
        raise ValueError("ALS cần ID không âm trong miền 32-bit; không cast ngầm")

    events = cleaned.select("timestamp", "visitorid", "event", "itemid").cache()
    stats = describe_events(events)
    stats.update({"source": source, "path": path, "raw_rows": raw_rows,
                  "removed_rows": raw_rows - stats["clean_rows"]})
    return events, stats


def describe_events(events) -> dict:
    """Thống kê mô tả (tính lại được sau khi lọc/cắt dữ liệu)."""
    from pyspark.sql import functions as F
    clean_rows = events.count()
    counts = {r["event"]: r["count"] for r in events.groupBy("event").count().collect()}
    agg = events.agg(F.countDistinct("visitorid").alias("users"), F.countDistinct("itemid").alias("items"),
                     F.min("timestamp").alias("t0"), F.max("timestamp").alias("t1")).first()
    return {"clean_rows": clean_rows, "event_counts": counts, "users": agg["users"], "items": agg["items"],
            "first_event_ms": agg["t0"], "last_event_ms": agg["t1"]}


def build_gold(events):
    """Gold của Phase 4: (visitorid, itemid, interaction_score, interaction_count)."""
    from pyspark.sql import functions as F
    score = (F.when(F.col("event") == "view", 1.0).when(F.col("event") == "addtocart", 3.0)
             .when(F.col("event") == "transaction", 5.0).otherwise(0.0))
    return (events.withColumn("interaction_score", score)
            .groupBy("visitorid", "itemid")
            .agg(F.sum("interaction_score").alias("interaction_score"),
                 F.count("*").alias("interaction_count")))


def verify_gold_consistency(events, gold) -> dict:
    """rating ALS (view + 3*cart + 5*purchase) phải bằng interaction_score của Gold, từng cặp."""
    from pyspark.sql import functions as F
    from pipeline.train import prepare_interactions
    rating = prepare_interactions(events).select(
        F.col("visitorid").cast("long").alias("visitorid"), F.col("itemid").cast("long").alias("itemid"),
        F.col("rating"))
    joined = rating.join(gold, ["visitorid", "itemid"], "full_outer")
    mismatches = joined.filter(F.col("rating").isNull() | F.col("interaction_score").isNull()
                               | (F.abs(F.col("rating") - F.col("interaction_score")) > 1e-6)).count()
    return {"rating_pairs": rating.count(), "gold_pairs": gold.count(), "mismatched_pairs": mismatches,
            "consistent": mismatches == 0}
