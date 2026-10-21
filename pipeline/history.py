"""Full-history batch import, distinct from bounded-watermark Kafka replay."""
from pathlib import Path


def import_history(spark, input_csv, run_root, run_id):
    from pyspark.sql import functions as F, types as T
    from phase3.transforms import parse_events, valid_events, quarantined_events
    schema = T.StructType([
        T.StructField("timestamp", T.LongType()), T.StructField("visitorid", T.LongType()),
        T.StructField("event", T.StringType()), T.StructField("itemid", T.LongType()),
        T.StructField("transactionid", T.LongType()), T.StructField("_corrupt_record", T.StringType()),
    ])
    original = spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE").csv(input_csv).cache()
    try:
        count = original.count()
        mapping = F.create_map(F.lit("view"), F.lit("page_view"), F.lit("addtocart"), F.lit("add_to_cart"), F.lit("transaction"), F.lit("purchase"))
        canonical = F.concat(F.lit("["), F.col("timestamp"), F.lit(","), F.col("visitorid"), F.lit(',"'),
                             F.col("event"), F.lit('",'), F.col("itemid"), F.lit(","),
                             F.coalesce(F.col("transactionid").cast("string"), F.lit("null")), F.lit("]"))
        envelope = (original.withColumn("schema_version", F.lit(1)).withColumn("event_id", F.sha2(canonical, 256))
                    .withColumn("run_id", F.lit(run_id)).withColumn("event_type", mapping[F.col("event")])
                    .withColumn("ingested_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))
        frame = envelope.select(F.when(F.col("_corrupt_record").isNotNull(), F.lit("{")).otherwise(F.to_json(F.struct(
            "schema_version", "event_id", "run_id", "timestamp", "visitorid", "event", "itemid", "transactionid", "event_type", "ingested_at"))).alias("raw_value"))
        for name, dtype in (("kafka_topic", "string"), ("kafka_partition", "int"), ("kafka_offset", "long"),
                            ("kafka_timestamp", "timestamp"), ("source_file", "string")):
            frame = frame.withColumn(name, F.lit(input_csv if name == "source_file" else None).cast(dtype))
        parsed = parse_events(frame).cache()
        try:
            rejected = quarantined_events(parsed)
            errors = rejected.count()
            rejected.write.mode("overwrite").parquet(str(Path(run_root) / "quarantine"))
            if errors:
                raise ValueError(f"History import rejected {errors} records; inspect quarantine")
            clean = valid_events(parsed).dropDuplicates(["event_id"])
            clean.write.mode("errorifexists").parquet(str(Path(run_root) / "clean"))
            return {"raw_rows": count, "clean_rows": clean.count(), "rejected_rows": errors,
                    "source": input_csv, "mode": "batch_full_history_no_watermark"}
        finally:
            parsed.unpersist()
    finally:
        original.unpersist()
