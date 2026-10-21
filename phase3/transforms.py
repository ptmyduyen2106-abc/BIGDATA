"""Reusable transforms; no SparkSession is created when importing this module."""

from pyspark.sql import DataFrame, functions as F

from phase3.schema import CLEAN_SCHEMA, EVENT_TYPES, WIRE_SCHEMA


def parse_events(raw: DataFrame, filter_run_id: str | None = None) -> DataFrame:
    """Parse transport rows and attach a deterministic validation_error.

    ``raw`` must contain raw_value and the five metadata columns added by
    read_source. Preserve the raw payload in quarantine for investigating errors.
    No wall-clock check is made: RetailRocket is a historical replay dataset.
    """
    parsed = raw.withColumn(
        "payload", F.from_json("raw_value", WIRE_SCHEMA, {
            "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
        }),
    ).select("raw_value", "kafka_topic", "kafka_partition", "kafka_offset",
             "kafka_timestamp", "source_file", "payload.*")
    if filter_run_id is not None:
        # Invalid JSON is still quarantined rather than silently filtered out.
        parsed = parsed.filter(
            F.col("run_id").isNull() | (F.col("run_id") == filter_run_id)
        )
    event_mapping = F.create_map(*[
        F.lit(value) for pair in EVENT_TYPES.items() for value in pair
    ])
    # The stable ID is SHA256 of compact JSON [ts,visitor,event,item,tx].
    # Integers are concatenated directly to avoid Spark coercing mixed arrays
    # into an array<string>; the event enum needs no JSON escaping.
    canonical = F.concat(
        F.lit("["), F.col("timestamp").cast("string"), F.lit(","),
        F.col("visitorid").cast("string"), F.lit(',"'), F.col("event"),
        F.lit('",'), F.col("itemid").cast("string"), F.lit(","),
        F.coalesce(F.col("transactionid").cast("string"), F.lit("null")), F.lit("]"),
    )
    invalid = (
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_json_or_type")
        .when(F.col("schema_version").isNull() | (F.col("schema_version") != 1),
              "unsupported_schema_version")
        .when(F.col("run_id").isNull() | (F.length(F.trim("run_id")) == 0),
              "missing_run_id")
        .when(F.col("ingested_at").isNull()
              | ~F.col("ingested_at").rlike(r"T.*(?:Z|[+-]\d{2}:\d{2})$")
              | F.to_timestamp("ingested_at").isNull(), "invalid_ingested_at")
        .when(F.col("timestamp").isNull() | (F.col("timestamp") <= 0)
              | (F.col("timestamp") > 253402300799999), "invalid_timestamp_ms")
        .when(F.col("visitorid").isNull() | (F.col("visitorid") < 0),
              "invalid_visitorid")
        .when(F.col("itemid").isNull() | (F.col("itemid") < 0), "invalid_itemid")
        .when(F.col("event").isNull() | ~F.col("event").isin(*EVENT_TYPES),
              "unsupported_event")
        .when(F.col("event_type").isNull()
              | (F.col("event_type") != event_mapping[F.col("event")]),
              "event_type_mismatch")
        .when(F.col("kafka_topic").isNotNull()
              & (F.col("kafka_topic") != F.col("event_type")), "topic_mismatch")
        .when(F.col("transactionid").isNotNull() & (F.col("transactionid") < 0),
              "invalid_transactionid")
        .when((F.col("event") == "transaction") & F.col("transactionid").isNull(),
              "purchase_without_transactionid")
        .when(F.col("rating").isNotNull()
              & (F.isnan("rating") | (F.col("rating") < 1) | (F.col("rating") > 5)
                 | (F.floor("rating") != F.col("rating"))), "invalid_rating")
        .when(F.col("event_id").isNull()
              | ~F.col("event_id").rlike("^[0-9a-f]{64}$")
              | (F.col("event_id") != F.sha2(canonical, 256)), "event_id_mismatch")
        .otherwise(F.lit(None).cast("string"))
    )
    return (parsed.withColumn("validation_error", invalid)
            .drop("event_time")
            .withColumn("event_time", F.when(
                (F.col("timestamp") > 0) & (F.col("timestamp") <= 253402300799999),
                F.expr("timestamp_millis(timestamp)"))))


def valid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("validation_error").isNull()).select(
        *[field.name for field in CLEAN_SCHEMA]
    )


def quarantined_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("validation_error").isNotNull()).select(
        "raw_value", "validation_error", "kafka_topic", "kafka_partition",
        "kafka_offset", "kafka_timestamp", "source_file", "event_id", "run_id",
    )


def deduplicate(events: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    # One stateful operator in this query. Aggregations reread the durable sink.
    return events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )


def _aggregates() -> list:
    return [
        F.count("*").alias("event_count"),
        *[F.sum(F.when(F.col("event_type") == kind, 1).otherwise(0))
          .cast("long").alias(f"{kind}_count")
          for kind in ("page_view", "add_to_cart", "purchase", "review")],
        F.approx_count_distinct("itemid").alias("unique_items_approx"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    ]


def _ratios(features: DataFrame) -> DataFrame:
    # This is a behavioral proxy, not revenue, true CTR, or unique-user conversion.
    return features.withColumn(
        "interaction_score", F.col("page_view_count")
        + 3 * F.col("add_to_cart_count") + 5 * F.col("purchase_count"),
    ).withColumn(
        "purchase_per_view",
        F.when(F.col("page_view_count") > 0,
               F.col("purchase_count") / F.col("page_view_count"))
         .otherwise(F.lit(None).cast("double")),
    )


def window_features(events: DataFrame, watermark: str = "10 minutes",
                    duration: str = "5 minutes", slide: str = "1 minute") -> DataFrame:
    grouped = events.withWatermark("event_time", watermark).groupBy(
        "visitorid", F.window("event_time", duration, slide).alias("window"),
    ).agg(*_aggregates())
    return _ratios(grouped.select(
        "visitorid", F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        *[column for column in grouped.columns if column not in {"visitorid", "window"}],
    ))


def session_features(events: DataFrame, watermark: str = "10 minutes",
                     gap: str = "30 minutes") -> DataFrame:
    grouped = events.withWatermark("event_time", watermark).groupBy(
        "visitorid", F.session_window("event_time", gap).alias("session"),
    ).agg(*_aggregates())
    flattened = grouped.select(
        "visitorid", F.col("session.start").alias("session_start"),
        F.col("session.end").alias("session_end"),
        *[column for column in grouped.columns if column not in {"visitorid", "session"}],
    )
    return _ratios(flattened.withColumn(
        # Activity duration excludes the trailing 30-minute inactivity timeout.
        "active_duration_seconds",
        F.col("last_event_time").cast("double")
        - F.col("first_event_time").cast("double"),
    ).withColumn(
        "session_id", F.sha2(F.concat_ws(
            "|", F.col("visitorid").cast("string"),
            F.col("session_start").cast("string"),
        ), 256),
    ))
