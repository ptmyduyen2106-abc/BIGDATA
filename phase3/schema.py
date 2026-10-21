"""The versioned wire contract and durable clean-event schema.

Keep this explicit: inferSchema on live JSON can silently change types between
micro-batches. Timestamps in the source CSV are epoch milliseconds, not seconds.
"""

from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

TOPICS = ("page_view", "add_to_cart", "purchase", "review")
EVENT_TYPES = {
    "view": "page_view", "addtocart": "add_to_cart",
    "transaction": "purchase", "review": "review",
}
RAW_COLUMNS = ("timestamp", "visitorid", "event", "itemid", "transactionid")

WIRE_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("run_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("event_time", StringType()),
    StructField("visitorid", LongType()),
    StructField("itemid", LongType()),
    StructField("event", StringType()),
    StructField("event_type", StringType()),
    StructField("transactionid", LongType()),
    StructField("ingested_at", StringType()),
    StructField("review_text", StringType()),
    StructField("rating", DoubleType()),
    StructField("synthetic", BooleanType()),
    StructField("_corrupt_record", StringType()),
])

CLEAN_SCHEMA = StructType([
    field for field in WIRE_SCHEMA.fields
    if field.name not in {"event_time", "_corrupt_record"}
] + [
    StructField("event_time", TimestampType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("source_file", StringType()),
])
