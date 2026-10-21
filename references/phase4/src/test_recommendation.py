from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"

USER_ID = 1098176
TOP_N = 10

spark = (
    SparkSession.builder
    .appName("Test-Recommendation")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

gold_df = spark.read.format("delta").load(GOLD_PATH)

# Các item user đã tương tác
user_items = (
    gold_df
    .filter(col("visitorid") == USER_ID)
    .select("itemid")
    .distinct()
)

# Tổng interaction score của từng item
item_scores = (
    gold_df
    .groupBy("itemid")
    .agg(
        spark_sum("interaction_score").alias("total_score")
    )
)

# Đề xuất item user chưa từng tương tác
recommendations = (
    item_scores
    .join(user_items, on="itemid", how="left_anti")
    .orderBy(col("total_score").desc())
    .limit(TOP_N)
)

print("=== RECOMMENDATIONS FOR USER", USER_ID, "===")

recommendations.show(TOP_N, truncate=False)

spark.stop()