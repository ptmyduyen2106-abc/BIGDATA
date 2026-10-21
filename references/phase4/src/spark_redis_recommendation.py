from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import redis

GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"

USER_ID = 1098176
TOP_N = 10

REDIS_HOST = "redis"
REDIS_PORT = 6379


def main():
    spark = (
        SparkSession.builder
        .appName("RetailRocket-User-Recommendation-To-Redis")
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

    # Đọc Gold
    gold_df = spark.read.format("delta").load(GOLD_PATH)

    # Item user đã tương tác
    user_items = (
        gold_df
        .filter(col("visitorid") == USER_ID)
        .select("itemid")
        .distinct()
    )

    # Tính popularity của từng item
    item_scores = (
        gold_df
        .groupBy("itemid")
        .agg(
            spark_sum("interaction_score").alias("total_score")
        )
    )

    # Loại item user đã tương tác
    recommendations = (
        item_scores
        .join(user_items, on="itemid", how="left_anti")
        .orderBy(col("total_score").desc())
        .limit(TOP_N)
    )

    recommendations.show(TOP_N, truncate=False)

    # Lấy danh sách item
    items = [
        str(row["itemid"])
        for row in recommendations.collect()
    ]

    # Kết nối Redis
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    print("Redis ping:", r.ping())

    # Redis key cho user
    redis_key = f"user:{USER_ID}:recommendations"

    # Xóa dữ liệu cũ rồi ghi recommendation mới
    r.delete(redis_key)

    if items:
        r.rpush(redis_key, *items)

    print("Redis key:", redis_key)
    print("Recommendations:", items)

    print("=== USER RECOMMENDATION SAVED TO REDIS ===")

    spark.stop()


if __name__ == "__main__":
    main()