from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import redis

GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"

REDIS_HOST = "redis"
REDIS_PORT = 6379

TOP_N = 10


def main():
    spark = (
        SparkSession.builder
        .appName("RetailRocket-Gold-To-Redis")
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

    print("=== READ GOLD ===")

    gold_df = (
        spark.read
        .format("delta")
        .load(GOLD_PATH)
    )

    print("Gold count:", gold_df.count())

    print("=== TOP ITEMS ===")

    top_items = (
        gold_df
        .groupBy("itemid")
        .sum("interaction_score")
        .withColumnRenamed(
            "sum(interaction_score)",
            "total_score"
        )
        .orderBy(col("total_score").desc())
        .limit(TOP_N)
    )

    top_items.show()

    items = [
        int(row["itemid"])
        for row in top_items.collect()
    ]

    print("Top items:", items)

    print("=== CONNECT REDIS ===")

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    print("Redis ping:", r.ping())

    r.set(
        "global:top_items",
        ",".join(map(str, items))
    )

    print("=== REDIS WRITE COMPLETED ===")

    spark.stop()


if __name__ == "__main__":
    main()  