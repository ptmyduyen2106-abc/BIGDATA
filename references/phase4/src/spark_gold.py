from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum as spark_sum,
    count
)
from delta import configure_spark_with_delta_pip


SILVER_PATH = "hdfs://namenode:8020/ecommerce/delta/silver/events"
GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"


def main():
    builder = (
        SparkSession.builder
        .appName("RetailRocket-Batch-Gold")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("=== STEP 1: READ SILVER DELTA ===")

    silver_df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    print("Silver count:", silver_df.count())

    print("=== STEP 2: ASSIGN INTERACTION SCORE ===")

    scored_df = (
        silver_df
        .withColumn(
            "interaction_score",
            when(col("event") == "view", 1)
            .when(col("event") == "addtocart", 3)
            .when(col("event") == "transaction", 5)
            .otherwise(0)
        )
    )

    print("Scored event distribution:")

    (
        scored_df
        .groupBy("event", "interaction_score")
        .count()
        .orderBy("event")
        .show()
    )

    print("=== STEP 3: AGGREGATE USER-ITEM INTERACTIONS ===")

    gold_df = (
        scored_df
        .groupBy("visitorid", "itemid")
        .agg(
            spark_sum("interaction_score").alias("interaction_score"),
            count("*").alias("interaction_count")
        )
    )

    print("Gold schema:")
    gold_df.printSchema()

    print("Gold interaction count:", gold_df.count())

    print("=== STEP 4: WRITE GOLD DELTA ===")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    print("Gold Delta table written to:")
    print(GOLD_PATH)

    print("=== GOLD SAMPLE ===")

    (
        gold_df
        .orderBy(col("interaction_score").desc())
        .show(10, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()