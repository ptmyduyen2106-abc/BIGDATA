from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from delta import configure_spark_with_delta_pip


BRONZE_PATH = "hdfs://namenode:8020/ecommerce/delta/bronze/events"
SILVER_PATH = "hdfs://namenode:8020/ecommerce/delta/silver/events"


def main():
    builder = (
        SparkSession.builder
        .appName("RetailRocket-Batch-Silver")
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

    print("=== STEP 1: READ BRONZE DELTA ===")

    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    print("Bronze count:", bronze_df.count())

    print("=== STEP 2: CLEAN DATA ===")

    silver_df = (
        bronze_df
        .filter(col("visitorid").isNotNull())
        .filter(col("itemid").isNotNull())
        .filter(
            col("event").isin(
                "view",
                "addtocart",
                "transaction"
            )
        )
        .dropDuplicates([
            "timestamp",
            "visitorid",
            "event",
            "itemid",
            "transactionid"
        ])
        .withColumn(
            "event_time",
            from_unixtime(col("timestamp") / 1000).cast("timestamp")
        )
    )

    print("Silver schema:")
    silver_df.printSchema()

    print("Silver count:", silver_df.count())

    print("=== STEP 3: EVENT DISTRIBUTION ===")

    (
        silver_df
        .groupBy("event")
        .count()
        .orderBy("event")
        .show()
    )

    print("=== STEP 4: WRITE SILVER DELTA ===")

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print("Silver Delta table written to:")
    print(SILVER_PATH)

    print("=== SILVER SAMPLE ===")

    silver_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()