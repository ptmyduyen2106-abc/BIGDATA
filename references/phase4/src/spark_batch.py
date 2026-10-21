from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip


HDFS_RAW_PATH = "hdfs://namenode:8020/ecommerce/raw/events/events.csv"
DELTA_BRONZE_PATH = "hdfs://namenode:8020/ecommerce/delta/bronze/events"


def main():
    builder = (
        SparkSession.builder
        .appName("RetailRocket-Batch-Bronze")
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

    print("=== STEP 1: READ RAW DATA FROM HDFS ===")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(HDFS_RAW_PATH)
    )

    print("Raw schema:")
    df.printSchema()

    print("Raw count:", df.count())

    print("=== STEP 2: STANDARDIZE BRONZE SCHEMA ===")

    bronze_df = (
        df.select(
            col("timestamp").cast("long").alias("timestamp"),
            col("visitorid").cast("long").alias("visitorid"),
            col("event").cast("string").alias("event"),
            col("itemid").cast("long").alias("itemid"),
            col("transactionid").cast("long").alias("transactionid")
        )
    )

    print("Bronze schema:")
    bronze_df.printSchema()

    print("=== STEP 3: WRITE DELTA BRONZE ===")

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .save(DELTA_BRONZE_PATH)
    )

    print("Bronze Delta table written to:")
    print(DELTA_BRONZE_PATH)

    print("Bronze count:", bronze_df.count())

    print("=== BRONZE SAMPLE ===")
    bronze_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()