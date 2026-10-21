from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"


def main():
    builder = (
        SparkSession.builder
        .appName("Test-Gold-Interactions")
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

    print("=== GOLD TABLE TEST ===")

    df = (
        spark.read
        .format("delta")
        .load(GOLD_PATH)
    )

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== TOTAL RECORDS ===")
    print(df.count())

    print("\n=== NULL CHECK ===")

    null_counts = df.select(
        df.visitorid.isNull().cast("int").alias("visitorid_null"),
        df.itemid.isNull().cast("int").alias("itemid_null"),
        df.interaction_score.isNull().cast("int").alias("score_null")
    )

    null_counts.agg({
        "visitorid_null": "sum",
        "itemid_null": "sum",
        "score_null": "sum"
    }).show()

    print("\n=== SCORE STATISTICS ===")

    df.select(
        "interaction_score",
        "interaction_count"
    ).summary().show()

    print("\n=== TOP USER-ITEM INTERACTIONS ===")

    (
        df.orderBy(df.interaction_score.desc())
        .show(20, truncate=False)
    )

    print("\n=== SAMPLE GOLD DATA ===")

    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()