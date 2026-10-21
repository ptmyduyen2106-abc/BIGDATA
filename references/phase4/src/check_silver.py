from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


SILVER_PATH = "hdfs://namenode:8020/ecommerce/delta/silver/events"


builder = (
    SparkSession.builder
    .appName("Check-Silver")
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

df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

print("=== SILVER COUNT ===")
print(df.count())

print("=== EVENT DISTRIBUTION ===")
df.groupBy("event").count().orderBy("event").show()

print("=== SILVER SCHEMA ===")
df.printSchema()

spark.stop()