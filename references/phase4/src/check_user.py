from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckUser") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load(
    "hdfs://namenode:8020/ecommerce/delta/gold/interactions"
)

df.filter(
    col("visitorid") == 1098176
).orderBy(
    col("interaction_score").desc()
).show(20, truncate=False)

spark.stop()