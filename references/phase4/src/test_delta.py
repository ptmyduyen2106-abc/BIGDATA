from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("DeltaACIDTimeTravel")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = builder.getOrCreate()

delta_path = "hdfs://namenode:8020/ecommerce/delta/acid_test"

# ============================================================
# 1. CREATE INITIAL TABLE
# ============================================================

print("\n=== 1. CREATE INITIAL TABLE ===")

initial_data = [
    (1, "view", 1001),
    (2, "addtocart", 1002),
    (3, "transaction", 1003)
]

initial_df = spark.createDataFrame(
    initial_data,
    ["visitorid", "event", "itemid"]
)

(
    initial_df.write
    .format("delta")
    .mode("overwrite")
    .save(delta_path)
)

initial_df.show()

print("Version 0 count:", initial_df.count())


# ============================================================
# 2. APPEND
# ============================================================

print("\n=== 2. APPEND NEW RECORD ===")

new_data = [
    (4, "view", 1004)
]

new_df = spark.createDataFrame(
    new_data,
    ["visitorid", "event", "itemid"]
)

(
    new_df.write
    .format("delta")
    .mode("append")
    .save(delta_path)
)

current_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

current_df.show()

print("Version 1 count:", current_df.count())


# ============================================================
# 3. UPDATE
# ============================================================

print("\n=== 3. UPDATE RECORD ===")

spark.sql(f"""
    UPDATE delta.`{delta_path}`
    SET event = 'transaction'
    WHERE visitorid = 4
""")

current_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

current_df.show()

print("After UPDATE count:", current_df.count())


# ============================================================
# 4. DELETE
# ============================================================

print("\n=== 4. DELETE RECORD ===")

spark.sql(f"""
    DELETE FROM delta.`{delta_path}`
    WHERE visitorid = 2
""")

current_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

current_df.show()

print("After DELETE count:", current_df.count())


# ============================================================
# 5. TIME TRAVEL
# ============================================================

print("\n=== 5. TIME TRAVEL ===")

print("\n--- VERSION 0 ---")

version_0_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(delta_path)
)

version_0_df.show()
print("Version 0 count:", version_0_df.count())


print("\n--- VERSION 1 ---")

version_1_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(delta_path)
)

version_1_df.show()
print("Version 1 count:", version_1_df.count())


print("\n--- VERSION 2 ---")

version_2_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 2)
    .load(delta_path)
)

version_2_df.show()
print("Version 2 count:", version_2_df.count())


print("\n--- CURRENT VERSION ---")

current_df.show()
print("Current count:", current_df.count())


# ============================================================
# 6. DELTA HISTORY
# ============================================================

print("\n=== 6. DELTA HISTORY ===")

history = spark.sql(
    f"DESCRIBE HISTORY delta.`{delta_path}`"
)

history.select(
    "version",
    "timestamp",
    "operation"
).show(truncate=False)


spark.stop()