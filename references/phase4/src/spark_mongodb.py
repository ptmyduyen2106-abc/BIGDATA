from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    max as spark_max
)
from pymongo import MongoClient

GOLD_PATH = "hdfs://namenode:8020/ecommerce/delta/gold/interactions"

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "ecommerce"

USER_COLLECTION = "user_features"
ITEM_COLLECTION = "item_features"


def main():
    spark = (
        SparkSession.builder
        .appName("RetailRocket-Gold-To-MongoDB")
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

    print("Gold records:", gold_df.count())

    print("\n=== USER FEATURES ===")

    user_features = (
        gold_df
        .groupBy("visitorid")
        .agg(
            count("*").alias("unique_items"),
            spark_sum("interaction_score").alias("total_interaction_score"),
            spark_sum("interaction_count").alias("total_interactions"),
            avg("interaction_score").alias("avg_interaction_score"),
            spark_max("interaction_score").alias("max_interaction_score")
        )
    )

    user_features.printSchema()
    print("User feature records:", user_features.count())

    print("\n=== ITEM FEATURES ===")

    item_features = (
        gold_df
        .groupBy("itemid")
        .agg(
            count("*").alias("unique_users"),
            spark_sum("interaction_score").alias("total_interaction_score"),
            spark_sum("interaction_count").alias("total_interactions"),
            avg("interaction_score").alias("avg_interaction_score"),
            spark_max("interaction_score").alias("max_interaction_score")
        )
    )

    item_features.printSchema()
    print("Item feature records:", item_features.count())

    print("\n=== WRITE USER FEATURES TO MONGODB ===")

    def write_users_to_mongo(partition):
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DB][USER_COLLECTION]

        documents = []

        for row in partition:
            documents.append({
                "visitorid": int(row["visitorid"]),
                "unique_items": int(row["unique_items"]),
                "total_interaction_score": int(row["total_interaction_score"]),
                "total_interactions": int(row["total_interactions"]),
                "avg_interaction_score": float(row["avg_interaction_score"]),
                "max_interaction_score": int(row["max_interaction_score"])
            })

        if documents:
            collection.insert_many(documents)

        client.close()

    user_features.foreachPartition(write_users_to_mongo)

    print("\n=== WRITE ITEM FEATURES TO MONGODB ===")

    def write_items_to_mongo(partition):
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DB][ITEM_COLLECTION]

        documents = []

        for row in partition:
            documents.append({
                "itemid": int(row["itemid"]),
                "unique_users": int(row["unique_users"]),
                "total_interaction_score": int(row["total_interaction_score"]),
                "total_interactions": int(row["total_interactions"]),
                "avg_interaction_score": float(row["avg_interaction_score"]),
                "max_interaction_score": int(row["max_interaction_score"])
            })

        if documents:
            collection.insert_many(documents)

        client.close()

    item_features.foreachPartition(write_items_to_mongo)

    print("\n=== MONGODB WRITE COMPLETED ===")

    spark.stop()


if __name__ == "__main__":
    main()