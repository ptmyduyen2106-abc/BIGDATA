"""RMSE theo yêu cầu ActionPlan (task 18) — mô hình ALS *explicit* trên mức hành vi mạnh nhất.

Lưu ý phương pháp: pipeline gợi ý chính dùng ALS implicit (xếp hạng, đo bằng Precision/Recall/NDCG).
RMSE chỉ có nghĩa trên nhãn số. Ở đây nhãn = mức hành vi mạnh nhất của cặp (user, item):
view=1, addtocart=3, transaction=5. Mô hình explicit chỉ dùng để báo RMSE phụ trợ, có baseline
(trung bình toàn cục, trung bình theo item) trên CÙNG tập cặp để so sánh công bằng.
Chỉ đánh giá cặp MỚI trong cửa sổ held-out và user/item đã có trong train (coldStartStrategy=drop);
tỉ lệ phủ được báo cáo, không giấu.
"""
from __future__ import annotations

LEVELS = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}


def explicit_levels(events):
    from pyspark.sql import functions as F
    level = (F.when(F.col("event") == "view", 1.0).when(F.col("event") == "addtocart", 3.0)
             .when(F.col("event") == "transaction", 5.0))
    return (events.filter(F.col("event").isin(*LEVELS)).groupBy("visitorid", "itemid")
            .agg(F.max(level).alias("level"))
            .withColumn("visitorid", F.col("visitorid").cast("int"))
            .withColumn("itemid", F.col("itemid").cast("int")))


def rmse_window(train_events, window_events, *, rank: int = 8, reg_param: float = 0.1,
                max_iter: int = 5, seed: int = 42) -> dict:
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import functions as F
    train = explicit_levels(train_events).cache()
    test = explicit_levels(window_events).join(train.select("visitorid", "itemid"),
                                               ["visitorid", "itemid"], "left_anti").cache()
    try:
        test_pairs = test.count()
        if not test_pairs:
            return {"status": "not_evaluable_no_novel_pairs"}
        global_mean = float(train.agg(F.avg("level")).first()[0])
        item_mean = train.groupBy("itemid").agg(F.avg("level").alias("item_mean"))
        als = ALS(userCol="visitorid", itemCol="itemid", ratingCol="level", implicitPrefs=False,
                  rank=rank, regParam=reg_param, maxIter=max_iter, seed=seed, coldStartStrategy="drop",
                  numUserBlocks=2, numItemBlocks=2, intermediateStorageLevel="MEMORY_AND_DISK",
                  finalStorageLevel="MEMORY_AND_DISK")
        model = als.fit(train)
        scored = (model.transform(test).join(item_mean, "itemid", "left")
                  .withColumn("als_raw", F.col("prediction"))
                  .withColumn("als_clipped", F.least(F.lit(5.0), F.greatest(F.lit(1.0), F.col("prediction"))))
                  .withColumn("global_mean", F.lit(global_mean))
                  .withColumn("item_mean", F.coalesce("item_mean", F.lit(global_mean))))
        row = scored.agg(
            F.count("*").alias("n"),
            F.sqrt(F.avg(F.pow(F.col("level") - F.col("als_raw"), 2))).alias("als_raw"),
            F.sqrt(F.avg(F.pow(F.col("level") - F.col("als_clipped"), 2))).alias("als_clipped"),
            F.sqrt(F.avg(F.pow(F.col("level") - F.col("global_mean"), 2))).alias("global_mean"),
            F.sqrt(F.avg(F.pow(F.col("level") - F.col("item_mean"), 2))).alias("item_mean"),
            F.avg("level").alias("mean_level")).first()
        n = int(row["n"])
        return {"status": "evaluated", "rank": rank, "reg_param": reg_param, "max_iter": max_iter,
                "novel_test_pairs": test_pairs, "scored_pairs": n, "coverage": n / test_pairs,
                "train_pairs": train.count(), "train_global_mean_level": global_mean,
                "test_mean_level": float(row["mean_level"]),
                "rmse": {"als_clipped_1_5": float(row["als_clipped"]), "als_raw": float(row["als_raw"]),
                         "baseline_global_mean": float(row["global_mean"]),
                         "baseline_item_mean": float(row["item_mean"])}}
    finally:
        train.unpersist()
        test.unpersist()
