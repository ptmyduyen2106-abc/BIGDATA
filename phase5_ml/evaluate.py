"""Đánh giá xếp hạng Top-K: ALS vs Popularity vs Hybrid, tách user warm/cold.

Định nghĩa (khớp pipeline/train.py của nhóm):
  - Mục tiêu = cặp (user, item) MỚI trong cửa sổ held-out (cặp đã có trong train bị loại).
  - Precision@K = hits / K ; Recall@K = hits / số item mới của user ; NDCG@K nhị phân.
  - Item đã xem trong train bị loại khỏi gợi ý (đúng hành vi API phục vụ).
  - Trung bình macro theo user. Cold user (không có lịch sử train) KHÔNG bị bỏ: ALS không gợi ý được -> miss.
Lấy mẫu phân tầng (warm / cold) có cố định hash để chạy được trên máy 2 nhân; số liệu tổng hợp
được gia quyền lại theo quy mô quần thể thật của từng tầng.
"""
from __future__ import annotations

from pipeline.train import recommend_unseen


def novel_targets(history, window_events):
    """Cặp (visitorid, itemid) xuất hiện trong cửa sổ nhưng chưa có trong lịch sử train."""
    from pyspark.sql import functions as F
    pairs = window_events.select(F.col("visitorid").cast("int").alias("visitorid"),
                                 F.col("itemid").cast("int").alias("itemid")).distinct()
    seen = history.select("visitorid", "itemid").distinct()
    return pairs.join(seen, ["visitorid", "itemid"], "left_anti")


def stratified_users(eligible, warm_ids, n_warm: int, n_cold: int, seed: int = 42):
    """Lấy mẫu xác định (hash) theo tầng warm/cold. Trả về (users_df[visitorid,is_warm], population)."""
    from pyspark.sql import functions as F, Window
    flagged = (eligible.join(warm_ids.withColumn("is_warm", F.lit(True)), "visitorid", "left")
               .fillna({"is_warm": False}).cache())
    population = {row["is_warm"]: row["count"] for row in flagged.groupBy("is_warm").count().collect()}
    order = Window.partitionBy("is_warm").orderBy(F.xxhash64("visitorid", F.lit(seed)))
    ranked = flagged.withColumn("_rn", F.row_number().over(order))
    sample = ranked.filter((F.col("is_warm") & (F.col("_rn") <= n_warm))
                           | (~F.col("is_warm") & (F.col("_rn") <= n_cold))).select("visitorid", "is_warm").cache()
    return sample, {"warm": int(population.get(True, 0)), "cold": int(population.get(False, 0))}


def popularity_recommendations(history, users, k: int, pool: int = 500):
    """Top-K phổ biến nhất (tổng rating toàn lịch sử train), loại item user đã xem."""
    from pyspark.sql import functions as F, Window
    popular = (history.groupBy("itemid").agg(F.sum("rating").alias("score"))
               .orderBy(F.desc("score"), F.asc("itemid")).limit(pool))
    seen = history.select("visitorid", "itemid").distinct()
    candidates = (users.select("visitorid").distinct().crossJoin(F.broadcast(popular))
                  .join(seen, ["visitorid", "itemid"], "left_anti"))
    order = Window.partitionBy("visitorid").orderBy(F.desc("score"), F.asc("itemid"))
    return (candidates.withColumn("rank", F.row_number().over(order)).filter(F.col("rank") <= k)
            .select("visitorid", "itemid", "score", "rank"))


def score_users(targets, predictions, k: int):
    """Precision/Recall/NDCG@K từng user (user không có dự đoán -> 0)."""
    from pyspark.sql import functions as F
    actual = targets.groupBy("visitorid").agg(F.collect_set("itemid").alias("actual"))
    predicted = predictions.groupBy("visitorid").agg(
        F.sort_array(F.collect_list(F.struct("rank", "itemid"))).alias("ordered"))
    return (actual.join(predicted, "visitorid", "left")
            .withColumn("predicted", F.coalesce(F.expr("transform(ordered, x -> x.itemid)"),
                                                F.array().cast("array<int>")))
            .withColumn("hits", F.size(F.array_intersect("actual", "predicted")))
            .withColumn("precision", F.col("hits") / F.lit(float(k)))
            .withColumn("recall", F.col("hits") / F.size("actual"))
            .withColumn("dcg", F.expr("""CASE WHEN size(predicted) = 0 THEN 0D ELSE
                aggregate(transform(sequence(1,size(predicted)), i ->
                  IF(array_contains(actual,element_at(predicted,i)),1D/log2(i+1D),0D)),
                  0D,(acc,x)->acc+x) END"""))
            .withColumn("ideal", F.expr(f"""aggregate(transform(sequence(1,least({k},size(actual))),
                i -> 1D/log2(i+1D)),0D,(acc,x)->acc+x)"""))
            .withColumn("ndcg", F.col("dcg") / F.col("ideal"))
            .select("visitorid", "hits", "precision", "recall", "ndcg"))


def summarize(scored, users, population: dict) -> dict:
    """Macro theo user trong từng tầng; 'overall' gia quyền theo quy mô quần thể thật."""
    from pyspark.sql import functions as F
    joined = scored.join(users, "visitorid", "inner")
    rows = {r["is_warm"]: r for r in joined.groupBy("is_warm").agg(
        F.avg("precision").alias("p"), F.avg("recall").alias("r"), F.avg("ndcg").alias("n"),
        F.avg(F.when(F.col("hits") > 0, 1.0).otherwise(0.0)).alias("hit"),
        F.count("*").alias("count")).collect()}

    def tier(flag):
        row = rows.get(flag)
        if row is None:
            return {"users_evaluated": 0, "precision_at_k": None, "recall_at_k": None,
                    "ndcg_at_k": None, "hit_rate_at_k": None}
        return {"users_evaluated": int(row["count"]), "precision_at_k": float(row["p"]),
                "recall_at_k": float(row["r"]), "ndcg_at_k": float(row["n"]), "hit_rate_at_k": float(row["hit"])}

    warm, cold = tier(True), tier(False)
    total = population["warm"] + population["cold"]
    overall = {}
    for key in ("precision_at_k", "recall_at_k", "ndcg_at_k", "hit_rate_at_k"):
        parts = []
        for tier_metrics, size in ((warm, population["warm"]), (cold, population["cold"])):
            if size and tier_metrics[key] is None:
                parts = None
                break
            parts.append((tier_metrics[key] or 0.0) * size)
        overall[key] = (sum(parts) / total) if parts is not None and total else None
    return {"warm": warm, "cold": cold, "overall_weighted": overall}


def evaluate_window(als_model, history, window_events, *, k: int = 10, n_warm: int = 30000,
                    n_cold: int = 20000, seed: int = 42) -> dict:
    """So sánh ALS / Popularity / Hybrid trên một cửa sổ thời gian held-out."""
    from pyspark.sql import functions as F
    targets = novel_targets(history, window_events).cache()
    try:
        all_pairs = window_events.select("visitorid", "itemid").distinct().count()
        novel_pairs = targets.count()
        eligible = targets.select("visitorid").distinct()
        warm_ids = history.select("visitorid").distinct()
        users, population = stratified_users(eligible, warm_ids, n_warm, n_cold, seed)
        sample_ids = users.select("visitorid")
        catalog = als_model.itemFactors.count()
        als_preds = recommend_unseen(als_model, sample_ids, history, k, catalog).cache()
        pop_preds = popularity_recommendations(history, sample_ids, k).cache()
        cold_pop = pop_preds.join(warm_ids, "visitorid", "left_anti")
        hybrid_preds = als_preds.select("visitorid", "itemid", "score", "rank").unionByName(cold_pop).cache()
        result = {
            "k": k, "heldout_unique_pairs": all_pairs, "novel_pairs": novel_pairs,
            "repeat_pairs_excluded": all_pairs - novel_pairs,
            "eligible_users": population["warm"] + population["cold"],
            "eligible_warm_users": population["warm"], "eligible_cold_users": population["cold"],
            "cold_user_share": population["cold"] / max(1, population["warm"] + population["cold"]),
            "train_users": warm_ids.count(), "train_catalog_size": catalog,
            "sample": {"warm_users": n_warm, "cold_users": n_cold, "seed": seed,
                       "method": "xxhash64(visitorid, seed) trong từng tầng; gia quyền lại theo quần thể"},
        }
        for name, preds in (("als", als_preds), ("popularity", pop_preds), ("hybrid_als_popularity", hybrid_preds)):
            result[name] = summarize(score_users(targets, preds, k), users, population)
        sampled_warm = max(1, result["als"]["warm"]["users_evaluated"])
        result["als"]["catalog_coverage_warm_sample"] = (
            als_preds.select("itemid").distinct().count() / catalog if catalog else 0.0)
        result["als"]["users_with_recommendations_warm_sample"] = als_preds.select("visitorid").distinct().count() / sampled_warm
        for frame in (als_preds, pop_preds, hybrid_preds, users):
            frame.unpersist()
        return result
    finally:
        targets.unpersist()
