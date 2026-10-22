"""Phase 5 — Spark MLlib (task 16, 17, 18): Pipeline, huấn luyện offline, đánh giá.

Chạy (ví dụ, dữ liệu đầy đủ từ CSV, Spark local):
    python -m phase5_ml.run_phase5 --source csv --input data/raw/events.csv --run-id p5-full-01

Đọc Silver Delta trên HDFS của nhóm (trong Docker Phase 4):
    python -m phase5_ml.run_phase5 --source delta --input hdfs://namenode:8020/ecommerce/delta/silver/events --run-id p5-delta-01

Quy trình:
  1. Nạp + làm sạch sự kiện (quy tắc Silver Phase 4), dựng Gold, kiểm tra Gold == rating ALS.
  2. Chia theo thời gian TRƯỚC khi gộp user-item: mốc 50/70/85% của timestamp sự kiện.
  3. Chọn tham số ALS implicit bằng 2 fold mở rộng theo thời gian (rolling-origin CV).
  4. Đánh giá held-out (train <= 85%, test > 85%) : ALS vs Popularity vs Hybrid, RMSE phụ trợ.
  5. Fit lại toàn bộ dữ liệu -> model phục vụ, xuất gợi ý mẫu.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from pipeline.ranking import rolling_folds
from pipeline.train import _selection_grid, fit_als_pipeline, prepare_interactions, recommend_unseen
from phase5_ml.data import EVENT_WEIGHTS, build_gold, describe_events, load_events, verify_gold_consistency
from phase5_ml.evaluate import evaluate_window
from phase5_ml.explicit import rmse_window


def utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_spark(master: str, memory: str, use_delta: bool, local_dir: str):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master(master).appName("Phase5-MLlib-ALS")
               .config("spark.driver.memory", memory).config("spark.sql.shuffle.partitions", "16")
               .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false").config("spark.sql.session.timeZone", "UTC")
               .config("spark.local.dir", local_dir).config("spark.driver.maxResultSize", "1g"))
    if use_delta:
        from delta import configure_spark_with_delta_pip
        builder = (builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=["csv", "parquet", "delta"], default="csv")
    parser.add_argument("--input", required=True, help="CSV / thư mục Parquet / đường dẫn Delta Silver")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", default="outputs/phase5")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="5g")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--full-grid", action="store_true", help="8 ứng viên (rank 8/16 x reg x alpha), 10 vòng lặp")
    parser.add_argument("--eval-warm-users", type=int, default=30000)
    parser.add_argument("--eval-cold-users", type=int, default=20000)
    parser.add_argument("--serving-users", type=int, default=5000, help="số user xuất gợi ý mẫu")
    parser.add_argument("--limit-events", type=int, default=0, help="chỉ để thử nhanh: lấy N sự kiện đầu theo thời gian")
    args = parser.parse_args(argv)

    quick = not args.full_grid
    k = args.top_k
    out = Path(args.output_root) / args.run_id
    if out.exists():
        raise FileExistsError(f"Run đã tồn tại: {out}; dùng --run-id mới")
    (out / "metrics").mkdir(parents=True)
    timings: dict[str, float] = {}
    t_start = time.perf_counter()

    def stage(name, started):
        timings[name] = round(time.perf_counter() - started, 1)
        print(f"[{now()}] {name}: {timings[name]}s", flush=True)

    spark = build_spark(args.master, args.driver_memory, args.source == "delta", str(out / "spark-tmp"))
    spark.sparkContext.setLogLevel("ERROR")
    from pyspark.sql import functions as F
    report: dict = {"run_id": args.run_id, "generated_at_utc": now(), "status": "running",
                    "environment": {"spark": spark.version, "master": args.master, "driver_memory": args.driver_memory,
                                    "python_executable_cores": os.cpu_count()},
                    "config": {"top_k": k, "quick_grid": quick, "event_weights": EVENT_WEIGHTS, "seed": 42,
                               "eval_warm_users": args.eval_warm_users, "eval_cold_users": args.eval_cold_users}}
    try:
        # 1. Dữ liệu ---------------------------------------------------------------------------
        t = time.perf_counter()
        events, stats = load_events(spark, args.source, args.input)
        if args.limit_events:
            cutoff = events.orderBy("timestamp").limit(args.limit_events).agg(F.max("timestamp")).first()[0]
            events = events.filter(F.col("timestamp") <= cutoff).cache()
            stats.update(describe_events(events))
            stats["limit_events_applied"] = args.limit_events
            stats["removed_rows"] = stats["raw_rows"] - stats["clean_rows"]
        report["data"] = {**stats, "first_event_utc": utc_iso(stats["first_event_ms"]),
                          "last_event_utc": utc_iso(stats["last_event_ms"])}
        gold = build_gold(events)
        gold_fmt = "delta" if args.source == "delta" else "parquet"
        gold.write.format(gold_fmt).mode("errorifexists").save(str(out / "lake" / "gold_interactions"))
        gold_saved = spark.read.format(gold_fmt).load(str(out / "lake" / "gold_interactions"))
        report["gold"] = {"format": gold_fmt, "path": str(out / "lake" / "gold_interactions"),
                          **verify_gold_consistency(events, gold_saved)}
        if not report["gold"]["consistent"]:
            raise AssertionError("Gold không khớp rating ALS: kiểm tra lại trọng số")
        stage("1_load_clean_gold", t)

        # 2. Chia thời gian --------------------------------------------------------------------
        boundaries = [int(v) for v in events.stat.approxQuantile("timestamp", [0.50, 0.70, 0.85], 0.001)]
        if any(a >= b for a, b in zip(boundaries, boundaries[1:])):
            raise ValueError("Không đủ mốc thời gian phân biệt để chia fold")
        folds = rolling_folds(boundaries)
        b1, b2, b3 = boundaries
        counts = events.agg(
            F.sum(F.when(F.col("timestamp") <= b1, 1).otherwise(0)).alias("p1"),
            F.sum(F.when((F.col("timestamp") > b1) & (F.col("timestamp") <= b2), 1).otherwise(0)).alias("p2"),
            F.sum(F.when((F.col("timestamp") > b2) & (F.col("timestamp") <= b3), 1).otherwise(0)).alias("p3"),
            F.sum(F.when(F.col("timestamp") > b3, 1).otherwise(0)).alias("p4")).first()
        report["split"] = {
            "boundaries_epoch_ms": boundaries, "boundaries_utc": [utc_iso(v) for v in boundaries],
            "events_per_partition": {"train_fold1_<=50%": int(counts["p1"]), "validation_fold1_(50%,70%]": int(counts["p2"]),
                                     "validation_fold2_(70%,85%]": int(counts["p3"]), "test_>85%": int(counts["p4"])},
            "semantics": "chia theo timestamp sự kiện thô trước khi gộp user-item; fold1: train<=50% val(50,70]; "
                         "fold2: train<=70% val(70,85]; held-out: train<=85% test>85%"}

        development = events.filter(F.col("timestamp") <= b3)
        test = events.filter(F.col("timestamp") > b3).cache()
        cv_iter = 3 if quick else 10
        fit_iter = 5 if quick else 10
        fold_data = []
        for fold in folds:
            history_events = events.filter(F.col("timestamp") <= fold["train_end_inclusive"])
            history = prepare_interactions(history_events).cache()
            validation = events.filter((F.col("timestamp") > fold["validation_start_exclusive"])
                                       & (F.col("timestamp") <= fold["validation_end_inclusive"])).cache()
            fold_data.append((history_events, history, validation))

        # 3. Cross-validation theo thời gian ---------------------------------------------------
        t = time.perf_counter()
        candidates = []
        for params in _selection_grid(quick):
            fold_results = []
            for index, ((_, history, validation), fold) in enumerate(zip(fold_data, folds), 1):
                t_fit = time.perf_counter()
                model = fit_als_pipeline(history, params, max_iter=cv_iter)
                fit_seconds = round(time.perf_counter() - t_fit, 1)
                result = evaluate_window(model.stages[-1], history, validation, k=k,
                                         n_warm=args.eval_warm_users, n_cold=args.eval_cold_users)
                result.update({"fold": index, "train_end_inclusive": fold["train_end_inclusive"],
                               "fit_seconds": fit_seconds})
                fold_results.append(result)
                print(f"  CV {params} fold{index}: warm NDCG@{k} ALS={result['als']['warm']['ndcg_at_k']:.4f} "
                      f"POP={result['popularity']['warm']['ndcg_at_k']:.4f} (fit {fit_seconds}s)", flush=True)
                model.stages[-1].userFactors.unpersist()
                model.stages[-1].itemFactors.unpersist()
            mean_ndcg = sum(r["als"]["warm"]["ndcg_at_k"] for r in fold_results) / len(fold_results)
            mean_recall = sum(r["als"]["warm"]["recall_at_k"] for r in fold_results) / len(fold_results)
            mean_prec = sum(r["als"]["warm"]["precision_at_k"] for r in fold_results) / len(fold_results)
            candidates.append({"parameters": params, "folds": fold_results,
                               "mean_warm_ndcg_at_k": mean_ndcg, "mean_warm_recall_at_k": mean_recall,
                               "mean_warm_precision_at_k": mean_prec})
        best = max(candidates, key=lambda c: (c["mean_warm_ndcg_at_k"], c["mean_warm_recall_at_k"]))
        params = best["parameters"]
        report["cross_validation"] = {"selection_metric": "mean_warm_ndcg_at_k (user warm, 2 fold thời gian)",
                                      "candidates": candidates, "selected_parameters": params,
                                      "iterations": cv_iter}
        stage("3_cross_validation", t)

        # 4. Held-out test ---------------------------------------------------------------------
        t = time.perf_counter()
        dev_interactions = prepare_interactions(development).cache()
        t_fit = time.perf_counter()
        eval_model = fit_als_pipeline(dev_interactions, params, max_iter=fit_iter)
        eval_fit_seconds = round(time.perf_counter() - t_fit, 1)
        heldout = evaluate_window(eval_model.stages[-1], dev_interactions, test, k=k,
                                  n_warm=args.eval_warm_users, n_cold=args.eval_cold_users)
        heldout["fit_seconds"] = eval_fit_seconds
        heldout["train_events"] = development.count()
        heldout["test_events"] = test.count()
        eval_model.write().save(str(out / "models" / "evaluation_pipeline"))
        report["heldout_test"] = heldout
        report["heldout_test"]["fit_iterations"] = fit_iter
        stage("4_heldout_test", t)

        # 4b. RMSE phụ trợ (explicit ALS) --------------------------------------------------------
        t = time.perf_counter()
        rmse_iter = 5 if quick else 10
        rmse_grid = []
        for reg in (0.05, 0.1, 0.3):
            per_fold = [rmse_window(h_events, validation, rank=8, reg_param=reg, max_iter=rmse_iter)
                        for h_events, _, validation in fold_data]
            mean_rmse = sum(r["rmse"]["als_clipped_1_5"] for r in per_fold) / len(per_fold)
            rmse_grid.append({"rank": 8, "reg_param": reg, "folds": per_fold, "mean_als_clipped_rmse": mean_rmse})
            print(f"  RMSE reg={reg}: mean fold RMSE={mean_rmse:.4f}", flush=True)
        best_rmse = min(rmse_grid, key=lambda c: c["mean_als_clipped_rmse"])
        rmse_test = rmse_window(development, test, rank=best_rmse["rank"], reg_param=best_rmse["reg_param"],
                                max_iter=rmse_iter)
        report["rmse"] = {"label": "ALS explicit trên mức hành vi mạnh nhất (1/3/5); chỉ cặp mới, user/item đã biết",
                          "grid": rmse_grid, "selected": {"rank": best_rmse["rank"], "reg_param": best_rmse["reg_param"]},
                          "heldout_test": rmse_test}
        stage("4b_rmse", t)
        dev_interactions.unpersist()
        for _, history, validation in fold_data:
            history.unpersist()
            validation.unpersist()

        # 5. Model phục vụ (fit toàn bộ dữ liệu) -------------------------------------------------
        t = time.perf_counter()
        full = prepare_interactions(events).cache()
        t_fit = time.perf_counter()
        serving = fit_als_pipeline(full, params, max_iter=fit_iter)
        serving_fit_seconds = round(time.perf_counter() - t_fit, 1)
        serving.write().save(str(out / "models" / "als_pipeline"))
        als = serving.stages[-1]
        catalog = als.itemFactors.count()
        trained_users = als.userFactors.count()
        warm_ids = als.userFactors.select(F.col("id").alias("visitorid"))
        sample_users = (warm_ids.withColumn("_h", F.xxhash64("visitorid", F.lit(42)))
                        .orderBy("_h").limit(args.serving_users).select("visitorid").cache())
        recs = recommend_unseen(als, sample_users, full, k, catalog).cache()
        recs.write.mode("errorifexists").parquet(str(out / "models" / "recommendations_sample"))
        recs_pd = recs.orderBy("visitorid", "rank").toPandas()
        recs_pd.to_csv(out / "recommendations_top{}_sample.csv".format(k), index=False)
        popular = (full.groupBy("itemid").agg(F.sum("rating").alias("score"))
                   .orderBy(F.desc("score"), F.asc("itemid")).limit(50).toPandas())
        popular.to_csv(out / "popular_items_top50.csv", index=False)
        report["serving_model"] = {
            "model_version": f"{args.run_id}:als-serving-v1", "parameters": params, "fit_iterations": fit_iter,
            "fit_seconds": serving_fit_seconds, "trained_users": trained_users, "catalog_size": catalog,
            "interaction_pairs": full.count(), "sample_users_exported": int(recs_pd["visitorid"].nunique()),
            "note": "Gợi ý xuất cho mẫu user (hash cố định). Chạy toàn bộ user: dùng pipeline/train.py trên cụm/Docker của nhóm."}
        stage("5_serving_model", t)

        report["timings_seconds"] = {**timings, "total": round(time.perf_counter() - t_start, 1)}
        report["status"] = "completed"
    except Exception as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        write_json(out / "metrics" / "phase5_metrics.json", report)
        raise
    finally:
        write_json(out / "metrics" / "phase5_metrics.json", report) if report["status"] == "completed" else None
        spark.stop()
    return report


if __name__ == "__main__":
    main()
