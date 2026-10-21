"""Chạy riêng Phase 5 (task 16-18) trên Delta Silver của một run đã có.

Điều kiện: run đó đã hoàn thành Phase 3-4 (có <run_root>/delta/silver, hoặc LAKE_BASE_URI
trỏ tới HDFS). Training từ chối ghi đè model cũ -> dùng --run-id mới cho mỗi lần chạy.

Ví dụ (trong container app):
    python -m phase5.run_phase5 --run-id demo-01 --quick
    python -m phase5.run_phase5 --run-id history-full-01 --full-grid --top-k 10
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Phase 5: ALS pipeline, training and ranking evaluation")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "outputs"))
    ap.add_argument("--master", default="local[2]")
    ap.add_argument("--top-k", type=int, default=10)
    grid = ap.add_mutually_exclusive_group()
    grid.add_argument("--quick", action="store_true", help="lưới tham số nhỏ (mặc định)")
    grid.add_argument("--full-grid", action="store_true", help="lưới đầy đủ 8 ứng viên x 2 fold")
    args = ap.parse_args(argv)

    from phase3.streaming import create_spark
    from pipeline.config import run_path
    from pipeline.train import train_model

    root = run_path(args.run_id, args.output_root)
    spark = create_spark(args.master, enable_delta=True, download_jars=True)
    try:
        report = train_model(spark, str(root), args.run_id, quick=not args.full_grid, top_k=args.top_k)
    finally:
        spark.stop()
    ho = report.get("heldout_test", {})
    print(json.dumps({
        "run_id": args.run_id,
        "selected_parameters": report.get("selected_parameters"),
        f"precision@{args.top_k}": ho.get("precision_at_k"),
        f"recall@{args.top_k}": ho.get("recall_at_k"),
        f"ndcg@{args.top_k}": ho.get("ndcg_at_k"),
        "catalog_coverage": ho.get("catalog_coverage"),
        "user_coverage": ho.get("user_coverage"),
        "cold_users": ho.get("cold_users"),
        "metrics_file": str(Path(root) / "metrics/model_metrics.json"),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
