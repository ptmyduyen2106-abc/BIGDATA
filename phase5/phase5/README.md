# Phase 5 — Tầng ML (Spark MLlib ALS)

Phụ trách: **Phan Thị Mỹ Duyên** — Task 16, 17, 18 của ActionPlan.

| Task | Nội dung | Nơi cài đặt |
|---|---|---|
| 16 | Pipeline MLlib: `VectorAssembler` + `ALS(implicitPrefs=True)` | `pipeline/train.py` (`fit_als_pipeline`) |
| 17 | Huấn luyện offline trên Delta Silver; lưu model, gợi ý Parquet, bundle phục vụ | `pipeline/train.py` (`train_model`) |
| 18 | Đánh giá: 2 fold thời gian + holdout cuối; Precision/Recall/NDCG@K, coverage, cold-start | `pipeline/train.py`, `pipeline/ranking.py` |

Thư mục `phase5/` là điểm vào riêng cho Phase 5; mã lõi vẫn ở `pipeline/` vì `pipeline/runner.py`
import từ đó (không di chuyển để tránh làm hỏng luồng end-to-end).

## Nội dung

- `__init__.py` — re-export các hàm Phase 5
- `run_phase5.py` — chạy riêng Phase 5 trên một run đã có Silver
- `docs/PHASE_5.md` — thiết kế, chia thời gian, định nghĩa metric
- `tests/test_training.py` — kiểm thử (copy của `tests/test_training.py`)
- `example_results/verified-20261021c/` — `model_metrics.json` và model đã lưu của lần chạy mẫu

## Chạy

```
docker compose run --rm --no-deps app python -m phase5.run_phase5 --run-id demo-01 --quick
docker compose run --rm --no-deps app python -m phase5.run_phase5 --run-id demo-01-full --full-grid
python -m unittest discover -s phase5/tests -p test_training.py -v
```

## Kết quả mẫu (lần chạy `verified-20261021c`)

Chỉ 10.000 dòng; chưa huấn luyện trên toàn bộ 2.756.101 dòng. Rank 8, regParam 0.05, alpha 20.
Precision@10 = Recall@10 = NDCG@10 = **0**; 871/912 user đánh giá là cold-start; catalog coverage ≈ 3,5%, user coverage ≈ 4,5%.
Đây là kết quả thật của dữ liệu mẫu rất thưa, không phải metric bị thiếu.
