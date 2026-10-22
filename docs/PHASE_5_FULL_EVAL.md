# Phase 5 – Đánh giá đầy đủ trên toàn bộ dữ liệu (task 16, 17, 18)

Phụ trách: Phan Thị Mỹ Duyên. Gói `phase5_ml/` chạy **ALS implicit trên toàn bộ 2.755.641 sự kiện RetailRocket** (sau bỏ 460 dòng trùng), chia theo thời gian, chọn tham số bằng cross-validation, so với baseline và xuất báo cáo. Mã lõi (`fit_als_pipeline`, `prepare_interactions`, `recommend_unseen`) vẫn là `pipeline/train.py` của nhóm; `phase5_ml/` chỉ thêm phần đọc dữ liệu, baseline, RMSE, chạy toàn bộ và báo cáo. Không sửa file có sẵn. (Thư mục `phase5/phase5/` có sẵn là điểm vào mỏng, chỉ có kết quả mẫu 10.000 dòng.)

## Kết quả (`phase5_results/p5-full-grid/`, lưới đầy đủ 8 cấu hình × 2 fold, 10 vòng lặp)

| | ALS | Popularity | Hybrid (ALS cho warm + Popularity cho cold) |
|---|---|---|---|
| NDCG@10 – user có lịch sử (warm, 18.055 user) | **0,0090** | 0,0024 | 0,0090 |
| Precision@10 – warm | 0,0027 | 0,0005 | 0,0027 |
| NDCG@10 – toàn bộ user mục tiêu (gồm cold) | 0,0007 | 0,0042 | **0,0047** |

- Cấu hình chọn: rank 16, regParam 0,1, alpha 20 (rank 16 tốt hơn rank 8 khoảng 61%; rank 16 là biên lưới nên chưa chắc tối ưu).
- 92,2% user ở giai đoạn test là cold (chưa có lịch sử) → ALS không gợi ý được; Hybrid chỉ hơn Popularity 12,3% trên toàn bộ user.
- RMSE phụ trợ (ALS explicit trên mức hành vi 1/3/5): 0,666 so với baseline trung bình 0,664 → không vượt baseline; không dùng để chọn mô hình.
- Bản chạy nhanh (2 cấu hình, 3 vòng lặp): `phase5_results/p5-full-quick/` – NDCG warm 0,0042, kém rõ rệt bản đầy đủ.
- Báo cáo Word: `docs/BAO_CAO_PHASE5_MLLIB.docx`.

## Chạy lại (từ thư mục gốc repo, cần Java + PySpark 3.5.6)

```bash
python -m unittest discover -s tests -p test_phase5_ml.py -v                        # 6 test Spark, đáp án tính tay
python -m phase5_ml.run_phase5 --source csv --input data/raw/events.csv --run-id p5-01              # nhanh (~17 phút, 2 nhân)
python -m phase5_ml.run_phase5 --source csv --input data/raw/events.csv --run-id p5-02 --full-grid  # đầy đủ (~60 phút)
python -m phase5_ml.run_phase5 --source delta --input hdfs://namenode:8020/ecommerce/delta/silver/events --run-id p5-delta-01
python -m phase5_ml.charts outputs/phase5/p5-02/metrics/phase5_metrics.json outputs/phase5/p5-02/charts
python -m phase5_ml.build_report --metrics outputs/phase5/p5-02/metrics/phase5_metrics.json --charts outputs/phase5/p5-02/charts --out docs/BAO_CAO_PHASE5_MLLIB.docx
```

## Phương pháp & giới hạn

- Chia theo timestamp **trước khi gộp user–item** (mốc 50/70/85%); fold 1: train ≤50% → val (50,70]; fold 2: train ≤70% → val (70,85]; held-out: train ≤85% → test >85%.
- Mục tiêu là cặp (user, item) mới; item đã xem bị loại khỏi gợi ý; cold user không bị loại khỏi phép đo (ALS trượt).
- Lấy mẫu phân tầng warm/cold có seed cố định, gia quyền lại theo quần thể; warm được đánh giá gần như toàn bộ.
- Chọn tham số theo NDCG@10 trung bình của user warm trên hai fold.
- **Delta Lake chưa chạy trực tiếp:** lần chạy này đọc Parquet cùng schema Silver/Gold vì môi trường chạy không tải được JAR Delta. `--source delta` đã viết nhưng chưa chạy trên HDFS của nhóm.
- Model (~200 MB) và Gold Parquet không đưa lên repo; chạy lại để tạo. Gợi ý top-10 chỉ xuất cho mẫu 5.000 user (`recommendations_top10_sample.csv`).
- Chỉ số offline không chứng minh tăng doanh thu hay CTR; dataset không có giá, impression, review.
