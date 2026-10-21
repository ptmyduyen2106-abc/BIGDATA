# Đối chiếu ActionPlan: Phase 1–7, task 1–25

Tài liệu này ánh xạ yêu cầu của ActionPlan sang **mã nguồn trong gói thống nhất**.
Việc có mã không đồng nghĩa đã chạy thành công. Kết quả thực tế phải đối chiếu
`outputs/evidence/` và `outputs/runs/<run_id>/metrics/` của lần chạy tương ứng.
Task 26–29 của Phase 8 (báo cáo Word, slide, video, trình bày) nằm ngoài phạm vi
Phase 1–7, không được tính là đã hoàn thành bởi bảng dưới đây.

## Nguồn và phần công việc kế thừa

- ActionPlan: <https://docs.google.com/spreadsheets/d/1kW7TqTzqVtJa9I6Z393JGhL84YijK8u0/edit>.
- Mô tả dự án: <https://docs.google.com/document/d/17BBAhojY4miAK4f2FCxBk3AWOa20MA5l/edit>.
- `references/phase1/`: tài liệu, script và cấu hình Phase 1 của bạn trong nhóm.
- `references/phase4/`: script và cấu hình Phase 4 của bạn trong nhóm.

Các thư mục `references` giữ nguồn tham khảo để đối chiếu đóng góp; Compose chính
không gọi Compose cũ. Luồng tích hợp dùng `compose.yaml`, `pipeline/`, `phase2/`,
`phase3/`, `service/` và `orchestration/`. Phần lưu trữ thống nhất đã được viết
lại để đọc **clean của Phase 3**, dùng namespace theo run và upsert NoSQL thay cho
cầu nối CSV thủ công và `insert_many` lặp bản ghi của script Phase 4 tham khảo.

## Bảng ánh xạ đầy đủ

| Task | Yêu cầu ActionPlan | Tệp/thành phần hiện hành | Đầu ra hoặc cách kiểm tra |
| --- | --- | --- | --- |
| 1 | Lập kế hoạch, phân công | ActionPlan gốc; tài liệu này | Nhóm tiếp tục quản lý owner/deadline trên ActionPlan; gói không tự thay phân công |
| 2 | Nghiên cứu pipeline Kafka–Spark–Delta–MLlib | `docs/ARCHITECTURE.md`, `docs/PHASE_3_4.md`, `docs/PHASE_5.md`, `docs/PHASE_6.md` | Giải thích đường đi dữ liệu, hợp đồng và giới hạn |
| 3 | Chọn, thu thập bộ dữ liệu | `data/raw/events.csv`, `data/raw/category_tree.csv`, `data/metadata/`; script nguồn tại `references/phase1/scripts/` | Manifest nguồn/checksum; RetailRocket đầy đủ 2.756.101 dòng, mẫu `data/sample/events_10000.csv` có 10.000 dòng |
| 4 | Thiết kế kiến trúc 5 tầng | `docs/ARCHITECTURE.md`, `compose.yaml` | Ingestion → xử lý → lưu trữ → ML → phục vụ, điều phối tách riêng |
| 5 | Setup Python, Spark, Kafka bằng Docker | `Dockerfile`, `requirements*.txt`, `compose.yaml` | Image `retailrocket-phase17:1.0`; Java 17/Python 3.11/PySpark 3.5.6/Delta 3.3.3 |
| 6 | Kafka Broker Docker | Service `kafka` trong `compose.yaml` | Kafka KRaft một broker; healthcheck của broker |
| 7 | Tạo bốn topic | `pipeline/topics.py`, `phase2/contract.py` | `page_view`, `add_to_cart`, `purchase`, `review`; 3 partition/topic, replication 1 |
| 8 | Producer mô phỏng clickstream | `phase2/producer.py`, `phase2/contract.py` | JSON có schema/event ID/run ID, key visitor; `metrics/producer.json` |
| 9 | Test gửi/nhận Kafka CLI | `phase2/scripts/kafka_cli_smoke.sh`; xác minh hợp đồng bằng `phase2/verify.py` | CLI roundtrip trên topic tạm riêng; runner ghi `metrics/kafka_verify.json` |
| 10 | Spark Structured Streaming đọc Kafka | `phase3/streaming.py`, điều phối bởi `pipeline/runner.py` | Quarantine/clean Parquet, checkpoint và progress thật |
| 11 | Feature theo session/window | `phase3/transforms.py`, `phase3/snapshot.py` | Window 5 phút trượt 1 phút; session gap 30 phút; bảng streaming và snapshot hữu hạn tách biệt |
| 12 | Watermark, dữ liệu trễ, dedup | `phase3/transforms.py`, `phase3/schema.py`, kiểm thử `test_phase3_streaming.py` | Watermark 10 phút; dedup event ID; metric dropped-by-watermark, checkpoint restart |
| 13 | Delta Lake lưu lịch sử batch | `pipeline/storage.py`, `pipeline/config.py` | HDFS Bronze insert-only MERGE; Silver và Gold snapshot xác định theo run |
| 14 | Redis/MongoDB feature và session real-time | `pipeline/online.py`, `pipeline/streaming.py` | Mongo events/user/item/session upsert; Redis seen/features/window/session; projection provisional và session đã chốt phân biệt |
| 15 | Kiểm tra ACID, time travel | `pipeline/acid_check.py` | `metrics/acid_check.json`: update, version lịch sử, CHECK constraint từ chối cả append, không sửa bảng nghiệp vụ |
| 16 | MLlib Pipeline với VectorAssembler và ALS | `pipeline/train.py` | Pipeline có assembler các count và ALS implicit; vector phụ không phải đầu vào của ALS |
| 17 | Huấn luyện offline trên Delta | `pipeline/train.py`, gọi bởi `pipeline/runner.py` | Đọc Silver; lưu `models/evaluation_pipeline`, `models/als_pipeline`, recommendations Parquet và bundle |
| 18 | Đánh giá và cross-validation | `pipeline/train.py`, `pipeline/ranking.py`, `tests/test_training.py` | Hai fold thời gian, holdout cuối; Precision/Recall/NDCG@K, coverage và cold-start; `metrics/model_metrics.json` |
| 19 | Recommendation API | `service/app.py`, `service/store.py`, `service/backends.py` | FastAPI; gợi ý lọc seen, fallback, cache theo version; `/docs`, live/ready health |
| 20 | Dashboard KPI | `service/static/`, `GET /api/metrics` | Tổng event/user/item, phân bố, conversion theo visitor, model metrics và độ trễ HTTP đo được; CTR/doanh thu để thiếu dữ liệu |
| 21 | Airflow huấn luyện lại định kỳ | `orchestration/dags/retrain_recommendations.py`, `pipeline/job_server.py` | DAG hàng ngày 02:00 Asia/Ho_Chi_Minh gọi typed worker; refit từ clean của run được chọn, không tự thu thập dữ liệu mới |
| 22 | Tích hợp end-to-end | `pipeline/runner.py`, `scripts/run_demo.ps1`, `compose.yaml` | Kafka → Spark → Delta/HDFS và NoSQL → ALS → model registry → API/dashboard |
| 23 | Kiểm thử chức năng toàn luồng | `scripts/verify_e2e.py`, `tests/`, `scripts/run_tests.ps1` | `metrics/e2e_verification.json`; kiểm tra cùng run/model, store thật và không gợi ý seen |
| 24 | Kiểm thử độ trễ, thông lượng | `scripts/benchmark.py`; progress Spark/producer trong metrics | `metrics/benchmark.json` đo HTTP mean/p50/p95/p99 và requests/s; số đo có scope và môi trường |
| 25 | Sửa lỗi, tối ưu | Các kiểm thử hồi quy; checkpoint, upsert, temporal CV, bộ xuất recommendation có giới hạn | Đọc evidence của lần chạy; không suy ra SLA/khả năng chạy cloud từ test demo |

## Các lệnh vào đúng luồng thống nhất

Chạy từ thư mục có `compose.yaml`. Cần Docker Desktop Linux engine, image đã
build và các dịch vụ nền healthy. Run ID mới cho mỗi lần chạy độc lập:

```powershell
# Build, khởi động nền, chạy mẫu, khởi động API và xác minh E2E.
./scripts/run_demo.ps1 -RunId demo-01

# Cùng quy trình với grid chọn tham số lớn hơn.
./scripts/run_demo.ps1 -RunId demo-full-grid-01 -FullGrid

# Kiểm thử; -Integration bật thêm kiểm thử Delta/ML Spark opt-in.
./scripts/run_tests.ps1
./scripts/run_tests.ps1 -Integration
```

Để tách thao tác sau khi image và nền đã sẵn sàng:

```powershell
docker compose run --rm --no-deps app python -m pipeline.topics
docker compose run --rm --no-deps app python -m pipeline.runner demo --run-id demo-02
docker compose up -d --no-deps api
docker compose run --rm --no-deps app python scripts/verify_e2e.py --base-url http://api:8000 --run-id demo-02
docker compose run --rm --no-deps app python scripts/benchmark.py --base-url http://api:8000 --run-id demo-02 --requests 100 --concurrency 4
```

CLI smoke task 9 dùng Bash trên **máy có Docker CLI**, tại thư mục dự án (ví dụ
Git Bash trên Windows), không chạy trong app container không có Docker CLI:

```bash
bash phase2/scripts/kafka_cli_smoke.sh
```

Đầu vào đầy đủ là dữ liệu lịch sử chưa bảo đảm thứ tự thời gian toàn cục. Trước
khi replay, sort bằng `scripts/sort_events.py`; runner từ chối input không tăng
theo timestamp. Chạy mẫu 10.000 không được báo là đã huấn luyện 2.756.101 dòng.

```powershell
docker compose run --rm --no-deps app python scripts/sort_events.py --input data/raw/events.csv --output data/raw/events_sorted.csv
docker compose run --rm --no-deps app python -m pipeline.runner demo --run-id full-history-01 --input data/raw/events_sorted.csv
```

Luồng liên tục task 10–14 và huấn luyện lại task 21 là thao tác riêng:

```powershell
docker compose run --rm --no-deps app python -m pipeline.streaming --run-id live-01 --master 'local[2]' --processing-interval '5 seconds'
docker compose run --rm --no-deps app python -m phase2.producer --input data/sample/events_10000.csv --run-id live-01 --rate 100
docker compose run --rm --no-deps app python -m pipeline.runner retrain --run-id retrain-01 --source-run-id demo-02
docker compose --profile orchestration up -d job-worker airflow
```

Producer và live consumer chạy ở hai terminal; tạo topic trước và dùng cùng run
ID khi lọc theo run. Dừng live writer trước khi đọc một snapshot để retrain.
`retrain` không chạy lại producer: nó lấy clean đã commit của source run, tạo
run mới và chỉ đổi latest pointer khi toàn bộ bước bắt buộc hoàn tất.

## Cách diễn giải phạm vi đáp ứng

Task 18 cho lựa chọn RMSE/Precision@K: hiện thực dùng ranking metrics phù hợp
implicit feedback, không tạo rating tường minh để có RMSE. Task 20 nêu CTR và
doanh thu, nhưng dữ liệu thiếu impressions/giá/giá trị đơn; gói giữ các chỉ số
này là `null` và nêu lý do. `category_tree.csv` chỉ chứa cây danh mục, không có
ánh xạ item→category; không tuyên bố đã huấn luyện đặc trưng danh mục.

Phase 4 gốc chưa có session real-time hoàn chỉnh theo chính ghi chú ActionPlan.
Hiện thực thống nhất thêm live projection/upsert có checkpoint, đồng thời vẫn
giữ giới hạn provisional session, watermark và tính nhất quán từng kho. Các
kiểm thử/benchmark cần được thực thi và đọc kết quả thực tế trước khi điền
“Done” hoặc phần trăm tiến độ trong ActionPlan.

