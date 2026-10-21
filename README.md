# Dự án Big Data RetailRocket từ Phase 1 đến Phase 7

Một dự án Docker thống nhất cho dữ liệu thương mại điện tử: Kafka → Spark Structured Streaming → Delta Lake trên HDFS cùng MongoDB/Redis → Spark MLlib ALS → FastAPI và dashboard. Airflow điều phối đánh giá, huấn luyện lại và kích hoạt mô hình mới. ZIP chứa cả dữ liệu gốc 2.756.101 dòng, mẫu 10.000 dòng, mã nguồn, cấu hình, kiểm thử, kết quả chạy mẫu và tài liệu phân tích.

Model và metric đã ghi nhận được giữ trong `example_results/<RunId>/`; log và báo cáo xác minh thực tế nằm trong `outputs/evidence/`. Đây là bằng chứng tham khảo của lần chạy mẫu. Gói bàn giao có vùng chạy mới: không kèm checkpoint đang hoạt động, `outputs/latest_run.json` hay volume HDFS/Kafka/MongoDB/Redis/Airflow. Sau giải nén, chạy demo để build image và thực hiện lại toàn bộ pipeline từ dữ liệu nguồn; API chỉ sẵn sàng sau khi run mới xuất và kích hoạt bundle thành công. Script không dùng model mẫu để bỏ qua huấn luyện.

## Chạy nhanh trên Windows

Giải nén toàn bộ vào đường dẫn ngắn, ví dụ `C:\BigData\BigData_Phase_1_7`, để tránh giới hạn độ dài tên file Windows với các thư mục model Spark. Mở Docker Desktop, chờ Linux engine hoạt động, rồi mở PowerShell tại thư mục có `compose.yaml`:

```powershell
docker version
./scripts/run_demo.ps1
```

Script build image, khởi động hạ tầng, tạo topic, replay mẫu thật, đối chiếu consumer, xử lý streaming, ghi HDFS/NoSQL, chọn tham số ALS bằng hai fold thời gian, đánh giá tập test, fit mô hình serving, kiểm tra ACID/time travel, khởi động API và xác minh toàn luồng. Dừng tại lỗi và đọc log thay vì tiếp tục bước phụ thuộc. Lần đầu cần Internet cho image, pip và Maven; có thể mất nhiều phút. Cần đủ bộ nhớ cho Kafka, HDFS, Spark và MongoDB cùng chạy; nên cấp khoảng 8 GB cho Docker khi chạy toàn bộ demo và Airflow.

Các wheel NumPy/SciPy đóng kèm dành cho Linux x86-64/Python 3.11 trong container. Dockerfile cài từ `vendor_wheels/` khi có wheel; khi thư mục này không có wheel thì tải đúng phiên bản trong `requirements-ml.txt` từ pip registry. Các phần còn lại của lần build đầu vẫn cần Internet; gói này không phải bộ cài offline hoàn toàn.

Khi hoàn tất:

- Dashboard và API: **http://localhost:8000/**
- OpenAPI: **http://localhost:8000/docs**
- HDFS UI: **http://localhost:19870/**
- Kafka từ host: `localhost:19092`; các container dùng `kafka:29092`.

Không cần tự cài Java/Spark trên Windows, không cần giải nén thêm ZIP Phase 1/4, và không cần dừng Kafka cũ đang dùng cổng 9092. Stack có project name `retailrocket17` cùng volume riêng. Nếu cổng API 8000 đã được dùng, copy `.env.example` thành `.env` và đổi `APP_PORT`.

Nếu PowerShell chặn script đã tải, đọc script trước và dùng `Unblock-File -LiteralPath .\scripts\run_demo.ps1` cho đúng tệp tin cậy; hoặc thực hiện các lệnh Docker tương ứng dưới đây theo policy máy.

Có thể chạy script đã kiểm tra trong một tiến trình PowerShell riêng, không đổi execution policy toàn máy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

Tùy chọn chỉ áp dụng cho tiến trình này; nếu máy do tổ chức quản lý chặn bằng Group Policy, dùng các lệnh Docker thủ công theo quy định của máy.

## Thực hiện từng bước

```powershell
docker build -t retailrocket-phase17:1.0 .
docker compose up -d --wait kafka namenode datanode mongodb redis
docker compose run --rm --no-deps app python -m pipeline.dataset
docker compose run --rm --no-deps app python -m pipeline.runner demo --run-id demo-01
docker compose up -d --no-deps api
docker compose run --rm --no-deps app python -m scripts.verify_e2e --run-id demo-01
docker compose run --rm --no-deps app python -m scripts.benchmark --run-id demo-01 --requests 100 --concurrency 4
```

Dùng RunId mới cho mỗi lần chạy độc lập. `outputs/latest_run.json` chỉ đổi sau khi các bước bắt buộc thành công. API tự nạp đúng bundle theo pointer này. Nếu chạy từ thư mục có dấu/khoảng trắng, script dùng `docker build` trực tiếp để tránh lỗi Compose Bake trên một số bản Docker Windows.

## Phạm vi từng phase

| Phase | Kết quả trong dự án |
|---|---|
| 1 | Dữ liệu đầy đủ, hash/metadata nguồn, kiến trúc và môi trường thống nhất |
| 2 | Bốn topic, producer có ACK/idempotence/backpressure, consumer kiểm tra và CLI roundtrip |
| 3 | Parse/quarantine, event-time UTC, dedup watermark 10 phút, window 5 phút trượt 1 phút và session gap 30 phút |
| 4 | Delta Bronze/Silver/Gold trên HDFS, kiểm tra commit/time travel/rollback; Mongo upsert và Redis feature/session |
| 5 | Pipeline VectorAssembler + ALS implicit, hai fold thời gian, tìm tham số, held-out ranking metrics và model được lưu |
| 6 | API gợi ý, dashboard KPI/mô hình, lọc item đã thấy, cache, fallback, Airflow chạy lại hằng ngày |
| 7 | Kiểm thử hồi quy, tích hợp thật, xác minh dữ liệu giữa các tầng và benchmark API |

Đọc `docs/PHASE_MAP.md` để đối chiếu từng task 1–25; `docs/ARCHITECTURE.md` giải thích lineage và các quyết định. `docs/ANALYSIS.md` phân tích những gì được giữ lại, viết mới, thay đổi và giới hạn. `docs/VALIDATION.md` chỉ liệt kê bằng chứng đã thực thi.

## Kết quả và kiểm thử

Lần chạy mẫu `verified-20261021c` nhận 10.000 dòng nguồn và có **9.999 sự kiện sạch khác nhau** sau dedup. Mô hình chỉ được huấn luyện/đánh giá trên mẫu này, chưa huấn luyện trên toàn bộ 2.756.101 dòng. Held-out **Precision@10 = 0, Recall@10 = 0, NDCG@10 = 0**; trong 912 user đánh giá có 871 user cold-start. Các giá trị 0 là kết quả thật, không phải metric bị thiếu. Việc pipeline và API hoạt động đúng không chứng minh chất lượng gợi ý tốt; cần đọc coverage/cold-start trong báo cáo và đánh giá lại trên dữ liệu lớn hơn. Model mẫu và báo cáo tương ứng có trong `example_results/verified-20261021c/`.

Mỗi run tạo `outputs/runs/<RunId>/`:

- `clean`, `quarantine`: sự kiện đã nhận và lỗi theo schema; checkpoint riêng cho từng query.
- `windows`, `sessions`: feature Delta streaming; session append chờ watermark chốt phiên.
- `snapshots`: snapshot batch hữu hạn có nhãn; giữ phần đuôi chưa chốt trên dữ liệu sạch đã nhận.
- `models`: Pipeline MLlib đánh giá/serving và Parquet gợi ý.
- `serving/model_bundle.json`, `seen_items.sqlite`: bundle phục vụ và toàn bộ lịch sử item đã thấy.
- `metrics`: báo cáo producer, consumer, Spark progress, KPI, ALS CV/test, ACID, kiểm tra toàn luồng và benchmark.

Delta lịch sử chính nằm trên HDFS: `/ecommerce/runs/<RunId>/delta/{bronze,silver,gold_interactions}`. MongoDB database `bigdata` và Redis key `bigdata:<RunId>:...` tách run. API không mở truy vấn tùy ý vào HDFS. Chỉ bundle và pointer đã công bố thành công được dùng phục vụ.

```powershell
./scripts/run_tests.ps1 -Integration
./scripts/kafka_cli_smoke.ps1
```

Launcher kiểm thử tách mỗi module sang tiến trình Python/JVM riêng để không trộn classpath Spark thuần với Delta. Các test tích hợp dùng namespace riêng và không xóa dữ liệu nghiệp vụ.

Chạy `-Integration` sau khi hạ tầng đã healthy bằng demo hoặc lệnh `docker compose up -d --wait kafka namenode datanode mongodb redis`; launcher test không tự khởi động các dịch vụ phụ thuộc.

## Streaming liên tục và session online

Sau khi một demo thành công, giữ API chạy; writer và producer tiếp tục dùng **data run** gốc của demo, ví dụ `demo-01`:

```powershell
docker compose run --rm --no-deps app python -m pipeline.streaming --run-id demo-01
```

Mở terminal khác và gửi thêm CSV hợp lệ, dùng cùng RunId:

```powershell
docker compose run --rm --no-deps app python -m phase2.producer --input data/sample/events_10000.csv --run-id demo-01 --rate 100
```

Replay cùng bản ghi giữ cùng event ID: trong phạm vi watermark không được tăng đúp feature. Để kiểm tra cập nhật mới, dùng timestamp/item/sự kiện mới thực sự. Sự kiện quá trễ bị loại theo watermark. Các query ingest, feature và lake/online có checkpoint riêng, không ghép nhiều stateful operator trong cùng query Spark 3.5. Chỉ chạy một live writer cho một run; không chạy batch overwrite đồng thời vào cùng các bảng đó.

Session online Mongo/Redis là projection tạm của sự kiện đã quan sát, có thể đổi khi dữ liệu đến trễ nối hai phiên. Retry dùng upsert facts và tính lại projection thay cho `$inc`, nên không cộng đúp. API luôn lọc lịch sử online mới, kể cả khi cache hit. Model ALS được cập nhật qua retraining, không tự học sau từng sự kiện.

Khi Airflow kích hoạt model trong một run retrain mới, giữ writer/producer ở data run gốc; API tự theo lineage `source_data_run_id` / `source_run_id` trong báo cáo để tìm namespace dữ liệu chuẩn. API hợp lịch sử seen của model run và data run, nên một item vừa tương tác ở luồng gốc vẫn bị loại khỏi gợi ý của model mới, kể cả khi danh sách đang lấy từ Redis cache. Response `/api/recommendations` và `/api/metrics` trả cả `run_id` (model đang phục vụ) và `data_run_id` (luồng dữ liệu gốc); cache tiếp tục khóa theo model run/version/user/k. Những run không thuộc lineage không được hợp lịch sử. Lineage lỗi hoặc có vòng lặp làm API trả 503 thay vì suy đoán namespace khác.

### Đối chiếu một sự kiện thật với gợi ý đang phục vụ

`scripts/live_check.py` tìm **một dòng đã tồn tại trong CSV nguồn**, có timestamp sau đoạn mẫu và item đang được API gợi ý; script chỉ sao chép dòng đó, không đổi ID/timestamp hay tạo hành vi giả. Phép thử làm thay đổi lịch sử đã quan sát của run được chọn, vì vậy dùng một run demo riêng và chỉ một live writer. Không chạy batch overwrite vào cùng run trong lúc kiểm tra.

Airflow có thể đổi latest pointer trong lúc thử. Dùng API phụ **cố định `RUN_ID`** để phép đối chiếu chẩn đoán này không đổi model/run giữa chừng. Ví dụ dưới đây chọn bundle của chính data run gốc đã hoàn thành `demo-01`; thay tất cả `demo-01` bằng RunId gốc thực của bạn. API chính vẫn tự theo model retrain mới và đọc lịch sử data run như mô tả trên; không cần chuyển writer sang run của model mới. Không đặt `RUN_ID` cố định cho API chính nếu muốn API chính tiếp tục theo các lần retrain tự động.

Terminal 1, mở API phụ trên localhost:8001, giữ tiến trình chạy:

```powershell
docker compose run --rm --no-deps --name retailrocket-live-check-api -e RUN_ID=demo-01 -p 127.0.0.1:8001:8000 api
```

Terminal 2, chọn dòng thật và ghi báo cáo trước khi phát:

```powershell
docker compose run --rm --no-deps app python -m scripts.live_check prepare --run-id demo-01 --base-url http://retailrocket-live-check-api:8000 --source data/raw/events.csv --event-csv data/sample/live_check.csv --report outputs/evidence/live_check_demo-01.json
```

Terminal 3, khởi động writer cho cùng run, giới hạn ba phút:

```powershell
docker compose run --rm --no-deps app python -m pipeline.streaming --run-id demo-01 --master 'local[2]' --duration-seconds 180
```

Khi writer đã khởi động, tại Terminal 2 phát đúng dòng được chọn rồi chờ kiểm chứng Redis seen set, MongoDB fact và phản hồi API đã loại item đó:

```powershell
docker compose run --rm --no-deps app python -m phase2.producer --input data/sample/live_check.csv --run-id demo-01 --rate 1
docker compose run --rm --no-deps app python -m scripts.live_check verify --run-id demo-01 --base-url http://retailrocket-live-check-api:8000 --report outputs/evidence/live_check_demo-01.json --timeout 120
```

Chỉ kết luận đạt khi report có `status=passed`; nếu không tìm được dòng thật phù hợp hoặc API đổi run thì script báo lỗi, không thay bằng dữ liệu giả. Writer tự dừng sau thời lượng đã chọn; Ctrl+C ở Terminal 1 dừng và dọn container API phụ. Không dùng dữ liệu live đã bổ sung để diễn giải lại các metric held-out đã chốt trước đó.

## Airflow huấn luyện lại hằng ngày

Sau demo đầu tiên:

```powershell
./scripts/start_scheduler.ps1
```

Mở **http://localhost:8088/**, tài khoản demo local `admin` / `demo-admin`. DAG `retailrocket_daily_retrain` chạy lúc **02:00 giờ Việt Nam**, `catchup=False`; có thể bấm Trigger DAG để thử ngay. Lịch chỉ thực thi khi các container còn chạy.

Airflow gọi worker nội bộ có một tác vụ huấn luyện đang chạy tối đa; không mount Docker socket. Worker chụp dữ liệu clean đã commit từ run hiện tại sang run mới, xây lake/features, đánh giá và fit ALS, rồi đổi pointer sau thành công. Request ID giúp retry của cùng DAG không tạo nhiều job giống nhau. Airflow SQLite/SequentialExecutor và tài khoản demo dùng cho môi trường học tập trên localhost.

Có thể huấn luyện lại trực tiếp:

```powershell
docker compose run --rm --no-deps app python -m pipeline.runner retrain --run-id retrain-01
```

## Chạy lịch sử đầy đủ

ZIP đã kèm `data/raw/events.csv`; không cần tải lại Kaggle. Chế độ batch đọc toàn bộ CSV không có thứ tự thời gian, kiểm tra/quarantine và dedup toàn cục, không áp watermark:

```powershell
docker compose run --rm --no-deps app python -m pipeline.runner batch --run-id history-full-01 --input data/raw/events.csv --full-grid
```

Đây là nhánh offline toàn bộ lịch sử; không dùng nó để tuyên bố replay Kafka đã nhận đủ mọi dòng. Huấn luyện/gợi ý trên 1,4 triệu user và 235 nghìn item cần nhiều tài nguyên hơn demo nhỏ; kết quả mẫu kèm ZIP không phải benchmark toàn bộ dữ liệu. Nếu muốn replay toàn bộ qua Kafka, sort trước bằng `scripts/sort_events.py`, chọn tốc độ/watermark phù hợp và theo dõi số bản ghi bị loại.

## Ý nghĩa mô hình và KPI

ALS dùng implicit feedback trọng số view=1, cart=3, purchase=5; `VectorAssembler` giữ các feature hỗ trợ, còn ALS nhận đúng cột user/item/rating. Chia theo timestamp trước khi tổng hợp, tìm tham số bằng hai fold mở rộng theo thời gian, đánh giá test chưa dùng chọn tham số, sau đó fit lại toàn bộ dữ liệu quan sát cho serving. Metric model và serving model có định danh riêng. Cold-start được tính miss khi đánh giá; API có popularity fallback được ghi rõ nguồn.

JSON gợi ý giới hạn mặc định 5.000 user; Parquet giữ gợi ý phân tán, SQLite giữ lịch sử seen của tất cả user quan sát. User ngoài phạm vi JSON dùng fallback và vẫn lọc seen. Đây là lựa chọn cho demo API, không phải inference ALS trực tiếp cho mọi user trên mỗi request.

Dataset không có review thật, giá, impression hoặc mapping item-category. Dashboard hiển thị CTR/doanh thu chưa có dữ liệu, không dựng giá hoặc review giả. Tỷ lệ mua theo visitor được định nghĩa rõ; purchase/view là tỷ lệ sự kiện và không được gọi là CTR. Bộ mẫu 10.000 dòng theo thời gian phục vụ kiểm thử kết nối, không đại diện chất lượng recommender trên toàn bộ lịch sử.

## Dừng và bảo toàn dữ liệu

```powershell
docker compose --profile orchestration stop
```

File output và volume được giữ. Không dùng `down -v` nếu muốn giữ Kafka/HDFS/MongoDB/Redis/Airflow. Nếu tiến trình bị tắt đột ngột và còn `outputs/.pipeline.lock`, kiểm tra không còn pipeline đang chạy trước khi phục hồi lock thủ công.

Các file gốc của bạn làm Phase 1 và Phase 4 được giữ trong `references/` để đối chiếu, không phải cấu hình khởi chạy của dự án mới. Attribution và metadata nguồn nằm trong `data/metadata/`; dữ liệu RetailRocket ghi giấy phép CC BY-NC-SA 4.0 trong metadata của nguồn.
