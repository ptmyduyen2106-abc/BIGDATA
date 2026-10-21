# Phase 3–4: Streaming, HDFS/Delta và feature/session trực tuyến

## Luồng đã nối trong mã

```text
Kafka (4 topic) -> Phase 3 validate + watermark + dedup -> clean/ Parquet
                                                            |
                      +-------------------------------------+------------------+
                      |                                     |                  |
              window 5m / slide1m                     session gap30m     lake_online foreachBatch
                      |                                     |                  |
                windows/ Delta                         sessions/ Delta    HDFS Delta Bronze
                                                                                |
                                                                          Silver -> Gold
                                                                                |
                                                                       Mongo events + projections
                                                                                |
                                                                          Redis seen/features
```

Phase 3 giữ nguyên raw `event` (view/addtocart/transaction/review), tính UTC bằng
`timestamp_millis(timestamp)` và dedup bằng ID SHA256 của năm cột nguồn. ID không
phụ thuộc lần replay. Watermark 10 phút chỉ nhận trễ trong giới hạn event time;
sự kiện quá trễ bị loại qua state metrics. JSON lỗi có quarantine với Kafka
topic/partition/offset. Không nối dedup và aggregation trong cùng query có state.

Window Delta cập nhật bằng MERGE theo visitor và hai mốc cửa sổ. Session Phase 3
chỉ append sau khi watermark chốt; khoảng nghỉ là 30 phút. Không dùng sự kiện giả
ở tương lai để ép chốt phiên cuối. Snapshot hữu hạn là ảnh chụp các event đã nhận,
không khôi phục event bị watermark loại.

## Lake HDFS là nguồn dữ liệu thật của Phase 4–5

`pipeline.storage.build_lake(spark, run_root)` tạo:

| Bảng | Hành vi | Hợp đồng chính |
|---|---|---|
| Bronze | insert-only MERGE theo event_id | clean event hợp lệ và metadata nguồn |
| Silver | snapshot Delta, dedup event_id | epoch ms + event_time UTC + event_date + điểm từng event |
| Gold interactions | snapshot Delta | visitorid long, itemid long, interaction_score float, count long |

Trọng số view=1, addtocart=3, transaction=5, review=0. Gold chỉ gộp event có điểm
dương theo cặp visitor/item. Phase 5 chia thời gian từ **Silver trước khi gộp** để
tránh dùng Gold toàn lịch sử gây rò rỉ dữ liệu đánh giá.

Khi `LAKE_BASE_URI=hdfs://namenode:8020/ecommerce/runs`, đường dẫn chính thức là:

```text
hdfs://namenode:8020/ecommerce/runs/<run_id>/delta/bronze
hdfs://namenode:8020/ecommerce/runs/<run_id>/delta/silver
hdfs://namenode:8020/ecommerce/runs/<run_id>/delta/gold_interactions
```

ML đọc đúng các URI qua cùng `pipeline.config.lake_path`; HDFS không chỉ là một
service đứng ngoài pipeline. Khi biến môi trường này không đặt, bảng được ghi
trên filesystem tại `outputs/runs/<run_id>/delta/` cho kiểm thử cục bộ.
`clean`, checkpoint, feature streaming và metric vẫn trên volume `outputs`.

Mỗi Delta table có transaction riêng. Ba bảng lake, Mongo và Redis không tạo
thành một distributed transaction. Callback lỗi phải được retry; các ghi facts
theo ID và projection SET làm kết quả hội tụ. Không chạy hai writer cùng run.

## Phiên trực tuyến thay đổi trong khi producer đang chạy

Chạy từ app container; broker, HDFS, Mongo và Redis cần sẵn sàng:

```powershell
docker compose run --rm app python -m pipeline.streaming --run-id live01 --output-root outputs --duration-seconds 180
```

Ở terminal khác, producer gửi cùng `--run-id live01`. Wrapper này dùng **một
SparkSession**, tạo các query Phase 3 độc lập và một query clean `foreachBatch`
có checkpoint `checkpoints/lake_online`. Mỗi micro-batch có dữ liệu mới:

1. Đọc clean đã commit, MERGE Bronze và dựng Silver/Gold thực trên HDFS.
2. Upsert event Mongo bằng unique index `(run_id,event_id)`.
3. Tính lại feature/window/session từ toàn bộ event đã lưu của người dùng bị tác
   động; thay thế projection thay vì tăng `$inc`.
4. Cập nhật nguyên tử các key Redis của người dùng bằng transaction pipeline.

Nếu lỗi sau bước 2 hoặc 3, retry không nhân đôi số event hay điểm. Event đến trễ
còn được Phase 3 nhận có thể nối hai phiên đã thấy: publisher tính lại, upsert
phiên mới và xóa **projection** session ID cũ trong đúng run/user; facts không bị
xóa. Mongo user/item features và Redis seen thay đổi khi event mới tới. Mô hình
ALS chỉ đổi khi có bước retrain/publish riêng; cache/gợi ý vẫn phải lọc seen mới.

Tùy chọn `--duration-seconds 0` chạy đến khi dừng; giới hạn thời gian không bảo
đảm đã hết backlog. Mặc định wrapper chỉ nhận payload có run_id tương ứng;
`--all-source-runs` cho phép nhận các run khác. Giữ nguyên checkpoint khi restart.
Không chạy thêm `pipeline.storage`, publisher hoặc wrapper thứ hai cùng run khi
writer live đang hoạt động.

Chế độ chỉ cập nhật NoSQL từ clean đã có:

```powershell
docker compose run --rm app python -m pipeline.online --run-id live01 --continuous --duration-seconds 120
```

Chế độ này có checkpoint `checkpoints/online`, không tự tạo ingest hay lake. Chạy
nó thay cho writer online khác, không đồng thời với wrapper live cùng run.
Hàm `publish_features` dùng cho bước batch của runner sau ingest hữu hạn.

Đây là implementation demo có giới hạn: publisher tính lại toàn lịch sử đã lưu
của user/item bị tác động; wrapper dựng lại lake snapshot từ clean sau mỗi batch.
Không tuyên bố độ trễ 1–2 giây hoặc khả năng xử lý hàng triệu event/giây. Tăng dữ
liệu cần incremental Gold, state có giới hạn và benchmark thật.

## Hợp đồng Mongo/Redis cho API

Mongo DB `bigdata` có các collection:

- `events`: facts bất biến, unique `(run_id,event_id)`; `source_run_id` giữ nhãn
  producer còn `run_id` là namespace của pipeline.
- `user_features`: unique `(run_id,visitorid)`, có `seen_items`, counts, điểm,
  `latest_window`, `current_session` và mốc epoch ms.
- `sessions`: unique `(run_id,visitorid,session_id)`, là projection cập nhật thực
  theo event; `provisional=true` cho biết chưa phải phiên watermark đã chốt.
- `item_features`: unique `(run_id,itemid)`, có counts, số visitor, điểm.

Redis dùng đúng namespace run:

| Key | Kiểu | Ý nghĩa |
|---|---|---|
| `bigdata:<run>:seen:<visitor>` | SET item ID string | API loại sản phẩm đã thấy |
| `bigdata:<run>:features:<visitor>` | JSON string | Feature user mới nhất |
| `bigdata:<run>:window:<visitor>` | JSON string | 5 phút neo theo event mới nhất user, cập nhật trực tuyến |
| `bigdata:<run>:session:<visitor>` | JSON string | Phiên quan sát gần nhất, provisional |
| `bigdata:<run>:stream_window:<visitor>` | JSON string | Window từ output Phase 3 khi có sẵn |
| `bigdata:<run>:final_session:<visitor>` | JSON string | Session Phase 3 đã watermark chốt khi có sẵn |

Window live dùng `[start,end)`, kết thúc ở phút UTC kế tiếp event user mới nhất;
không coi thời gian máy hiện tại là thời gian của dữ liệu RetailRocket 2015.
`stream_window` và `final_session` được đồng bộ trong bước publish khi các sink
Phase 3 có kết quả. Chúng không thay thế projection provisional đang cập nhật.
API cache recommendation cần áp dụng lại seen filter khi trả kết quả.

Connector chỉ import khi sử dụng. Biến môi trường: `MONGO_URI` (chấp nhận alias
`MONGODB_URI`), `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `LAKE_BASE_URI`.

## KPI và giới hạn ý nghĩa

`outputs/runs/<run>/metrics/kpis.json` ghi số event, visitor, item, phân bố loại
event, số visitor có purchase, số transaction ID khác nhau và thời gian UTC.
`visitor_purchase_conversion` được định nghĩa chính xác là số visitor có ít nhất
một purchase chia cho toàn bộ visitor đã quan sát. Đây không phải số đơn hàng
chia lượt xem hay tỷ lệ trên toàn bộ traffic chưa thu thập.

`purchase_per_view` của feature là tỷ lệ số event, null nếu không có view và có
thể lớn hơn 1. `ctr`, `revenue`, `average_order_value` là null vì dữ liệu không có
impression/quy chiếu quảng cáo, giá, tiền tệ hoặc tổng tiền đơn hàng. Không tạo
doanh thu hay CTR giả từ số lượt xem. Review có trong fixture tổng hợp có nhãn;
RetailRocket gốc không có review.

Metric JSON được ghi qua atomic rename, tránh dashboard đọc file đang ghi dở.
`lake-online-batch-*.json`, `online-batch-*.json`, `online_publish.json` và Spark
progress ghi kết quả thực; không cộng input của mọi query thành số Kafka event
duy nhất vì mỗi query đọc nguồn riêng.

## ACID/time travel và kiểm thử

```powershell
docker compose run --rm app python -m pipeline.acid_check --run-id demo01
docker compose run --rm app python -m unittest discover -s tests -p "test_storage.py" -v
docker compose run --rm -e RUN_DELTA_TESTS=1 -e RUN_ONLINE_TESTS=1 app python -m unittest discover -s tests -p "test_storage.py" -v
```

`verify_acid` tạo bảng thử có UUID riêng trên lake, đọc số version thật trước và
sau update, xác nhận historical read giữ dữ liệu cũ. Sau khi thêm CHECK constraint,
nó thử append gồm dòng tốt và dòng sai, đòi lỗi đúng tên constraint, xác nhận
không có commit mới và không có dòng tốt bị ghi một phần. Không giả định lịch sử
luôn bắt đầu tại version 0. Bảng nghiệp vụ không bị chỉnh bởi bài test. Kết quả
thật nằm tại `metrics/acid_check.json`; bài này không kiểm chứng cạnh tranh
concurrent-writer hay distributed transaction.

Test thuần Python kiểm tra duplicate/order invariance, retry không double-add,
bridge phiên trễ, xóa projection phiên cũ, run isolation, Redis seen và null ratio.
`RUN_DELTA_TESTS=1` chạy thật build/rebuild Delta và ACID trên Java/Delta;
`RUN_ONLINE_TESTS=1` kết nối Mongo/Redis thật và chỉ dọn namespace UUID của test.
Khi không bật, integration test được báo **skipped**, không tính là thành công.
Xem báo cáo kiểm thử của gói để phân biệt test đã chạy với test có sẵn.

Nguồn API chính thức:
[Spark Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html),
[Delta MERGE/foreachBatch](https://docs.delta.io/delta-update/),
[PyMongo upsert](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/update/),
[Redis pipelines/transactions](https://redis.io/docs/latest/develop/clients/redis-py/transpipe/).
