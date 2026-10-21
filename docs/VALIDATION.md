# Kết quả kiểm chứng thực tế

Ngày chạy Docker có bằng chứng: **21/10/2026**; cập nhật phạm vi bản sửa nguồn
ngày **22/10/2026**. Baseline được đối chiếu trong tài liệu này là
`verified-20261021c`, chạy trên Linux container trong Docker Desktop/WSL2.
Pipeline mẫu đã hoàn tất và kiểm tra end-to-end đạt; **chất lượng gợi ý trên
holdout của mẫu còn thấp: Precision@10, Recall@10 và NDCG@10 đều bằng 0**.
Hai kết luận này có ý nghĩa khác nhau: đường xử lý đúng và chạy được chưa
chứng minh mô hình gợi ý có chất lượng tốt.

Sau baseline đã thử thêm một sự kiện thật để kiểm tra cập nhật online, rồi sửa
lineage giữa model run và ingestion run. Những lượt Docker thành công dưới đây
là bằng chứng của phiên bản đã chạy ngày 21/10, **không phải xác nhận toàn bộ
Docker/E2E đã chạy lại sau bản sửa lineage**. Giới hạn này và callback live bị
ngắt được trình bày riêng bên dưới.

Các số dưới đây lấy từ tệp evidence và metrics đi kèm, không phải số minh họa.
Đường dẫn tương đối trong tài liệu tính từ thư mục gốc của gói dự án.
Bản kết quả được chụp để bàn giao nằm tại
`example_results/<run_id>/metrics/`; `metrics/...` trong các phần nói về
baseline là viết gọn cho `example_results/verified-20261021c/metrics/...`.
Khi người nhận chạy lại, kết quả mới sinh ở `outputs/runs/<run_id>/metrics/`;
không dùng bản chụp làm trạng thái đang chạy của một máy khác.

## Tổng hợp phạm vi đã chạy

| Hạng mục | Kết quả quan sát | Bằng chứng |
| --- | --- | --- |
| Build và nền Docker | Đã build và khởi động các dịch vụ cho baseline | `outputs/evidence/docker-build.log`, `infra-start.log` |
| Dataset đầy đủ | Đã kiểm checksum/profile 2.756.101 dòng; chưa train toàn bộ | `outputs/evidence/dataset_profile.json` |
| Kafka CLI | Gửi/nhận khớp chính xác key và payload trên topic kiểm tra riêng | `outputs/evidence/kafka-cli.json`, `kafka-cli.log` |
| Sáu module kiểm thử Docker trước bản sửa lineage cuối | 52 kiểm thử đạt; mỗi module exit code 0, không ghi skipped | `outputs/evidence/all-tests.log` |
| Lineage helper sau bản sửa | 6 kiểm thử Python host đạt; không dùng Docker | `outputs/evidence/lineage-tests-host.log` |
| API sau bản sửa lineage | 16 TestClient test đạt trên Python 3.12 host, gồm 2 regression mới; Redis/Mongo ở biên được giả lập | `outputs/evidence/service-lineage-tests-host.log` |
| Pipeline mẫu | `status=completed`, 10.000 dòng nguồn → 9.999 event ID khác nhau | `example_results/verified-20261021c/metrics/run_report.json` |
| E2E với store thật và API | `status=passed` | `example_results/verified-20261021c/metrics/e2e_verification.json` |
| Benchmark HTTP | 100/100 request thành công ở concurrency 4 | `example_results/verified-20261021c/metrics/benchmark.json` |
| Airflow | Retrain thật hoàn tất; DAG và cả hai task success; 10/10 kiểm tra đối chiếu đạt | `outputs/evidence/airflow_retrain_proof.json`, `airflow_dag_state.json`, `airflow_job_worker.log` |
| History batch import | Smoke test thật trên 10.000 dòng: 9.999 clean, 0 quarantine; không Kafka/watermark/streaming query | `outputs/evidence/batch-import-smoke.json`, `batch-import-smoke.log` |
| Một sự kiện online thật | Producer ACK, Mongo fact và Redis seen cập nhật; API loại item mới xem kể cả cache hit | `outputs/evidence/live_check.json`, `live-producer.json` |
| Kết thúc luồng live theo thời gian | Callback batch 1 đã commit lake rồi bị ngắt ở bước online; chưa xác minh drain/restart hoàn chỉnh | `example_results/verified-20261021c/metrics/lake-online-batch-000001.json`, `live_pipeline.json`; `outputs/evidence/live-stream.log` |
| Dashboard | Kiểm tra HTTP/static asset và logic JavaScript; chưa có ảnh chụp xác minh giao diện trên trình duyệt thật | `tests/test_service.py`, `outputs/evidence/all-tests.log`; xem giới hạn UI dưới đây |

## Dataset và tính bảo toàn dữ liệu trong baseline

Profile của CSV đầy đủ xác nhận:

- 2.756.101 dòng: 2.664.312 `view`, 69.332 `addtocart`, 22.457 `transaction`.
- `events.csv`: 94.237.913 byte, SHA-256
  `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`.
- `category_tree.csv`: 14.454 byte, SHA-256
  `94e865eb0a3d48cbbfe3b79079018dd92509315c88f5fd8d00d0b4b5af434f5b`.
- Có 1.377.377 vị trí timestamp giảm so với dòng trước: CSV đầy đủ không được
  sort toàn cục. Không dùng trực tiếp nó để tuyên bố replay event-time không
  mất dữ liệu; cần sort và kiểm tra offsets/watermark cho lần chạy toàn nguồn.

Baseline dùng **`data/sample/events_10000.csv`**, không dùng toàn bộ CSV lớn.
Producer đã gửi và Kafka acknowledge đủ 10.000 message. Consumer xác minh
đủ 10.000, có **9.999 event ID duy nhất và 1 ID lặp**; không có lỗi hợp đồng.
Clean nhận 9.999 sự kiện, đúng số ID duy nhất từ nguồn. Chênh lệch một dòng
là dedup theo hợp đồng event ID, không phải bằng chứng mất một message Kafka.

| Bước/kết quả baseline trước lượt live bổ sung | Số lượng |
| --- | ---: |
| Producer sent / acknowledged | 10.000 / 10.000 |
| Kafka verify matched / unique / duplicate IDs | 10.000 / 9.999 / 1 |
| Clean / Bronze / Silver | 9.999 / 9.999 / 9.999 |
| Silver view / addtocart / transaction | 9.727 / 211 / 61 |
| Visitor / item phân biệt | 5.882 / 6.776 |
| Gold user-item pairs | 8.313 |
| Snapshot window / session rows | 37.727 / 6.170 |
| Mongo durable events / users / sessions | 9.999 / 5.882 / 6.170 |

Các số baseline này được giữ trong `run_report.json` và model metrics của lần
train ban đầu. Sau lượt live bổ sung được mô tả bên dưới, `kpis.json`/lake và
`online_publish.json` của **cùng data run** đã tiến tới 10.000 event; không lấy
trạng thái mới đó để sửa ngược phạm vi training 9.999 event của model đã lưu.

Snapshot window/session tính batch trên clean hữu hạn và bao gồm phần đuôi
chưa được watermark chốt. Không coi số session snapshot/provisional này là
số session streaming finalized. Không cộng `event_count` của tất cả sliding
window để suy ra tổng nguồn, vì một event nằm trong nhiều window.

Baseline bắt đầu 19:54:22 và hoàn tất 20:02:46 ngày 21/10/2026 theo giờ Việt Nam
(503,438 giây; timestamp gốc trong báo cáo lưu UTC). Thời gian này bao gồm
nhiều bước Spark, NoSQL, lựa chọn/fit model và ACID, không phải độ trễ mỗi event.

Đã kiểm tra riêng đường **batch history import** qua `pipeline.history` trên
chính mẫu 10.000 dòng, run `batch-import-check`. `batch-import-smoke.json`
ghi `status=passed`, `mode=batch_full_history_no_watermark`: dữ liệu ghi bền
vững có 9.999 clean/event ID duy nhất, 0 dòng quarantine, 0 dòng có Kafka
metadata và 0 active streaming query. Đây là phép kiểm tra nhập lịch sử batch
thật, không chạy Kafka/watermark/training và không sửa run baseline. Kết quả
này chưa chứng minh đã import và train toàn bộ 2.756.101 dòng; số 10.000 phải
được giữ nguyên khi mô tả phạm vi kiểm chứng.

## Bộ kiểm thử và ACID

`outputs/evidence/all-tests.log` chạy từng module trong tiến trình Python/JVM
riêng, kết thúc với sáu exit code 0:

| Module | Số test | Phạm vi chính |
| --- | ---: | --- |
| `test_phase2.py` | 17 | Hợp đồng, validation, producer/consumer và event ID |
| `test_phase3_streaming.py` | 3 | Phép biến đổi Spark, streaming/state/checkpoint, snapshot |
| `test_service.py` | 14 | API, input bounds, bundle reload, seen filter, fallback, cache, static dashboard |
| `test_sort_events.py` | 6 | Sort CSV, dữ liệu lỗi và hợp đồng file |
| `test_storage.py` | 5 | Projection, Delta thật, MongoDB/Redis thật, retry/session bridge và phân tách run |
| `test_training.py` | 7 | Ranking/time split, seen index và fit/lưu/nạp ALS Spark thật |
| **Tổng** | **52** | Tất cả module ghi `OK` |

Đây là tổng số của lần chạy được lưu, không cộng lần test dịch vụ riêng trước
đó để tăng số kiểm thử. Một phần test logic dùng fixture/mocked boundary;
không diễn giải toàn bộ 52 test như 52 lượt benchmark với hệ thống phân tán thật.
Kiểm tra E2E riêng bổ sung đối chiếu MongoDB, Redis và HTTP đang chạy.

Bản sửa lineage cuối có bằng chứng riêng: sáu test `test_lineage.py` trên
Python host đạt trong `lineage-tests-host.log`. Chúng kiểm tra truy ngược nhiều
thế hệ retrain về ingestion run, canonical source, báo cáo cũ thiếu lineage,
vòng lặp, sai danh tính và đường dẫn không hợp lệ. Không cộng các test host
này vào 52 test để gọi đó là một lượt full Docker mới; full suite sau bản sửa
chưa được xác minh lại bằng Docker.

Lượt bổ sung `service-lineage-tests-host.log` ghi **16 API TestClient test đạt
trong 1,132 giây**, trên Python 3.12 host. Hai regression mới kiểm tra hợp seen
giữa source data/model run qua cache hit, không dùng run không liên quan và
từ chối lineage lỗi bằng 503. Bản sửa cũng đóng tường minh hai kết nối SQLite
read bằng `contextlib.closing`, tránh giữ file handle trên Windows. Redis và
MongoDB của lượt TestClient này được giả lập ở biên; không coi đó là kết quả
kiểm tra hai store thật sau sửa. Sáu helper và 16 API là **các lượt bổ sung**,
trong đó API chạy lại cả test cũ; không gọi chúng là 22 test mới hoặc gộp vào
52 test Docker cũ như một lượt full suite của phiên bản cuối.

`metrics/acid_check.json` của baseline ghi `status=passed` và bốn kiểm tra:
UPDATE tạo version mới; đọc version cũ giữ nguyên; CHECK constraint từ chối
append vi phạm; append lỗi không commit một phần. Log có stack trace
`DeltaInvariantViolationException` cho giá trị âm **được tạo có chủ đích**;
assertion xác nhận đây đúng là lỗi constraint mong đợi. Kết quả không chứng
minh concurrent writer hoặc distributed transaction xuyên nhiều kho.

## Huấn luyện thật và chất lượng mô hình

Baseline chạy ALS implicit Spark MLlib thực, chế độ quick, seed 42. Hai cấu hình
regularization được so sánh trên hai fold thời gian; chọn rank 8,
`reg_param=0.05`, alpha 20. Training/test chia trên sự kiện trước khi tổng hợp
user-item: model đánh giá fit 8.490 event đầu, test 1.509 event sau.

| Chỉ số test cuối tại K=10 | Giá trị |
| --- | ---: |
| Precision@10 | **0,000000** |
| Recall@10 | **0,000000** |
| NDCG@10 | **0,000000** |
| Catalog coverage | 3,4838% |
| User coverage | 4,4956% |
| User có đích test mới | 912 |
| Cold user | 871 / 912 ≈ **95,50%** |
| Novel held-out user-item pairs | 1.223 |
| Cold item pairs trong các đích mới | 881 |
| Cặp lặp lịch sử bị bỏ khỏi target | 65 |

Nguồn chi tiết: `metrics/model_metrics.json`, đặc biệt `cross_validation`,
`heldout_test` và `split_semantics`. Trong validation fold 2 có điểm rất nhỏ
khác 0, nhưng không được thay điểm validation cho điểm held-out cuối. Cold
user/item được giữ làm miss trong mẫu đo; không bỏ chúng để làm đẹp chỉ số.

Mẫu ít lịch sử và cold-start rất cao là giới hạn đáng kể. Kết quả test bằng
0 chưa chứng minh mô hình mang lại gợi ý hữu ích cho người dùng mới và không
chứng minh tăng CTR, conversion hay doanh thu. Chưa chạy full grid hoặc train
toàn bộ 2.756.101 dòng trong baseline này. Cần đánh giá thêm dữ liệu, baseline
popularity và cách phục vụ cold-start trước khi kết luận về hiệu quả mô hình.

Sau khi đóng băng đánh giá, model phục vụ được refit trên cả 9.999 event
đã nhận. Hai phiên bản được ghi rõ:

- Metric model: `verified-20261021c:als-evaluation-v1`.
- Serving model: `verified-20261021c:als-serving-v1`.

Model phục vụ học 5.882 user, catalog 6.776 item; JSON xuất 5.000 user, còn
recommendations toàn user ở Parquet. SQLite chứa 8.313 cặp seen của mọi user.
Người ngoài phạm vi JSON dùng popularity và vẫn loại seen, không được gắn
nhãn là đã nhận danh sách ALS cá nhân hóa xuất sẵn.

## API, dashboard và benchmark

E2E baseline xác nhận API trả đúng run/model và KPI; kiểm tra user 12, 385,
1250 qua `als_bundle`, user 1186759 ngoài phạm vi JSON và user mới 2147483647
qua `popular_fallback`. Mỗi lượt trả 10 item duy nhất, không giao với lịch sử
SQLite. MongoDB có 9.999 event và một model registry đúng phiên bản; Redis
có seen/active model đúng run. Các kết quả này có trong
`metrics/e2e_verification.json`, không phải chỉ từ mock của unit test.

Benchmark ghi 100 request sau 5 warmup, concurrency 4, cùng user được chọn:

| Thông số HTTP đo được | Giá trị |
| --- | ---: |
| Thành công / tổng | 100 / 100 |
| Mean | 136,986 ms |
| p50 | 113,882 ms |
| p95 | **237,328 ms** |
| p99 | 302,287 ms |
| Throughput | 28,690 request/giây |
| Thời gian nhóm request đo | 3,486 giây |

Môi trường benchmark ghi `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64`.
Đây là HTTP recommendation API trong cấu hình demo, có warmup/cache, không
phải Kafka→Spark latency, tải người dùng đa dạng, số đo production hoặc SLA.
Producer report riêng có 3.637,871 event/giây trong lượt gửi 10.000 message;
đó là ack throughput của producer, không phải throughput xử lý toàn pipeline.

TestClient/HTTP đã kiểm tra trang `/`, các static asset và API. Logic
JavaScript đã được kiểm tra trong quá trình chuẩn bị, nhưng chưa có screenshot
hoặc lượt tương tác trực quan xác nhận mọi trạng thái dashboard trên trình
duyệt thật; không gọi kiểm tra HTTP là visual QA hoàn chỉnh.

## Sự kiện online thật, trạng thái lake và giới hạn dừng live

`outputs/evidence/live_check.json` ghi **`status=passed`** cho một phép kiểm
thực tế trên data run `verified-20261021c`:

- Lấy đúng dòng **1.466.186 tính cả header** từ `data/raw/events.csv`:
  timestamp `1430704718752`, visitor **556819**, event `view`, item **198209**;
  không tạo một hành vi tổng hợp để thay nguồn thật.
- Trước khi phát, item 198209 đang có trong danh sách gợi ý của user này.
  Producer ACK một message `page_view`; Mongo có một fact khớp sự kiện và
  Redis seen chứa item vừa xem.
- Sau cập nhật, API dùng cùng model baseline đã loại 198209 ra khỏi danh sách
  dù **`cache_hit=true`**. Seen count tăng từ 14 lên 15; response vẫn trả 10
  item bằng cách bổ sung ứng viên khác. Phép kiểm này chứng minh lọc lịch sử
  mới trên cache hit, không chứng minh ALS đã học lại sau một click.

Trạng thái lake/online sau sự kiện thật bổ sung:

| Kho/đầu ra | Baseline trước live | Sau event bổ sung |
| --- | ---: | ---: |
| Bronze / Silver | 9.999 / 9.999 | 10.000 / 10.000 |
| Gold user-item pairs | 8.313 | 8.314 |
| Mongo durable events | 9.999 | 10.000 |
| Session projection Mongo | 6.170 | 6.171 |

Kết quả mới nằm trong `kpis.json` và `online_publish.json`; model metrics
baseline vẫn ghi input/training **9.999**, vì không retrain model baseline
sau event. Snapshot session batch cũ cũng không tự trở thành snapshot mới.

Lượt live được dừng bằng giới hạn **180 giây**. Callback `lake_online` batch
1 đã commit HDFS lake 10.000 event, rồi bị `InterruptedException` khi đang
thực hiện bước online. `lake-online-batch-000001.json` ghi **`status=failed`**;
bản publish online trước đó đã ghi 10.000 facts và phép kiểm API phía trên
đã đạt. File bao ngoài `live_pipeline.json` ghi `completed` theo kết thúc do
giới hạn thời gian, nhưng điều đó **không chứng minh tất cả callback hoặc
checkpoint sink đã commit hoàn chỉnh**. Cần đọc báo cáo từng batch thay vì
chỉ nhìn trạng thái bao ngoài.

Nguồn hiện có thêm `pipeline.streaming --trigger available-now` để có lựa
chọn kết thúc hữu hạn thay cho hủy theo đồng hồ; **chưa có bằng chứng chạy
Docker xác minh drain/restart này sau bản sửa**. Không xóa checkpoint hoặc
coi retry đã được kiểm chứng chỉ vì số lượng cuối ở lake và API khớp.

## Airflow: retrain thật đã hoàn tất

DAG `retailrocket_daily_retrain` import được, không có import error và đã
trigger manual run `verified-phase17-20261021`. Worker nhận job
`64af6b3f57f5c1763b303ac8`, lấy clean nguồn `verified-20261021c` và tạo run
đích `retrain-64af6b3f57f5c1763b303ac8`.

`outputs/evidence/airflow_retrain_proof.json` xác nhận **`status=passed`**:

- Worker job `completed`, bắt đầu 20:03:44 và kết thúc 20:09:44 ngày
  21/10/2026 giờ Việt Nam; runner báo 358,692 giây thực hiện.
- DAG run `success`, kết thúc 20:10:10; cả `submit_retraining` và
  `wait_for_evaluated_model` đều `success`, được chụp trong
  `outputs/evidence/airflow_dag_state.json`.
- 10/10 kiểm tra đối chiếu đều `true`: job hoàn tất, DAG success, mọi task
  success, latest pointer đúng, API ready, API reload model mới, registry Mongo
  đúng, registry Redis đúng, phiên bản recommendation đúng và không trả seen.
- API, latest pointer và hai registry đều trỏ tới
  `retrain-64af6b3f57f5c1763b303ac8:als-serving-v1`; API báo `stale=false` và
  MongoDB/Redis đều `ok`. Lượt kiểm user 12 trả 5 item, đã loại lịch sử quan sát.

Báo cáo của run retrain được chụp tại
`example_results/retrain-64af6b3f57f5c1763b303ac8/metrics/`; log worker nằm
trong `outputs/evidence/airflow_job_worker.log`. Phép xác minh chỉ đọc trạng
thái/kết quả sau chạy, không trigger thêm một lượt để tạo bằng chứng.

Lượt này chứng minh đường **Airflow → typed worker → retrain → kích hoạt và
reload API** hoạt động trên clean của mẫu. Nó không chứng minh lịch đã tự chạy
nhiều ngày hoặc thu thập dữ liệu mới. Retrain dùng lại cùng dữ liệu và heldout
Precision/Recall/NDCG@10 vẫn bằng 0; không gọi retrain thành công là mô hình
đã cải thiện chất lượng gợi ý.

Run retrain này chụp clean **9.999 event trước sự kiện online bổ sung**. Một
event thật được producer gửi sau khi retrain đã hoàn tất; model và metric của
run retrain không bao gồm event thứ 10.000 của data run baseline. Đối chiếu
lineage/các mốc chụp dữ liệu trước khi so số lake hiện tại với số training.

## Bản sửa lineage cuối và phần chưa chạy lại

Khi retrain tạo model run mới, producer vẫn có thể tiếp tục ghi vào ingestion
run gốc. Bản sửa `pipeline.config`/`pipeline.runner` truy ngược canonical
`source_data_run_id` qua nhiều thế hệ model để lần retrain sau đọc nguồn đang
nhận event. API được sửa để hợp seen của model run và data run gốc, kể cả khi
cache hit; lineage lỗi được từ chối thay vì âm thầm dùng sai lịch sử. Đây là
thay đổi mã sau các lượt Docker baseline/Airflow ở trên.

Sau khi tiếp tục phiên làm việc, Docker Desktop UI vẫn hiển thị đang chạy và
lệnh đọc context dùng được, nhưng các lệnh cần Docker daemon không trả lời.
Vì vậy chưa rebuild/chạy lại toàn bộ Docker E2E, lịch Airflow hoặc live drain
cho mã lineage cuối. Kiểm thử helper/API host được ghi riêng ở trên; không dùng
52 test Docker trước bản sửa để khẳng định runtime cuối đã được xác nhận lại.

## Những kết luận không nằm trong bằng chứng hiện tại

- Chưa train/benchmark toàn nguồn 2.756.101 event hoặc triển khai cloud/cụm HA.
- Chưa có kết quả A/B test, doanh thu, CTR chuẩn hoặc chất lượng cold-start tốt.
- Không có review thật, giá/giá trị đơn, impressions hoặc ánh xạ item-category
  trong dữ liệu đang đóng gói; các KPI tương ứng để `null`/không khả dụng.
- Không cam kết exactly-once xuyên Delta, MongoDB và Redis; retry dùng event
  upsert và projection xác định để hội tụ, trong điều kiện một writer/run.
- Chưa có phép đo phục hồi sau sự cố cụm, concurrent writer, tải kéo dài hay
  visual QA trình duyệt đầy đủ. Giữ nguyên các giới hạn này khi viết báo cáo.
- Chưa xác minh hoàn tất mọi callback/checkpoint của lượt live dừng 180 giây,
  drain `available-now`, hoặc full Docker sau bản sửa lineage cuối.

