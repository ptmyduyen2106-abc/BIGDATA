# Phân tích dự án tích hợp Phase 1 đến Phase 7

## Kết quả triển khai

Dự án mới biến các phần rời thành một luồng thực thi chung. `scripts/run_demo.ps1` khởi động hạ tầng, gửi dữ liệu, xử lý streaming, tạo lake và feature, huấn luyện/đánh giá ALS rồi xác minh API. Các phase chia sẻ schema, RunId, checkpoint và quy tắc công bố kết quả. Cần đọc `VALIDATION.md` cùng các JSON/log evidence để phân biệt chức năng đã triển khai với phạm vi dữ liệu đã thực sự chạy.

Baseline Docker ngày 21/10/2026 và một lượt retrain Airflow đã có bằng chứng
thành công. Bản sửa lineage cuối được thực hiện sau đó; chưa chạy lại full
Docker/E2E cho bản sửa này do Docker daemon không trả lời khi tiếp tục phiên
làm việc. Phép kiểm online một event đạt, nhưng dừng live theo thời gian đã
ngắt một callback sau khi lake commit; không gọi toàn bộ live checkpoint là
đã hoàn tất chỉ từ trạng thái bao ngoài hoặc kết quả API.

Toàn bộ CSV gốc RetailRocket nằm trong ZIP để không phải tìm lại ZIP Phase 1. Mẫu 10.000 dòng được giữ cho demo trên máy cá nhân. Mã và báo cáo gốc của bạn làm Phase 1, mã Phase 4 gốc nằm trong `references/` để giữ attribution và đối chiếu; chúng không khởi chạy song song với cấu hình mới.

## Phần kế thừa và phần thay đổi

| Phần | Kế thừa | Thay đổi trong bản tích hợp |
|---|---|---|
| Phase 1 | CSV, mẫu, metadata/hash nguồn, ý tưởng pipeline | Một Docker Compose, kiểm tra nguồn và tài liệu kiến trúc; toàn bộ dữ liệu được đóng chung |
| Phase 2 | Producer và schema của gói trước | Chờ metadata/leader Kafka; sửa giới hạn in-flight cho idempotence của kafka-python; CLI roundtrip thật |
| Phase 3 | Schema explicit, watermark, dedup, window/session | Phân biệt file local và HDFS bằng URI/config; wrapper live ghi đồng bộ các tầng và lưu progress |
| Phase 4 | Bronze/Silver/Gold và điểm 1/3/5 của bạn trong nhóm | Delta trên HDFS theo run, MERGE/upsert khi retry, Mongo facts duy nhất, Redis session/window; ACID test có assertion và namespace riêng |
| Phase 5 | Yêu cầu MLlib trong ActionPlan | Viết mới ALS implicit, temporal CV, held-out evaluation, model lưu/nạp, export phục vụ và SQLite seen index |
| Phase 6 | Yêu cầu API/dashboard/Airflow | Viết mới FastAPI, dashboard tiếng Việt, cache/fallback, model reload và DAG huấn luyện lại |
| Phase 7 | Yêu cầu kiểm thử tích hợp và hiệu năng | Unit/integration tests, xác minh giữa các tầng, benchmark HTTP, log và báo cáo tái lập |

Việc thống nhất Spark 3.5.6, Delta 3.3.3 và Python 3.11 tránh ghép trực tiếp runtime Spark khác nhau từ các ZIP. Service name và namespace dữ liệu được cấu hình một chỗ; không phụ thuộc container tên cố định của dự án cũ. Kafka publish cổng 19092 nên không chiếm 9092 mà stack khác có thể đang dùng.

## Từ sự kiện tới đặc trưng

Năm trường nguồn được giữ nguyên ý nghĩa. Timestamp là epoch milliseconds; thời gian hành vi được tính từ trường này trong UTC, không từ ngày chạy demo hoặc Kafka arrival time. View/cart không bị loại vì transactionid rỗng.

Event ID là SHA256 của nội dung nguồn chuẩn hóa, không chứa RunId hay thời gian ingest. Retry của cùng sự kiện giữ cùng ID. Một dòng gốc giống hệt một dòng khác được xem là trùng; dataset không cung cấp mã hành động độc lập để chứng minh hai dòng giống nhau là hai hành động thực.

Mẫu có 10.000 dòng vật lý nhưng 9.999 ID theo nội dung. Vì vậy producer/consumer có thể ghi 10.000 bản ghi trong khi lake và facts Mongo có 9.999 sự kiện sạch. Cần đối chiếu số dòng sau dedup với số ID nguồn riêng biệt, không kết luận mất dữ liệu chỉ từ chênh lệch này. Phần đối chiếu thực tế được ghi trong run report.

Đó là mốc baseline trước thử nghiệm online. Sau một view thật được lấy thêm
từ CSV gốc, lake và Mongo của cùng ingestion run có 10.000 facts duy nhất,
Gold có 8.314 cặp và session projection có 6.171 phiên. Model metrics baseline
vẫn đúng ở 9.999 event: nó mô tả dữ liệu lúc train, không được sửa theo số
facts mới. Mỗi báo cáo phải được đọc cùng thời điểm và nguồn dữ liệu của nó.

Watermark 10 phút cho phép quản lý state hữu hạn. CSV đầy đủ không có tổng thứ tự thời gian; replay nhanh nhiều tháng qua nhiều topic/partition có thể tạo dữ liệu quá trễ. Vì vậy có hai nhánh được gọi tên rõ: streaming có watermark cho sự kiện online, và batch history cho toàn bộ lịch sử không bị loại bởi watermark. Không dùng thành công của nhánh batch làm bằng chứng nhánh Kafka đã nhận đủ toàn bộ lịch sử.

Dedup, window và session là query riêng, nối qua Parquet đã commit. Cách tách này tránh chuỗi stateful operator không được hỗ trợ trong Spark 3.5. Window 5 phút trượt 1 phút khiến một sự kiện góp vào nhiều cửa sổ. Session có gap 30 phút; phiên cuối chỉ final khi watermark đủ xa. Snapshot batch giữ các phần đuôi còn mở với nhãn riêng, không đẩy đồng hồ bằng sự kiện giả.

## Lưu trữ và cập nhật online

Delta Bronze MERGE theo event ID, Silver/Gold được tính xác định từ dữ liệu đã nhận. Mỗi bảng có commit ACID riêng; cả chuỗi nhiều bảng và Mongo/Redis không phải một distributed transaction. Nếu lỗi giữa chừng, retry phải hội tụ kết quả thay vì cộng đúp.

Mongo lưu fact theo unique `(run_id,event_id)` và feature theo khóa nghiệp vụ. Projection user/session/item được tính lại từ facts đã lưu. Redis thay nội dung feature/session/seen, không cộng `$inc` theo số lần callback chạy. Một sự kiện đến trễ có thể nối hai phiên; projection cũ được thay và session ID không còn phù hợp được dọn trong đúng namespace.

Session Mongo/Redis là projection tạm của sự kiện quan sát; session streaming append là phiên đã final theo watermark. Hai loại được gắn nguồn riêng. Cập nhật feature online không có nghĩa mô hình ALS được học lại sau từng click.

Phép kiểm live dùng dòng 1.466.186 tính cả header của `events.csv`: visitor
556819 xem item 198209. Item này có trong gợi ý trước đó, sau Kafka ACK và
cập nhật Mongo/Redis thì bị API loại kể cả cache hit. `live_check.json` ghi
passed; đây là hành vi loại seen bằng lịch sử mới, không phải một phép train
ALS online hoặc bằng chứng chất lượng ranking tăng.

Giới hạn dừng 180 giây đã cắt callback batch 1 ở bước online sau khi Delta
lake commit 10.000 event. Một lần publish online trước đó và API đã xác nhận
facts mới, nhưng báo cáo batch này ghi failed/InterruptedException. Do đó
`live_pipeline.json` completed theo thời hạn không thay thế trạng thái từng
callback và commit checkpoint. Tùy chọn kết thúc hữu hạn `--trigger
available-now` được thêm vào nguồn để hỗ trợ xử lý tồn đọng; đường drain và
khôi phục cuối này chưa được kiểm chứng lại bằng Docker.

## Huấn luyện và đánh giá mô hình

ALS dùng implicit feedback: view=1, addtocart=3, transaction=5. VectorAssembler tạo vector feature hỗ trợ trong Pipeline, còn ALS dùng đúng ba cột user, item và rating; không đưa vector vào ALS sai API. User/item được kiểm tra nằm trong miền integer 32 bit để giữ ID gốc an toàn.

Việc chia thời gian diễn ra trước tổng hợp user-item: fold 1 học tới khoảng 50% và kiểm tra 50–70%; fold 2 học tới 70% và kiểm tra 70–85%; tập test nằm sau 85%. Quick mode vẫn có hai cấu hình tham số, không chỉ fit một mô hình rồi gọi đó là tìm tham số. Sau khi chọn bằng validation, mô hình đánh giá không học tập test. Chỉ sau khi đóng băng metrics mới fit lại toàn bộ sự kiện đã quan sát cho serving.

Precision@K, Recall@K, NDCG@K và coverage là metric ranking phù hợp với implicit interactions. Cold user/item được tính là miss, không âm thầm loại để làm đẹp điểm. Tương tác lặp với item đã thấy được tách khỏi mục tiêu khám phá item mới. Báo cáo chỉ rõ số cold case, user/target được đánh giá và model version của metric.

Điểm ALS là điểm xếp hạng, không phải xác suất mua. Tập mẫu theo thời gian chủ yếu phục vụ kiểm tra kết nối và rất thưa, nên không đủ khẳng định lợi ích kinh doanh hoặc chất lượng trên toàn bộ RetailRocket. Đánh giá mẫu và tốc độ xử lý local không chứng minh tăng conversion, giảm churn hoặc đạt quy mô 20–50 triệu sự kiện/ngày.

Baseline thực tế có Precision@10, Recall@10 và NDCG@10 trên holdout đều bằng
0, với 871/912 user đánh giá là cold user (khoảng 95,50%). Đường fit/lưu/nạp
và API hoạt động không đủ để kết luận mô hình hữu ích; cần đánh giá thêm dữ
liệu và baseline cold-start. Full CSV 2.756.101 dòng được đóng gói và profile,
chưa được dùng toàn bộ để train trong kết quả bàn giao.

## Phục vụ và điều phối

API đọc bundle đã kiểm tra, lọc toàn bộ lịch sử seen qua SQLite và bổ sung lịch sử online từ Mongo/Redis. Cache có RunId, model version, user và K trong khóa. Lịch sử được lọc lại kể cả cache hit để item vừa xem không xuất hiện vì cache cũ. Nếu dịch vụ online hỏng, API có thể phục vụ từ bundle và cảnh báo giới hạn cập nhật; không bảo đảm biết những sự kiện mới chưa có trong bundle khi cả nguồn online không truy cập được.

JSON gợi ý giới hạn mặc định 5.000 user để giảm bộ nhớ API; lịch sử seen đầy đủ ở SQLite và kết quả gợi ý phân tán ở Parquet. User ngoài phạm vi JSON hoặc cold user nhận popularity fallback, được trả rõ trường `source`. Mô hình không được suy ra là cá nhân hóa chỉ vì endpoint có user ID.

Pointer model chỉ đổi sau khi toàn pipeline thành công. API giữ bundle cũ hợp lệ trong cùng run khi reload lỗi, nhưng không lấy dữ liệu run cũ để giả là run mới. Airflow chạy lúc 02:00 giờ Việt Nam và gọi worker có request ID chống tạo job lặp. Worker tạo run mới, chụp dữ liệu clean đã commit, xây lại lake, đánh giá và công bố model. Không mount Docker socket vào Airflow; worker chỉ có tác vụ retrain định kiểu, không API thực thi shell tùy ý.

Model run là phiên bản phục vụ; data run là nguồn ingestion còn nhận event.
Đổi model pointer không đổi RunId mà producer đang sử dụng. Bản sửa cuối
truy ngược `source_data_run_id`/`source_run_id` về canonical ingestion run,
bao gồm qua nhiều thế hệ retrain. Runner lấy clean từ nguồn này cho lần train
sau; API hợp lịch sử seen của model run và ingestion run, rồi lọc lại cả cache
hit. Vòng lineage, nguồn sai danh tính hoặc đường dẫn lỗi được từ chối. Nếu
không giữ quan hệ này, một snapshot model mới có thể tiếp tục bỏ lỡ event đang
đến ở nguồn gốc dù các kho online vẫn được cập nhật.

Airflow manual run đã thật sự hoàn tất, cả hai task success và mười phép kiểm
API/latest pointer/registry đạt. Snapshot retrain đó chỉ có 9.999 event, được
chụp trước khi phát thêm view nói trên. Nó không chứng minh retrain đã học
event thứ 10.000, cũng không chứng minh bản sửa lineage sau này đã chạy toàn
luồng trên Docker. Test helper/API riêng và bằng chứng Docker cũ được tách
trong báo cáo kiểm thử để tránh gộp các mốc mã khác nhau.

## Dữ liệu thiếu và giới hạn thực tế

RetailRocket trong gói không có review thật, giá sản phẩm, impression hoặc item-properties nối danh mục. Topic review được hỗ trợ bằng fixture có nhãn tổng hợp. Dashboard trả CTR, doanh thu và AOV là không có dữ liệu; không dựng giá giả. Tỷ lệ visitor mua hàng được định nghĩa bằng số visitor có transaction chia tất cả visitor quan sát, khác tỷ lệ purchase/view theo sự kiện.

Các quy tắc validation không phải bộ phát hiện bot: nguồn không có IP/user-agent để chứng minh loại bot. Topology một Kafka broker, một DataNode replication=1, Spark local[2] và Airflow SQLite/SequentialExecutor dùng cho demo học tập; chưa kiểm thử chịu lỗi đa máy hoặc tải production. Lịch Airflow chỉ hoạt động khi container còn chạy. Dữ liệu gốc kèm ZIP không đồng nghĩa toàn bộ 2,75 triệu dòng đã được dùng để huấn luyện trong kết quả mẫu.

## Cách đọc bằng chứng

`outputs/evidence/` ghi checksum dữ liệu, kết quả test, Kafka CLI và runtime
Docker. Các kết quả chụp để bàn giao nằm ở `example_results/<run_id>/metrics/`;
lần chạy mới sinh dưới `outputs/runs/<run_id>/metrics/`. Producer ACK,
consumer, Spark progress, KPI, model metrics, ACID, E2E và benchmark mô tả các
bước khác nhau, không phải tất cả tệp đều là một snapshot cùng thời điểm.

52 test Docker của sáu module thuộc phiên bản đã chạy trước bản sửa lineage
cuối. Hai lượt native bổ sung xác nhận sáu test helper và 16 test API
TestClient đạt; 16 test API gồm test cũ chạy lại và hai regression lineage mới,
không phải 16 test mới. Lượt API dùng Python 3.12 host và Redis/Mongo mock ở
biên, có kiểm tra bản sửa đóng SQLite read handle trên Windows. Các lượt này
chỉ chứng minh phạm vi helper/API, không thay cho rebuild và full E2E.
Docker Desktop UI
vẫn hiển thị chạy và context đọc được, nhưng daemon không phản hồi ở lần
kiểm tra cuối, nên chưa thể xác nhận toàn runtime cho bản nguồn mới. Báo cáo
giữ cả callback live failed và giới hạn chưa chạy lại, không diễn giải các
bước thành công khác như đã xóa được mọi trạng thái lỗi. Những con số đo
luôn đi kèm dữ liệu, số request, concurrency và phạm vi thực hiện.
