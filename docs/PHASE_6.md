# Phase 6 — Recommendation API và dashboard

FastAPI phục vụ model bundle do job ALS Phase 5 thực sự huấn luyện và xuất. Dashboard tiếng Việt chạy cùng API, dùng HTML/CSS/JavaScript cục bộ, không cần CDN. Không có bước xuất bản lên Internet trong gói này.

## Chạy dịch vụ

Thực hiện pipeline theo README gốc để tạo model và KPI. Sau đó khởi động service API trong Compose, mở địa chỉ API/dashboard được README công bố. Khi dùng Python đã cài `requirements.txt`:

```powershell
$env:OUTPUT_ROOT = 'outputs'
python -m uvicorn service.app:app --host 127.0.0.1 --port 8000
```

Mở `http://localhost:8000/`; tài liệu tương tác OpenAPI tại `/docs`. `OUTPUT_ROOT` trỏ tới thư mục `outputs`, không trỏ vào thư mục của một run. Bỏ `RUN_ID` để theo `outputs/latest_run.json`; đặt biến này để cố định một run:

```powershell
$env:RUN_ID = 'demo-01'
```

API không tự tạo model giả khi file chưa tồn tại. `/health/live` vẫn trả 200; `/health/ready` và các API cần model trả 503 cho tới khi bundle hợp lệ sẵn sàng.

## API

| Route | Hành vi |
| --- | --- |
| `GET /health/live` | Kiểm tra tiến trình HTTP còn hoạt động |
| `GET /health/ready` | 200 khi có model hợp lệ; 503 nếu model hoặc history sidecar bắt buộc không dùng được. Trạng thái Redis/MongoDB trình bày riêng |
| `GET /api/recommendations/{visitorid}?k=10` | Gợi ý có lọc lịch sử; visitor là số nguyên 0…2.147.483.647; k là số nguyên 1…100 |
| `GET /api/users?limit=20` | Danh sách user có gợi ý cá nhân hóa xuất sẵn trong đúng bundle hiện tại, limit 1…200 |
| `GET /api/metrics` | KPI của đúng run và metric huấn luyện/held-out từ bundle, không sinh số liệu minh họa |
| `GET /` | Dashboard cục bộ; gọi trực tiếp các route phía trên |

Mỗi response gợi ý có `run_id`, `model_version`, `trained_at_utc`, `algorithm`, `source`, `cache_hit`, `history_sources`, `model_stale`, `warnings` và `request_ms`. `source=als_bundle` dùng danh sách ALS xuất sẵn, có thể bổ sung sản phẩm phổ biến khi thiếu ứng viên. `source=popular_fallback` áp dụng cho user chưa có danh sách xuất sẵn. Nếu tập ứng viên còn lại nhỏ hơn k thì trả ít hơn k; không đưa lại sản phẩm đã thấy để đủ số.

`request_ms` là thời gian xử lý handler API, không phải độ trễ Kafka→Spark hoặc SLA. Dashboard đo riêng thời gian yêu cầu end-to-end bằng `performance.now()` trong trình duyệt, hiển thị tối đa 20 lần truy vấn gợi ý vừa thực hiện; biểu đồ trống khi chưa có phép đo.

## Model và reload an toàn

File được đọc tại:

```text
outputs/latest_run.json
outputs/runs/<run_id>/serving/model_bundle.json
outputs/runs/<run_id>/serving/seen_items.sqlite
outputs/runs/<run_id>/metrics/kpis.json
```

Bundle có `run_id`, `model_version`, `trained_at_utc`, `algorithm='als_implicit'`, `recommendations`, `seen_items`, `popular_items`, `metrics`; `seen_items_store='seen_items.sqlite'` khai báo history sidecar. ID phải là int32 không âm, điểm phải hữu hạn và danh sách sản phẩm không trùng. Model bundle chỉ kích hoạt sau khi đọc xong và xác thực toàn bộ, dùng khóa để các request không nhìn thấy trạng thái nửa cập nhật. Job xuất phải ghi file tạm rồi rename/replace atomically, công bố history trước bundle và latest pointer. Một model version được xem là bất biến; model mới phải có version mới để cache không tái sử dụng điểm cũ.

Khi file cập nhật của **cùng run** hỏng, API có thể giữ bản hợp lệ trước đó và trả `model_stale=true` cùng phiên bản thực đang phục vụ. Nếu latest pointer chuyển sang **run khác** chưa có model hợp lệ, API trả 503; không gắn kết quả run trước vào tên run mới. Pointer không đọc được cũng không được suy đoán thành run cũ. Thiếu history sidecar đã khai báo làm readiness thất bại thay vì trả fallback không lọc đủ lịch sử.

Danh sách gợi ý JSON có giới hạn xuất mặc định 5.000 user, cấu hình tại job Phase 5. User ngoài giới hạn nhận baseline phổ biến. Sidecar SQLite giữ lịch sử **toàn bộ user đã quan sát**, kể cả user không có gợi ý xuất sẵn, để baseline vẫn loại sản phẩm đã thấy. API mở SQLite read-only, truy vấn tham số hóa theo visitor; không nhận tên file tùy ý từ request. Metric đánh giá được tính trước bước refit phục vụ trên toàn bộ dữ liệu; `metric_model_version` phân biệt model đánh giá và model phục vụ khi job cung cấp trường này.

## Redis và MongoDB tùy chọn

Biến môi trường `REDIS_URL`, `MONGO_URI` (hoặc `MONGODB_URI`) và `MONGO_DB` cấu hình kết nối. DB mặc định `bigdata`.

| Nội dung | Hợp đồng |
| --- | --- |
| Lịch sử Redis | SET `bigdata:{run_id}:seen:{visitorid}` chứa item ID |
| Lịch sử MongoDB | Collection `user_features`, truy vấn bằng cả `run_id` và `visitorid`, trường `seen_items` |
| Cache gợi ý | `bigdata:{run_id}:recommendations:{model_version}:{visitorid}:{k}`; JSON tự mang run/model/user/k; TTL 60 giây |

API hợp lịch sử từ bundle, SQLite và các kho online còn kết nối. Luôn lọc lại lịch sử hiện tại **kể cả khi cache hit**, nên sản phẩm vừa được tương tác trong Redis/MongoDB không tiếp tục xuất hiện vì còn cache. Cache khác run/version/user/k, nội dung lỗi hoặc điểm không hữu hạn bị bỏ qua. Khi Redis hỏng, API dùng bundle cục bộ, nêu `cache_hit=false` và cảnh báo; không trả dữ liệu của run khác. MongoDB/Redis tùy chọn bị gián đoạn không tự làm model readiness thành 503, nhưng `/health/ready` hiển thị trạng thái degraded. Lịch sử online chưa đồng bộ khi dịch vụ gián đoạn không thể được API suy đoán; response nêu giới hạn này thay vì gọi dữ liệu là hoàn toàn mới nhất.

## Đọc dashboard đúng ý nghĩa

Dashboard có tổng sự kiện, visitor/item phân biệt, số sự kiện theo loại, gợi ý cho user mẫu hoặc ID nhập tay, thông tin model, metric held-out và trạng thái kết nối. Mọi số liệu được đọc từ file KPI/bundle của run đang chọn; thiếu KPI hiển thị “—”, không dùng số giả.

`purchase_per_view` là tỷ lệ **số sự kiện mua / số sự kiện xem**, có thể lớn hơn 1; không phải CTR hay tỷ lệ chuyển đổi người dùng duy nhất. RetailRocket không có giá/giá trị đơn và mẫu số impressions quảng cáo, nên doanh thu/AOV/CTR chuẩn chưa có dữ liệu. Score ALS/popularity là điểm xếp hạng, không phải xác suất mua. Dashboard hiển thị item ID vì dataset hiện tại không cung cấp tên/ảnh sản phẩm.

## Kiểm thử

```powershell
python -m unittest discover -s tests -p test_service.py -v
```

`tests/test_service.py` dùng FastAPI TestClient thật, bundle/SQLite tạm và Redis giả ở biên giao tiếp. Kiểm tra readiness, schema, giới hạn ID/k, fallback user lạ, loại item đã thấy, sidecar user ngoài giới hạn, stale reload cùng run, chặn leak khi đổi run, cache version, lọc live history trên cache hit và Redis down. Đây là kiểm thử logic dịch vụ; không được coi Redis giả là bằng chứng đã chạy Redis/MongoDB ngoài hệ thống. Báo cáo kiểm thử Phase 7 ghi các phần đã chạy thực tế.
