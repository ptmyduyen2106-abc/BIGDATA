# Phase 5 — Spark MLlib ALS và đánh giá gợi ý

## Đầu vào và kết quả

`pipeline.train.train_model(spark, run_root, run_id, quick=True, top_k=10)` đọc Delta Silver qua `pipeline.config.lake_path`. Trong Compose, nguồn là HDFS `hdfs://namenode:8020/ecommerce/runs/<run_id>/delta/silver`; khi không đặt `LAKE_BASE_URI`, nguồn là `<run_root>/delta/silver` để kiểm thử cục bộ. Hàm chỉ đọc các sự kiện đã được Phase 3–4 xác thực. `event_id`/`eventid` loại trùng ổn định trước khi huấn luyện.

Hành vi được quy thành độ mạnh tương tác: `view=1`, `addtocart=3`, `transaction=5`. Review không có trong RetailRocket gốc và không tham gia tín hiệu ALS. Các ID phải là số nguyên không âm trong miền 32 bit; hệ thống kiểm tra trước khi cast, giữ nguyên ID cho API, không âm thầm cắt tràn hoặc đổi mã.

Pipeline Spark ML thực sự có hai stage:

1. `VectorAssembler` ghép `view_count`, `cart_count`, `purchase_count` thành `interaction_features`, phục vụ kiểm tra đặc trưng và lưu cùng pipeline.
2. `ALS(implicitPrefs=True)` học ma trận nhân tố từ `visitorid`, `itemid`, `rating`; `rating=view_count+3*cart_count+5*purchase_count`.

ALS không nhận vector `interaction_features` làm đầu vào. VectorAssembler và ALS tồn tại trong cùng Pipeline đúng API MLlib; ALS sử dụng ba cột mà thuật toán yêu cầu. Không dùng RandomForest hoặc nhãn rating giả để thay thế bài toán gợi ý tương tác ngầm định.

## Chia thời gian và chọn tham số

Các mốc xấp xỉ phân vị 50%, 70%, 85% lấy từ **timestamp của sự kiện thô đã làm sạch, trước khi tổng hợp user-item**. Mọi sự kiện cùng timestamp ở cùng một phía của ranh giới. Các khoảng đều dùng quy tắc `train <= mốc`, `validation > mốc trước và <= mốc sau`; do đó không có timestamp tương lai lọt vào train của fold tương ứng.

| Bước | Dữ liệu fit | Dữ liệu đánh giá |
|---|---|---|
| Rolling fold 1 | Đầu nguồn đến mốc 50% | Sau 50% đến 70% |
| Rolling fold 2 | Đầu nguồn đến mốc 70% | Sau 70% đến 85% |
| Đánh giá cuối | Đầu nguồn đến mốc 85% | Sau mốc 85%, chưa dùng để chọn tham số |
| Fit phục vụ API | Toàn bộ sự kiện quan sát được | Không tính lại kết quả test trên model này |

Mỗi fold tự tổng hợp tương tác từ phần train của mình. Đặc trưng/gold interactions của toàn dữ liệu không được dùng làm đầu vào cho fold sớm hơn. Mô hình được chọn theo trung bình NDCG@K qua **hai fold hợp lệ**, dùng Recall@K làm tiêu chí phụ; thứ tự grid cố định giải quyết trường hợp hòa.

- Quick: rank 8; regularization 0.05 và 0.1; alpha 20. Có 2 ứng viên × 2 fold = 4 lần fit chọn tham số, mỗi lần 3 iteration. Model đánh giá cuối và model phục vụ chạy 5 iteration mỗi model.
- Full: rank 8/16 × regularization 0.05/0.1 × alpha 10/20 = 8 ứng viên × 2 fold = 16 lần fit; cộng model đánh giá và model phục vụ, mỗi lần 10 iteration. Chi phí lớn hơn; không tự động coi chạy mẫu quick là bằng chứng đã huấn luyện toàn nguồn.
- Seed cố định 42. Bản ghi `cross_validation` lưu tham số và kết quả của từng fold; `selected_parameters` ghi cấu hình được chọn.

Hàm báo lỗi nếu dữ liệu quá ít, mốc thời gian không tách được, test rỗng hoặc không có đích validation mới trong cả hai fold. Không tự tạo tương tác hoặc điểm đánh giá để hoàn thành báo cáo.

## Chỉ số xếp hạng đúng bài toán

Test đo khả năng gợi ý **item mới với người dùng**. Các cặp đã xuất hiện trong lịch sử train được bỏ khỏi target, vì API chủ động loại item đã xem. `excluded_repeat_pairs` báo số cặp bị loại. User/item chưa được học trong train vẫn ở mẫu test và được tính là miss; `cold_users` và `cold_item_pairs` chỉ rõ giới hạn này.

- Precision@K = số item gợi ý đúng / K. Danh sách ngắn vẫn dùng mẫu số K.
- Recall@K = số item gợi ý đúng / số item liên quan mới của user trong holdout.
- NDCG@K dùng relevance nhị phân và chiết khấu `1/log2(vị trí+1)`, chuẩn hóa theo xếp hạng lý tưởng của user.
- Catalog coverage = số item train xuất hiện trong kết quả gợi ý / kích thước catalog của model train.
- User coverage = số user có ít nhất một gợi ý / số user có đích holdout mới.

Các chỉ số là trung bình theo user, bao gồm cold user bị miss. Không báo RMSE cho nhãn ngầm định, không bỏ cold user để làm điểm đẹp hơn, không dùng điểm confidence ALS như xác suất mua. Nếu không còn target mới, trạng thái là `not_evaluable_no_novel_targets` và chỉ số để `null`.

Dữ liệu mẫu 10.000 dòng có thể rất thưa, nhiều người chỉ tương tác một lần và nhiều item mới ở đoạn sau; chỉ số có thể bằng 0 hoặc coverage thấp. Đó là kết quả cần trình bày trung thực. Chỉ số offline không chứng minh tăng doanh thu hay hiệu quả A/B test thực tế.

## Gợi ý, lưu model và bộ nhớ

MLlib tính top-N từ nhân tố đã học. Để loại item đã xem mà vẫn lấy đủ top-K, user đã thấy `s` item cần tối đa `K+s` ứng viên thô; hệ thống chia user theo các mức lũy thừa hai và chặn ở kích thước catalog. User ít lịch sử không bị áp cùng ngân sách ứng viên với user hoạt động nhiều nhất. Sau khi loại seen, kết quả sắp theo score giảm dần, ID tăng dần và lấy K; điểm NaN/vô hạn không được xuất.

Các file được tạo dưới thư mục run:

| Đường dẫn | Nội dung |
|---|---|
| `models/evaluation_pipeline/` | Pipeline fit trước thời điểm test; dùng cho kết quả đánh giá |
| `models/als_pipeline/` | Pipeline cuối fit toàn dữ liệu quan sát, phục vụ hệ thống |
| `models/recommendations/` | Parquet phân tán của toàn bộ user được ALS học: user, item, score, rank |
| `metrics/model_metrics.json` | Chia thời gian, grid, fold, tham số chọn, held-out metrics và phạm vi phục vụ |
| `serving/model_bundle.json` | Metadata và gợi ý JSON cho tối đa 5.000 user mặc định |
| `serving/seen_items.sqlite` | Chỉ mục seen cho **toàn bộ user/item đã quan sát trong Silver**, gồm cả đoạn holdout |

Sau khi đóng băng test metrics, model phục vụ được fit lại toàn nguồn để dùng tương tác mới nhất. `metric_model_version` và `serving_model_version` phân biệt hai model này. Việc dùng toàn bộ lịch sử cho **phục vụ sau đánh giá** không được đưa ngược vào phép đo test.

`BIGDATA_SERVING_MAX_USERS` đổi giới hạn số user xuất gợi ý JSON (mặc định 5000). JSON chỉ chứa lịch sử của nhóm user xuất này; SQLite chứa lịch sử của mọi user, được ghi bằng iterator/batch 1.000 cặp nên không gom toàn bộ lịch sử vào driver. API truy vấn sidecar để fallback cho user ngoài phạm vi JSON và vẫn loại toàn bộ item đã thấy. User mới dùng mức phổ biến toàn nguồn. Danh sách phổ biến có tối đa `max(1000,20*K)` item, nên fallback có thể ít hơn K nếu người dùng đã thấy hết nhóm này.

Model và index SQLite được ghi xong trước khi file bundle được đổi tên nguyên tử để công bố. Runner chỉ chuyển `latest_run` sau khi mọi bước của run thành công. Training từ chối ghi đè model/bundle đã tồn tại; retrain cần run ID mới để giữ phiên bản có thể kiểm tra lại.

## Kiểm chứng

Kiểm thử thuần Python kiểm tra giá trị metric đã biết, cold/missing prediction, chống đếm ID trùng, ranh giới thời gian, grid có nhiều ứng viên và SQLite đầy đủ lịch sử user ngoài phạm vi JSON:

```powershell
python -m unittest discover -s tests -p test_training.py -v
```

Kiểm thử Spark thật là opt-in, chạy trong image có Java/PySpark:

```powershell
docker compose run --rm -e BIGDATA_RUN_SPARK_ML_TESTS=1 app python -m unittest discover -s tests -p test_training.py -v
```

Kiểm thử này thực sự fit ALS, sinh gợi ý không trùng seen, kiểm tra chỉ số với cold user/item, lưu Pipeline và nạp lại. Chạy kiểm thử thuần Python chỉ chứng minh các hàm thuần và hợp đồng; không được ghi nhận đó là bằng chứng toàn pipeline huấn luyện thành công. Bằng chứng chạy end-to-end, số dữ liệu và model metrics thực tế nằm trong thư mục run/evidence do runner tạo.
