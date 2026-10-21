# Phase 1 — Lựa chọn dữ liệu và thiết lập môi trường

Người thực hiện: Nguyễn Trương Minh Quân.
Đề tài: Phân tích hành vi người dùng thương mại điện tử và hệ thống gợi ý sản phẩm.

## Nội dung bàn giao

- `Phase1(dataset + setup).docx`: báo cáo tiểu luận về lựa chọn dữ liệu và thiết lập môi trường.
- `Dockerfile`, `compose.yaml`, `requirements.txt`: cấu hình Python, Spark và Kafka trên Docker.
- `scripts/`: chương trình thu thập, thống kê, gửi và xử lý sự kiện.
- `run_demo.ps1`: kịch bản kiểm thử trên PowerShell.
- `data/raw/`: dữ liệu sự kiện, cây danh mục và thông tin nguồn.
- `data/sample/events_10000.csv`: mẫu 10.000 sự kiện phục vụ kiểm thử môi trường.
- `outputs/data_profile.json`: thống kê toàn bộ tệp sự kiện.
- `outputs/evidence/`: kiểm kê dữ liệu, đối chiếu tệp và thông tin môi trường.
- `outputs/run-20261008-175527-9588cb/`: kết quả kiểm thử 10.000 sự kiện, gồm đối chiếu, nhật ký và dữ liệu Parquet.

## Dữ liệu

Nguồn: https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset
Giấy phép ghi trong metadata nguồn: CC BY-NC-SA 4.0. Giữ thông tin nguồn khi sử dụng hoặc chia sẻ dữ liệu.

Tệp events.csv có 2.756.101 sự kiện. Mẫu 10.000 dòng được chọn theo thời gian để kiểm thử kết nối, không phải mẫu đại diện dùng đánh giá mô hình.
Hai tệp item_properties không cần cho luồng kiểm thử và không kèm trong gói bàn giao này. Có thể tải bổ sung bằng chương trình thu thập với tùy chọn `--all`.

## Chạy kiểm thử

Máy cần Docker Desktop hoạt động ở chế độ Linux containers và kết nối Internet để tải image, thư viện và Kafka connector. Mở PowerShell ngay trong thư mục chứa README này, rồi chạy:

```powershell
docker version
docker compose config --quiet
docker compose build app
powershell -NoProfile -ExecutionPolicy Bypass -File .\run_demo.ps1 -Count 10000
```

Dữ liệu đầu vào và mẫu đã có sẵn. Kịch bản khởi động Kafka, tạo topic, gửi sự kiện và chạy Spark; mỗi phép thử tạo một thư mục kết quả riêng dưới `outputs/`.

Kết quả mong đợi: gửi 10.000, nhận 10.000, có 10.000 định danh sự kiện khác nhau, không có dòng vi phạm bộ kiểm tra và `passed: true` trong `verification.json`.

Dừng dịch vụ sau khi dùng:

```powershell
docker compose stop
```

Nếu cần tạo lại mẫu và thống kê từ dữ liệu nguồn:

```powershell
docker compose run --rm app python scripts/profile_data.py
```

Nếu cần tải thêm thuộc tính sản phẩm:

```powershell
docker compose run --rm app python scripts/download_data.py --all
```

Môi trường dùng Python 3.11.13, PySpark 3.5.6, Java 17, kafka-python 2.2.15 và Kafka 3.9.1. Kafka có một broker; Spark chạy local[2]. Kết quả thử nghiệm xác nhận kết nối và tính nhất quán dữ liệu, chưa đánh giá hiệu năng hay khả năng chịu lỗi đa nút.
