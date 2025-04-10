# Shopee-analyze-and-recommend-system
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tài liệu hướng dẫn sử dụng hệ thống gợi ý sản phẩm Shopee

Tài liệu này cung cấp hướng dẫn chi tiết về cách sử dụng hệ thống gợi ý sản phẩm Shopee.
"""

# Hướng dẫn sử dụng hệ thống

## Giới thiệu

Hệ thống đánh giá, tiềm năng và gợi ý sản phẩm từ dữ liệu Shopee là một hệ thống toàn diện được xây dựng dựa trên kiến trúc Lambda. Hệ thống này cung cấp các chức năng sau:

1. Thu thập dữ liệu sản phẩm, đánh giá và hành vi người dùng từ Shopee
2. Lưu trữ và xử lý dữ liệu theo thời gian thực và theo batch
3. Phân tích dữ liệu để đánh giá sản phẩm và phát hiện tiềm năng
4. Gợi ý sản phẩm cho người dùng dựa trên nhiều thuật toán khác nhau
5. Hiển thị kết quả thông qua giao diện web thân thiện với người dùng

## Cấu trúc hệ thống

Hệ thống được tổ chức thành các thành phần sau:

```
shopee_system/
├── data_collection/           # Thu thập dữ liệu
│   ├── shopee_scraper.py      # Web scraper cho Shopee
│   ├── kafka_producer.py      # Kafka producer
│   └── kafka_consumer.py      # Kafka consumer
├── data_storage/              # Lưu trữ dữ liệu
│   ├── hdfs_manager.py        # Quản lý HDFS
│   └── elasticsearch_manager.py # Quản lý Elasticsearch
├── data_processing/           # Xử lý dữ liệu
│   ├── spark_streaming.py     # Xử lý real-time với Spark Streaming
│   └── batch_processing.py    # Xử lý batch với Hadoop/MapReduce
├── recommendation_algorithms/  # Thuật toán gợi ý
│   ├── collaborative_filtering.py # Collaborative Filtering
│   ├── matrix_factorization.py    # Matrix Factorization
│   └── hybrid_methods.py          # Hybrid Methods
├── query_system/              # Hệ thống truy vấn
│   ├── spark_sql_manager.py   # Quản lý Spark SQL
│   ├── hive_impala_manager.py # Quản lý Hive/Impala
│   └── serving_layer.py       # Serving Layer
├── web_interface/             # Giao diện web
│   ├── app.py                 # Ứng dụng Flask
│   ├── templates/             # Templates HTML
│   └── static/                # Tài nguyên tĩnh (CSS, JS, images)
├── test_system.py             # Kiểm thử hệ thống
├── optimize_system.py         # Tối ưu hóa hệ thống
└── start_system.py            # Khởi động hệ thống
```

## Yêu cầu hệ thống

- Python 3.6+
- Các thư viện Python: Flask, pandas, numpy, scikit-learn, beautifulsoup4, selenium, requests, kafka-python
- Các công cụ Big Data (tùy chọn): Hadoop, Spark, Kafka, Elasticsearch

## Cài đặt

1. Clone repository:
```bash
git clone https://github.com/yourusername/shopee_recommendation_system.git
cd shopee_recommendation_system
```

2. Cài đặt các thư viện cần thiết:
```bash
pip install -r requirements.txt
```

3. Cấu hình hệ thống:
- Chỉnh sửa các tham số trong các file cấu hình (nếu cần)

## Khởi động hệ thống

Để khởi động toàn bộ hệ thống, sử dụng lệnh sau:

```bash
python start_system.py
```

Các tùy chọn:
- `--host`: Host để chạy web interface (mặc định: 0.0.0.0)
- `--port`: Port để chạy web interface (mặc định: 5000)
- `--debug`: Chạy web interface trong chế độ debug
- `--threaded`: Chạy web interface trong chế độ threaded
- `--processes`: Số lượng process cho web interface (mặc định: 1)

Ví dụ:
```bash
python start_system.py --port 8080 --threaded
```

## Sử dụng hệ thống

### Thu thập dữ liệu

Để thu thập dữ liệu từ Shopee, sử dụng lệnh sau:

```bash
python data_collection/shopee_scraper.py
```

Các tùy chọn:
- `--category`: Danh mục sản phẩm cần thu thập
- `--limit`: Số lượng sản phẩm tối đa cần thu thập
- `--output`: Đường dẫn đến file đầu ra

### Xử lý dữ liệu

Để xử lý dữ liệu theo batch, sử dụng lệnh sau:

```bash
python data_processing/batch_processing.py
```

Để xử lý dữ liệu theo thời gian thực, sử dụng lệnh sau:

```bash
python data_processing/spark_streaming.py
```

### Gợi ý sản phẩm

Để tạo gợi ý sản phẩm, sử dụng lệnh sau:

```bash
python recommendation_algorithms/hybrid_methods.py
```

### Truy vấn dữ liệu

Để truy vấn dữ liệu, sử dụng lệnh sau:

```bash
python query_system/serving_layer.py
```

### Giao diện web

Để chạy giao diện web, sử dụng lệnh sau:

```bash
python web_interface/app.py
```

Sau đó, truy cập vào địa chỉ http://localhost:5000 để sử dụng giao diện web.

## Kiểm thử hệ thống

Để kiểm thử hệ thống, sử dụng lệnh sau:

```bash
python test_system.py
```

## Tối ưu hóa hệ thống

Để tối ưu hóa hệ thống, sử dụng lệnh sau:

```bash
python optimize_system.py
```

## Các trang trong giao diện web

### Trang chủ

Trang chủ hiển thị các sản phẩm phổ biến và danh mục sản phẩm. Người dùng có thể xem các sản phẩm nổi bật và chuyển đến các trang khác.

### Trang sản phẩm

Trang sản phẩm hiển thị danh sách các sản phẩm với các bộ lọc như danh mục, giá, đánh giá và sắp xếp. Người dùng có thể lọc và tìm kiếm sản phẩm theo nhu cầu.

### Trang chi tiết sản phẩm

Trang chi tiết sản phẩm hiển thị thông tin chi tiết về một sản phẩm, bao gồm mô tả, giá, đánh giá và phản hồi từ người dùng. Người dùng cũng có thể xem các sản phẩm tương tự.

### Trang gợi ý sản phẩm

Trang gợi ý sản phẩm hiển thị các sản phẩm được gợi ý cho người dùng dựa trên hành vi mua sắm và lịch sử tương tác. Người dùng có thể xem phân tích hành vi của mình.

### Trang so sánh thuật toán

Trang so sánh thuật toán hiển thị hiệu suất của các thuật toán gợi ý khác nhau cho người dùng. Người dùng có thể so sánh các thuật toán và xem chi tiết theo sản phẩm.

### Trang bảng điều khiển

Trang bảng điều khiển hiển thị các biểu đồ và thống kê về dữ liệu sản phẩm, đánh giá và hành vi người dùng. Người dùng có thể xem phân tích tổng quan về hệ thống.

## API

Hệ thống cung cấp các API sau:

### API sản phẩm

- `GET /api/products/top`: Lấy danh sách sản phẩm hàng đầu
- `GET /api/products/<product_id>`: Lấy thông tin chi tiết sản phẩm

### API gợi ý

- `GET /api/recommendations/<user_id>`: Lấy gợi ý sản phẩm cho người dùng
- `GET /api/algorithms/comparison/<user_id>`: So sánh các thuật toán gợi ý cho người dùng

### API danh mục

- `GET /api/categories`: Lấy danh sách danh mục sản phẩm

### API người dùng

- `GET /api/user/behavior/<user_id>`: Lấy phân tích hành vi người dùng
