# Kiến trúc hệ thống đánh giá, tiềm năng và gợi ý sản phẩm từ dữ liệu Shopee

## Tổng quan kiến trúc

Hệ thống được xây dựng theo mô hình Lambda Architecture, kết hợp xử lý batch và real-time để đảm bảo tính chính xác và hiệu suất cao. Kiến trúc này cho phép hệ thống xử lý lượng lớn dữ liệu từ Shopee, đồng thời cung cấp khả năng phân tích và đề xuất sản phẩm theo thời gian thực.

```
+----------------+     +----------------+     +----------------+
|                |     |                |     |                |
|  Data Sources  |---->|  Speed Layer   |---->|  Serving Layer |
|  (Web Scraper) |     |  (Real-time)   |     |                |
|                |     |                |     |                |
+----------------+     +----------------+     +----------------+
        |                                            ^
        v                                            |
+----------------+     +----------------+            |
|                |     |                |            |
|  Batch Layer   |---->|  Batch Views   |------------+
|                |     |                |
|                |     |                |
+----------------+     +----------------+
```

## Các thành phần chính

### 1. Thu thập dữ liệu (Data Collection)
- **Web Scraper**: Sử dụng BeautifulSoup hoặc Selenium để thu thập dữ liệu sản phẩm từ Shopee
- **Kafka**: Hệ thống message broker để xử lý dữ liệu theo thời gian thực
  - Kafka Producers: Gửi dữ liệu từ scraper đến Kafka
  - Kafka Consumers: Đọc dữ liệu từ Kafka và chuyển vào hệ thống xử lý

### 2. Lưu trữ và xử lý dữ liệu (Storage & Processing)
- **HDFS**: Hệ thống lưu trữ dữ liệu phân tán cho dữ liệu lâu dài
- **Spark Streaming**: Xử lý dữ liệu theo thời gian thực (Speed Layer)
- **Hadoop/MapReduce**: Xử lý dữ liệu theo batch (Batch Layer)
- **Elasticsearch**: Tối ưu truy vấn và tìm kiếm dữ liệu nhanh chóng

### 3. Truy vấn dữ liệu (Query System)
- **Spark SQL**: Truy vấn dữ liệu linh hoạt
- **Hive/Impala**: Tối ưu truy vấn
- **Serving Layer**: Cung cấp dữ liệu tối ưu
  - Batch View: Kết quả từ xử lý batch
  - Real-time View: Kết quả từ xử lý real-time

### 4. Phân tích và hiển thị dữ liệu (Analytics & Visualization)
- **Tableau/Kibana**: Trực quan hóa dữ liệu
- **Dashboard**: Hiển thị
  - Xu hướng sản phẩm hot
  - Phân tích đánh giá sản phẩm
  - Gợi ý sản phẩm theo hành vi người dùng

### 5. Hệ thống gợi ý sản phẩm (Recommendation System)
- **Spark MLlib**: Triển khai thuật toán
  - Collaborative Filtering (ALS)
  - Hybrid Methods (kết hợp Content-Based)
  - Matrix Factorization (SVD, NMF)
- **Huấn luyện mô hình**: Trên dữ liệu người dùng và cập nhật theo thời gian thực
- **Tích hợp**: Kết quả vào Elasticsearch để hiển thị nhanh chóng

### 6. Giao diện người dùng (User Interface)
- **Web Application**: Giao diện web để người dùng tương tác với hệ thống
  - Frontend: Hiển thị kết quả và gợi ý sản phẩm
  - Backend: Xử lý yêu cầu và tương tác với hệ thống phân tích

## Luồng dữ liệu

1. Thu thập dữ liệu sản phẩm từ Shopee thông qua web scraper
2. Dữ liệu được đẩy vào Kafka để xử lý theo thời gian thực
3. Dữ liệu được lưu trữ trong HDFS cho xử lý batch
4. Spark Streaming xử lý dữ liệu real-time từ Kafka
5. Hadoop/MapReduce xử lý dữ liệu batch từ HDFS
6. Kết quả xử lý được lưu trong Elasticsearch
7. Spark SQL và Hive/Impala được sử dụng để truy vấn dữ liệu
8. Spark MLlib huấn luyện mô hình gợi ý sản phẩm
9. Tableau/Kibana hiển thị kết quả phân tích
10. Giao diện web hiển thị kết quả cho người dùng cuối

## Công nghệ sử dụng

- **Ngôn ngữ lập trình**: Python, Java, JavaScript
- **Thu thập dữ liệu**: BeautifulSoup, Selenium
- **Xử lý dữ liệu**: Apache Kafka, Apache Spark, Hadoop
- **Lưu trữ dữ liệu**: HDFS, Elasticsearch
- **Truy vấn dữ liệu**: Spark SQL, Hive, Impala
- **Machine Learning**: Spark MLlib
- **Trực quan hóa**: Tableau, Kibana
- **Giao diện web**: React/Angular, Node.js, Flask/Django

## Ưu điểm của kiến trúc

1. **Khả năng mở rộng**: Hệ thống có thể xử lý lượng lớn dữ liệu và dễ dàng mở rộng
2. **Xử lý real-time**: Cung cấp kết quả phân tích và gợi ý theo thời gian thực
3. **Độ chính xác cao**: Kết hợp xử lý batch và real-time để đảm bảo độ chính xác
4. **Linh hoạt**: Dễ dàng thay đổi và cập nhật các thành phần của hệ thống
5. **Khả năng phục hồi**: Hệ thống có khả năng phục hồi cao khi gặp sự cố
