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
Lưu ý nên tải các bản dưới phù hợp với JDK 8
- Python 3.6+
- Kafka bản 3.3.1
- Hadoop 3.3.4
- Spark 3.3.1
- Elasticsearch 8.6.0
- Apache Hive 3.1.3
- Kibana 8.6.0
- Flask

## Gợi ý cài đặt

1. Cài đặt Apache Kafka
Kafka là nền tảng xử lý dữ liệu theo luồng phân tán, được sử dụng để thu thập dữ liệu theo thời gian thực.
```bash
# Cài đặt Java (yêu cầu cho Kafka)
sudo apt update
sudo apt install -y openjdk-11-jdk

# Tải Kafka
wget https://downloads.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
tar -xzf kafka_2.13-3.3.1.tgz
cd kafka_2.13-3.3.1

# Khởi động Zookeeper (yêu cầu cho Kafka) 
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Khởi động Kafka server
bin/kafka-server-start.sh config/server.properties &

# Tạo topic cho dữ liệu Shopee
bin/kafka-topics.sh --create --topic shopee-products --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic shopee-reviews --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic shopee-user-behaviors --bootstrap-server localhost:9092
```

2.Cài đặt Hadoop và HDFS
Kafka là nền tảng xử lý dữ liệu theo luồng phân tán, được sử dụng để thu thập dữ liệu theo thời gian thực.
```bash
# Cài đặt SSH và các gói cần thiết
sudo apt install -y ssh rsync

# Tải Hadoop
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar -xzf hadoop-3.3.4.tar.gz
cd hadoop-3.3.4

# Cấu hình Hadoop
# Chỉnh sửa các file cấu hình: core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml

# Định dạng HDFS
bin/hdfs namenode -format

# Khởi động HDFS
sbin/start-dfs.sh

# Tạo thư mục trên HDFS
bin/hdfs dfs -mkdir -p /user/shopee/data
bin/hdfs dfs -mkdir -p /user/shopee/products
bin/hdfs dfs -mkdir -p /user/shopee/reviews
bin/hdfs dfs -mkdir -p /user/shopee/user-behaviors
```
3. Cài đặt Apache Spark
Spark được sử dụng để xử lý dữ liệu theo batch và real-time, cũng như cho các thuật toán gợi ý.
```bash
# Tải Spark
wget https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
tar -xzf spark-3.3.1-bin-hadoop3.tgz
cd spark-3.3.1-bin-hadoop3

# Khởi động Spark master
sbin/start-master.sh

# Khởi động Spark worker
sbin/start-worker.sh spark://hostname:7077

# Cài đặt PySpark
pip install pyspark==3.3.1

```
4. Cài đặt Elasticsearch
Elasticsearch được sử dụng để lưu trữ và tìm kiếm dữ liệu nhanh chóng.
```bash
# Cài đặt Elasticsearch
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.0-linux-x86_64.tar.gz
tar -xzf elasticsearch-8.6.0-linux-x86_64.tar.gz
cd elasticsearch-8.6.0

# Khởi động Elasticsearch
bin/elasticsearch

# Tạo index cho dữ liệu Shopee
curl -X PUT "localhost:9200/shopee-products" -H "Content-Type: application/json" -d'
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 2
  },
  "mappings": {
    "properties": {
      "product_id": { "type": "keyword" },
      "name": { "type": "text" },
      "price_numeric": { "type": "float" },
      "rating": { "type": "float" },
      "category": { "type": "keyword" },
      "brand": { "type": "keyword" },
      "description": { "type": "text" }
    }
  }
}'

```

5. Cài đặt Apache Hive
Hive được sử dụng để truy vấn dữ liệu trên HDFS bằng SQL.
```bash
# Tải Hive
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz
cd apache-hive-3.1.3-bin

# Cấu hình Hive
# Chỉnh sửa hive-site.xml

# Khởi tạo schema
bin/schematool -dbType derby -initSchema

# Khởi động Hive Metastore
bin/hive --service metastore &

# Khởi động HiveServer2
bin/hiveserver2 &

# Tạo bảng Hive
bin/beeline -u jdbc:hive2://localhost:10000
CREATE DATABASE shopee;
USE shopee;
CREATE TABLE products (
  product_id STRING,
  name STRING,
  price_numeric FLOAT,
  rating FLOAT,
  category STRING,
  brand STRING,
  description STRING
)  STORED AS PARQUET;

```
6. Cài đặt Kibana (cho trực quan hóa dữ liệu)
```bash
# Tải Kibana
wget https://artifacts.elastic.co/downloads/kibana/kibana-8.6.0-linux-x86_64.tar.gz
tar -xzf kibana-8.6.0-linux-x86_64.tar.gz
cd kibana-8.6.0-linux-x86_64

# Khởi động Kibana
bin/kibana

```
7. Cài đặt Flask (cho giao diện web)
```bash
# Cài đặt Flask và các thư viện cần thiết
pip install flask flask-cors requests pandas numpy scikit-learn

```
8. Kết nối các thành phần
Đảm bảo các thành phần kết nối được với nhau(Nên dùng Docker)

8.1. Kết nối Kafka với Spark Streaming
   file kafka_to_spark.py
   
8.2. Kết nối Spark với Elasticsearch
   file spark_to_elasticsearch.py
   
8.3. Triển khai thuật toán gợi ý với Spark MLlib
   file recommendation_engine.py
   
8.4. Xây dựng giao diện web với Flask
   file app.py
   
10. Tạo script khởi động hệ thống
    file start_system.sh
11. Tạo script thu thập dữ liệu
    file collect_data.py
12. Tạo script phân tích dữ liệu
    file analyze_data.py
13. Tạo script đánh giá thuật toán gợi ý
    file evaluate_recommendations.py
