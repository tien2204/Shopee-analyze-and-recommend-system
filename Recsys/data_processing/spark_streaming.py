#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark Streaming Manager cho dữ liệu Shopee

Mô-đun này quản lý việc xử lý dữ liệu theo thời gian thực bằng Spark Streaming.
"""

import os
import json
import logging
import time
from datetime import datetime
import findspark
try:
    findspark.init()
except:
    pass

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
except ImportError:
    print("PySpark hoặc các module liên quan không khả dụng. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_spark_streaming')

class ShopeeSparkStreaming:
    """
    Lớp ShopeeSparkStreaming quản lý việc xử lý dữ liệu theo thời gian thực bằng Spark Streaming.
    """
    
    def __init__(self, app_name='ShopeeSparkStreaming', master='local[*]', 
                 checkpoint_dir='/tmp/shopee_spark_checkpoint', 
                 kafka_bootstrap_servers='localhost:9092',
                 topic_prefix='shopee',
                 output_dir='../data/processed'):
        """
        Khởi tạo Spark Streaming với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            checkpoint_dir (str): Thư mục checkpoint cho Spark Streaming
            kafka_bootstrap_servers (str): Danh sách các Kafka broker
            topic_prefix (str): Tiền tố cho các Kafka topic
            output_dir (str): Thư mục đầu ra cho dữ liệu đã xử lý
        """
        self.app_name = app_name
        self.master = master
        self.checkpoint_dir = checkpoint_dir
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.output_dir = output_dir
        
        # Các topic Kafka
        self.topics = {
            'products': f"{topic_prefix}_products",
            'reviews': f"{topic_prefix}_reviews",
            'user_behavior': f"{topic_prefix}_user_behavior"
        }
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Khởi tạo Spark
        self._initialize_spark()
    
    def _initialize_spark(self):
        """
        Khởi tạo Spark và Spark Streaming.
        """
        try:
            # Tạo SparkConf
            conf = SparkConf().setAppName(self.app_name).setMaster(self.master)
            
            # Cấu hình bổ sung
            conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
            conf.set("spark.streaming.backpressure.enabled", "true")
            conf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
            
            # Khởi tạo SparkContext và StreamingContext
            self.sc = SparkContext(conf=conf)
            self.ssc = StreamingContext(self.sc, batchDuration=10)  # 10 giây mỗi batch
            self.ssc.checkpoint(self.checkpoint_dir)
            
            # Khởi tạo SparkSession
            self.spark = SparkSession(self.sc)
            
            logger.info(f"Đã khởi tạo Spark Streaming với master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def _create_kafka_stream(self, topics):
        """
        Tạo DStream từ Kafka.
        
        Args:
            topics (list): Danh sách các topic Kafka
            
        Returns:
            DStream: Kafka DStream
        """
        if self._simulate_spark:
            logger.warning("Đang mô phỏng Kafka stream.")
            return None
        
        try:
            # Tạo Kafka DStream
            kafka_params = {
                "bootstrap.servers": self.kafka_bootstrap_servers,
                "group.id": "spark_streaming_consumer",
                "auto.offset.reset": "largest"
            }
            
            stream = KafkaUtils.createDirectStream(
                self.ssc,
                topics,
                kafka_params,
                valueDecoder=lambda x: json.loads(x.decode('utf-8'))
            )
            
            return stream
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo Kafka stream: {e}")
            return None
    
    def process_product_stream(self):
        """
        Xử lý stream dữ liệu sản phẩm.
        """
        if self._simulate_spark:
            self._simulate_product_stream()
            return
        
        try:
            # Tạo stream từ Kafka
            product_stream = self._create_kafka_stream([self.topics['products']])
            
            # Trích xuất giá trị từ tin nhắn Kafka
            product_data = product_stream.map(lambda x: x[1])
            
            # Xử lý dữ liệu sản phẩm
            product_data.foreachRDD(self._process_product_rdd)
            
            logger.info("Đã thiết lập xử lý stream dữ liệu sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý stream dữ liệu sản phẩm: {e}")
    
    def process_review_stream(self):
        """
        Xử lý stream dữ liệu đánh giá.
        """
        if self._simulate_spark:
            self._simulate_review_stream()
            return
        
        try:
            # Tạo stream từ Kafka
            review_stream = self._create_kafka_stream([self.topics['reviews']])
            
            # Trích xuất giá trị từ tin nhắn Kafka
            review_data = review_stream.map(lambda x: x[1])
            
            # Xử lý dữ liệu đánh giá
            review_data.foreachRDD(self._process_review_rdd)
            
            logger.info("Đã thiết lập xử lý stream dữ liệu đánh giá")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý stream dữ liệu đánh giá: {e}")
    
    def process_user_behavior_stream(self):
        """
        Xử lý stream dữ liệu hành vi người dùng.
        """
        if self._simulate_spark:
            self._simulate_user_behavior_stream()
            return
        
        try:
            # Tạo stream từ Kafka
            behavior_stream = self._create_kafka_stream([self.topics['user_behavior']])
            
            # Trích xuất giá trị từ tin nhắn Kafka
            behavior_data = behavior_stream.map(lambda x: x[1])
            
            # Xử lý dữ liệu hành vi người dùng
            behavior_data.foreachRDD(self._process_user_behavior_rdd)
            
            logger.info("Đã thiết lập xử lý stream dữ liệu hành vi người dùng")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý stream dữ liệu hành vi người dùng: {e}")
    
    def _process_product_rdd(self, rdd):
        """
        Xử lý RDD dữ liệu sản phẩm.
        
        Args:
            rdd: RDD dữ liệu sản phẩm
        """
        if rdd.isEmpty():
            return
        
        try:
            # Chuyển đổi RDD thành DataFrame
            product_schema = StructType([
                StructField("name", StringType(), True),
                StructField("price", StringType(), True),
                StructField("rating", FloatType(), True),
                StructField("sold", StringType(), True),
                StructField("location", StringType(), True),
                StructField("url", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(rdd, product_schema)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_timestamp", current_timestamp())
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"products_{timestamp}")
            
            processed_df.write.json(output_path)
            logger.info(f"Đã lưu dữ liệu sản phẩm đã xử lý vào {output_path}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý RDD dữ liệu sản phẩm: {e}")
    
    def _process_review_rdd(self, rdd):
        """
        Xử lý RDD dữ liệu đánh giá.
        
        Args:
            rdd: RDD dữ liệu đánh giá
        """
        if rdd.isEmpty():
            return
        
        try:
            # Chuyển đổi RDD thành DataFrame
            review_schema = StructType([
                StructField("username", StringType(), True),
                StructField("rating", IntegerType(), True),
                StructField("comment", StringType(), True),
                StructField("time", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(rdd, review_schema)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_timestamp", current_timestamp())
            
            # Phân tích sentiment từ comment
            processed_df = processed_df.withColumn(
                "sentiment_score",
                when(col("rating") >= 4, 1.0)
                .when(col("rating") == 3, 0.0)
                .otherwise(-1.0)
            )
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"reviews_{timestamp}")
            
            processed_df.write.json(output_path)
            logger.info(f"Đã lưu dữ liệu đánh giá đã xử lý vào {output_path}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý RDD dữ liệu đánh giá: {e}")
    
    def _process_user_behavior_rdd(self, rdd):
        """
        Xử lý RDD dữ liệu hành vi người dùng.
        
        Args:
            rdd: RDD dữ liệu hành vi người dùng
        """
        if rdd.isEmpty():
            return
        
        try:
            # Chuyển đổi RDD thành DataFrame
            behavior_schema = StructType([
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(rdd, behavior_schema)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_timestamp", current_timestamp())
            
            # Tính toán trọng số hành vi
            processed_df = processed_df.withColumn(
                "action_weight",
                when(col("action") == "purchase", 5.0)
                .when(col("action") == "add_to_cart", 3.0)
                .when(col("action") == "view", 1.0)
                .otherwise(0.0)
            )
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"user_behavior_{timestamp}")
            
            processed_df.write.json(output_path)
            logger.info(f"Đã lưu dữ liệu hành vi người dùng đã xử lý vào {output_path}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý RDD dữ liệu hành vi người dùng: {e}")
    
    def _simulate_product_stream(self):
        """
        Mô phỏng xử lý stream dữ liệu sản phẩm.
        """
        logger.info("Đang mô phỏng xử lý stream dữ liệu sản phẩm")
        
        # Tạo thư mục đầu ra
        output_dir = os.path.join(self.output_dir, "products")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Mô phỏng dữ liệu sản phẩm
        sample_products = [
            {
                "name": "Samsung Galaxy S21",
                "price": "15.990.000đ",
                "rating": 4.8,
                "sold": "1k2",
                "location": "TP. Hồ Chí Minh",
                "url": "https://shopee.vn/product/123456789",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            {
                "name": "iPhone 13 Pro Max",
                "price": "29.990.000đ",
                "rating": 4.9,
                "sold": "2k5",
                "location": "Hà Nội",
                "url": "https://shopee.vn/product/987654321",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        ]
        
        # Lưu dữ liệu mô phỏng
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"products_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_products, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu sản phẩm vào {output_file}")
    
    def _simulate_review_stream(self):
        """
        Mô phỏng xử lý stream dữ liệu đánh giá.
        """
        logger.info("Đang mô phỏng xử lý stream dữ liệu đánh giá")
        
        # Tạo thư mục đầu ra
        output_dir = os.path.join(self.output_dir, "reviews")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Mô phỏng dữ liệu đánh giá
        sample_reviews = [
            {
                "username": "user123",
                "rating": 5,
                "comment": "Sản phẩm rất tốt, đóng gói cẩn thận, giao hàng nhanh!",
                "time": "2025-03-15",
                "product_id": "123456789",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sentiment_score": 1.0
            },
            {
                "username": "user456",
                "rating": 2,
                "comment": "Sản phẩm không như mô tả, chất lượng kém.",
                "time": "2025-03-16",
                "product_id": "987654321",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sentiment_score": -1.0
            }
        ]
        
        # Lưu dữ liệu mô phỏng
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"reviews_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_reviews, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu đánh giá vào {output_file}")
    
    def _simulate_user_behavior_stream(self):
        """
        Mô phỏng xử lý stream dữ liệu hành vi người dùng.
        """
        logger.info("Đang mô phỏng xử lý stream dữ liệu hành vi người dùng")
        
        # Tạo thư mục đầu ra
        output_dir = os.path.join(self.output_dir, "user_behavior")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Mô phỏng dữ liệu hành vi người dùng
        sample_behaviors = [
            {
                "user_id": "user123",
                "product_id": "123456789",
                "action": "view",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "action_weight": 1.0
            },
            {
                "user_id": "user123",
                "product_id": "123456789",
                "action": "add_to_cart",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "action_weight": 3.0
            },
            {
                "user_id": "user456",
                "product_id": "987654321",
                "action": "purchase",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processed_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "action_weight": 5.0
            }
        ]
        
        # Lưu dữ liệu mô phỏng
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"user_behavior_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_behaviors, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu hành vi người dùng vào {output_file}")
    
    def start(self):
        """
        Bắt đầu xử lý stream.
        """
        if self._simulate_spark:
            logger.info("Đang mô phỏng xử lý stream")
            self._simulate_product_stream()
            self._simulate_review_stream()
            self._simulate_user_behavior_stream()
            return
        
        try:
            # Thiết lập xử lý stream
            self.process_product_stream()
            self.process_review_stream()
            self.process_user_behavior_stream()
            
            # Bắt đầu StreamingContext
            self.ssc.start()
            logger.info("Đã bắt đầu Spark Streaming")
            
            # Đợi kết thúc
            self.ssc.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Đã nhận tín hiệu dừng, đang thoát...")
            self.stop()
        except Exception as e:
            logger.error(f"Lỗi khi chạy Spark Streaming: {e}")
            self.stop()
    
    def stop(self):
        """
        Dừng xử lý stream.
        """
        if not self._simulate_spark and hasattr(self, 'ssc'):
            self.ssc.stop(stopSparkContext=True, stopGraceFully=True)
            logger.info("Đã dừng Spark Streaming")


if __name__ == "__main__":
    # Ví dụ sử dụng
    streaming = ShopeeSparkStreaming()
    
    try:
        # Bắt đầu xử lý stream
        streaming.start()
    except KeyboardInterrupt:
        # Dừng khi nhận Ctrl+C
        streaming.stop()
