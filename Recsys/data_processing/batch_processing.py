#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch Processing Manager cho dữ liệu Shopee

Mô-đun này quản lý việc xử lý dữ liệu theo batch với Hadoop/MapReduce.
"""

import os
import json
import logging
import subprocess
from datetime import datetime
import findspark
try:
    findspark.init()
except:
    pass

try:
    from pyspark import SparkContext, SparkConf
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
logger = logging.getLogger('shopee_batch_processing')

class ShopeeBatchProcessing:
    """
    Lớp ShopeeBatchProcessing quản lý việc xử lý dữ liệu theo batch với Hadoop/MapReduce.
    """
    
    def __init__(self, app_name='ShopeeBatchProcessing', master='local[*]',
                 hdfs_base_dir='/user/shopee',
                 local_input_dir='../data',
                 local_output_dir='../data/batch_processed'):
        """
        Khởi tạo Batch Processing với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            hdfs_base_dir (str): Thư mục cơ sở trên HDFS
            local_input_dir (str): Thư mục đầu vào trên máy cục bộ
            local_output_dir (str): Thư mục đầu ra trên máy cục bộ
        """
        self.app_name = app_name
        self.master = master
        self.hdfs_base_dir = hdfs_base_dir
        self.local_input_dir = local_input_dir
        self.local_output_dir = local_output_dir
        
        # Các thư mục con trên HDFS
        self.hdfs_dirs = {
            'raw': f"{hdfs_base_dir}/raw",
            'processed': f"{hdfs_base_dir}/processed",
            'products': f"{hdfs_base_dir}/products",
            'reviews': f"{hdfs_base_dir}/reviews",
            'user_behavior': f"{hdfs_base_dir}/user_behavior"
        }
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(local_output_dir):
            os.makedirs(local_output_dir)
        
        # Khởi tạo Spark
        self._initialize_spark()
    
    def _initialize_spark(self):
        """
        Khởi tạo Spark.
        """
        try:
            # Tạo SparkConf
            conf = SparkConf().setAppName(self.app_name).setMaster(self.master)
            
            # Cấu hình bổ sung
            conf.set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            
            # Khởi tạo SparkSession
            self.spark = SparkSession.builder \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info(f"Đã khởi tạo Spark với master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def _run_hadoop_command(self, args):
        """
        Chạy lệnh Hadoop.
        
        Args:
            args (list): Danh sách các tham số lệnh
            
        Returns:
            dict: Kết quả thực thi lệnh
        """
        cmd = ['hadoop'] + args
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            stdout, stderr = process.communicate()
            
            result = {
                'returncode': process.returncode,
                'stdout': stdout.strip(),
                'stderr': stderr.strip(),
                'command': ' '.join(cmd)
            }
            
            if process.returncode != 0:
                logger.error(f"Lỗi khi chạy lệnh Hadoop: {result['command']}")
                logger.error(f"Lỗi: {result['stderr']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi thực thi lệnh Hadoop: {e}")
            return {
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'command': ' '.join(cmd)
            }
    
    def process_products_batch(self, input_path=None):
        """
        Xử lý dữ liệu sản phẩm theo batch.
        
        Args:
            input_path (str, optional): Đường dẫn đến dữ liệu đầu vào
            
        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_products_batch()
        
        try:
            # Xác định đường dẫn đầu vào
            if not input_path:
                input_path = os.path.join(self.local_input_dir, "products")
                if not os.path.exists(input_path):
                    input_path = self.hdfs_dirs['products']
            
            # Đọc dữ liệu
            df = self.spark.read.json(input_path)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_date", current_date())
            
            # Chuyển đổi giá từ chuỗi sang số
            processed_df = processed_df.withColumn(
                "price_numeric",
                regexp_replace(col("price"), "[^0-9]", "").cast("double")
            )
            
            # Chuyển đổi số lượng đã bán từ chuỗi sang số
            processed_df = processed_df.withColumn(
                "sold_numeric",
                when(col("sold").endswith("k"), regexp_replace(col("sold"), "k.*", "").cast("double") * 1000)
                .otherwise(regexp_replace(col("sold"), "[^0-9]", "").cast("double"))
            )
            
            # Tính toán điểm phổ biến dựa trên rating và số lượng đã bán
            processed_df = processed_df.withColumn(
                "popularity_score",
                col("rating") * col("sold_numeric") / 1000
            )
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.local_output_dir, f"products_batch_{timestamp}")
            
            processed_df.write.parquet(output_path)
            logger.info(f"Đã lưu dữ liệu sản phẩm đã xử lý vào {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu sản phẩm theo batch: {e}")
            return False
    
    def process_reviews_batch(self, input_path=None):
        """
        Xử lý dữ liệu đánh giá theo batch.
        
        Args:
            input_path (str, optional): Đường dẫn đến dữ liệu đầu vào
            
        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_reviews_batch()
        
        try:
            # Xác định đường dẫn đầu vào
            if not input_path:
                input_path = os.path.join(self.local_input_dir, "reviews")
                if not os.path.exists(input_path):
                    input_path = self.hdfs_dirs['reviews']
            
            # Đọc dữ liệu
            df = self.spark.read.json(input_path)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_date", current_date())
            
            # Phân tích sentiment từ comment
            processed_df = processed_df.withColumn(
                "sentiment_score",
                when(col("rating") >= 4, 1.0)
                .when(col("rating") == 3, 0.0)
                .otherwise(-1.0)
            )
            
            # Tính toán độ dài comment
            processed_df = processed_df.withColumn(
                "comment_length",
                length(col("comment"))
            )
            
            # Tính toán trọng số đánh giá dựa trên độ dài comment
            processed_df = processed_df.withColumn(
                "review_weight",
                when(col("comment_length") > 100, 2.0)
                .when(col("comment_length") > 50, 1.5)
                .otherwise(1.0)
            )
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.local_output_dir, f"reviews_batch_{timestamp}")
            
            processed_df.write.parquet(output_path)
            logger.info(f"Đã lưu dữ liệu đánh giá đã xử lý vào {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu đánh giá theo batch: {e}")
            return False
    
    def process_user_behavior_batch(self, input_path=None):
        """
        Xử lý dữ liệu hành vi người dùng theo batch.
        
        Args:
            input_path (str, optional): Đường dẫn đến dữ liệu đầu vào
            
        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_user_behavior_batch()
        
        try:
            # Xác định đường dẫn đầu vào
            if not input_path:
                input_path = os.path.join(self.local_input_dir, "user_behavior")
                if not os.path.exists(input_path):
                    input_path = self.hdfs_dirs['user_behavior']
            
            # Đọc dữ liệu
            df = self.spark.read.json(input_path)
            
            # Xử lý dữ liệu
            processed_df = df.withColumn("processed_date", current_date())
            
            # Tính toán trọng số hành vi
            processed_df = processed_df.withColumn(
                "action_weight",
                when(col("action") == "purchase", 5.0)
                .when(col("action") == "add_to_cart", 3.0)
                .when(col("action") == "view", 1.0)
                .otherwise(0.0)
            )
            
            # Tính toán tần suất hành vi theo người dùng và sản phẩm
            user_product_freq = processed_df.groupBy("user_id", "product_id") \
                .agg(count("*").alias("action_frequency"))
            
            # Tính toán điểm tương tác
            user_product_score = processed_df.groupBy("user_id", "product_id") \
                .agg(sum("action_weight").alias("interaction_score"))
            
            # Kết hợp các kết quả
            result_df = user_product_freq.join(
                user_product_score,
                ["user_id", "product_id"]
            ).withColumn("processed_date", current_date())
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.local_output_dir, f"user_behavior_batch_{timestamp}")
            
            result_df.write.parquet(output_path)
            logger.info(f"Đã lưu dữ liệu hành vi người dùng đã xử lý vào {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu hành vi người dùng theo batch: {e}")
            return False
    
    def generate_product_recommendations(self):
        """
        Tạo gợi ý sản phẩm dựa trên dữ liệu đã xử lý.
        
        Returns:
            bool: True nếu tạo gợi ý thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_product_recommendations()
        
        try:
            # Đọc dữ liệu sản phẩm đã xử lý
            products_path = os.path.join(self.local_output_dir, "products_batch_*")
            products_df = self.spark.read.parquet(products_path)
            
            # Đọc dữ liệu hành vi người dùng đã xử lý
            behavior_path = os.path.join(self.local_output_dir, "user_behavior_batch_*")
            behavior_df = self.spark.read.parquet(behavior_path)
            
            # Kết hợp dữ liệu
            joined_df = behavior_df.join(
                products_df.select("product_id", "name", "price", "rating", "popularity_score"),
                "product_id"
            )
            
            # Tính toán điểm gợi ý
            recommendation_df = joined_df.withColumn(
                "recommendation_score",
                col("interaction_score") * col("popularity_score")
            )
            
            # Lấy top sản phẩm được gợi ý cho mỗi người dùng
            top_recommendations = recommendation_df.orderBy(
                col("user_id"),
                col("recommendation_score").desc()
            )
            
            # Lưu kết quả
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.local_output_dir, f"product_recommendations_{timestamp}")
            
            top_recommendations.write.parquet(output_path)
            logger.info(f"Đã lưu gợi ý sản phẩm vào {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý sản phẩm: {e}")
            return False
    
    def _simulate_products_batch(self):
        """
        Mô phỏng xử lý dữ liệu sản phẩm theo batch.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        try:
            logger.info("Đang mô phỏng xử lý dữ liệu sản phẩm theo batch")
            
            # Tạo thư mục đầu ra
            output_dir = os.path.join(self.local_output_dir, "products_batch")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Mô phỏng dữ liệu sản phẩm đã xử lý
            sample_products = [
                {
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "price_numeric": 15990000.0,
                    "rating": 4.8,
                    "sold": "1k2",
                    "sold_numeric": 1200.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/123456789",
                    "product_id": "123456789",
                    "popularity_score": 5.76,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "price_numeric": 29990000.0,
                    "rating": 4.9,
                    "sold": "2k5",
                    "sold_numeric": 2500.0,
                    "location": "Hà Nội",
                    "url": "https://shopee.vn/product/987654321",
                    "product_id": "987654321",
                    "popularity_score": 12.25,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lưu dữ liệu mô phỏng
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"products_batch_{timestamp}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_products, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu sản phẩm theo batch vào {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng xử lý dữ liệu sản phẩm theo batch: {e}")
            return False
    
    def _simulate_reviews_batch(self):
        """
        Mô phỏng xử lý dữ liệu đánh giá theo batch.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        try:
            logger.info("Đang mô phỏng xử lý dữ liệu đánh giá theo batch")
            
            # Tạo thư mục đầu ra
            output_dir = os.path.join(self.local_output_dir, "reviews_batch")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Mô phỏng dữ liệu đánh giá đã xử lý
            sample_reviews = [
                {
                    "username": "user123",
                    "rating": 5,
                    "comment": "Sản phẩm rất tốt, đóng gói cẩn thận, giao hàng nhanh!",
                    "comment_length": 58,
                    "time": "2025-03-15",
                    "product_id": "123456789",
                    "sentiment_score": 1.0,
                    "review_weight": 1.5,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "username": "user456",
                    "rating": 2,
                    "comment": "Sản phẩm không như mô tả, chất lượng kém.",
                    "comment_length": 45,
                    "time": "2025-03-16",
                    "product_id": "987654321",
                    "sentiment_score": -1.0,
                    "review_weight": 1.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lưu dữ liệu mô phỏng
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"reviews_batch_{timestamp}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_reviews, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu đánh giá theo batch vào {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng xử lý dữ liệu đánh giá theo batch: {e}")
            return False
    
    def _simulate_user_behavior_batch(self):
        """
        Mô phỏng xử lý dữ liệu hành vi người dùng theo batch.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        try:
            logger.info("Đang mô phỏng xử lý dữ liệu hành vi người dùng theo batch")
            
            # Tạo thư mục đầu ra
            output_dir = os.path.join(self.local_output_dir, "user_behavior_batch")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Mô phỏng dữ liệu hành vi người dùng đã xử lý
            sample_behaviors = [
                {
                    "user_id": "user123",
                    "product_id": "123456789",
                    "action_frequency": 3,
                    "interaction_score": 9.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user123",
                    "product_id": "987654321",
                    "action_frequency": 1,
                    "interaction_score": 1.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "123456789",
                    "action_frequency": 2,
                    "interaction_score": 6.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "987654321",
                    "action_frequency": 4,
                    "interaction_score": 14.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lưu dữ liệu mô phỏng
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"user_behavior_batch_{timestamp}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_behaviors, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã mô phỏng xử lý và lưu dữ liệu hành vi người dùng theo batch vào {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng xử lý dữ liệu hành vi người dùng theo batch: {e}")
            return False
    
    def _simulate_product_recommendations(self):
        """
        Mô phỏng tạo gợi ý sản phẩm.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        try:
            logger.info("Đang mô phỏng tạo gợi ý sản phẩm")
            
            # Tạo thư mục đầu ra
            output_dir = os.path.join(self.local_output_dir, "product_recommendations")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Mô phỏng dữ liệu gợi ý sản phẩm
            sample_recommendations = [
                {
                    "user_id": "user123",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "interaction_score": 9.0,
                    "popularity_score": 5.76,
                    "recommendation_score": 51.84,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user123",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "interaction_score": 1.0,
                    "popularity_score": 12.25,
                    "recommendation_score": 12.25,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "interaction_score": 14.0,
                    "popularity_score": 12.25,
                    "recommendation_score": 171.5,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "interaction_score": 6.0,
                    "popularity_score": 5.76,
                    "recommendation_score": 34.56,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lưu dữ liệu mô phỏng
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"product_recommendations_{timestamp}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_recommendations, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã mô phỏng tạo và lưu gợi ý sản phẩm vào {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng tạo gợi ý sản phẩm: {e}")
            return False
    
    def run_batch_processing(self):
        """
        Chạy toàn bộ quy trình xử lý batch.
        
        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        try:
            logger.info("Bắt đầu xử lý batch")
            
            # Xử lý dữ liệu sản phẩm
            products_success = self.process_products_batch()
            
            # Xử lý dữ liệu đánh giá
            reviews_success = self.process_reviews_batch()
            
            # Xử lý dữ liệu hành vi người dùng
            behavior_success = self.process_user_behavior_batch()
            
            # Tạo gợi ý sản phẩm
            recommendations_success = self.generate_product_recommendations()
            
            # Kiểm tra kết quả
            overall_success = products_success and reviews_success and behavior_success and recommendations_success
            
            if overall_success:
                logger.info("Đã hoàn thành xử lý batch thành công")
            else:
                logger.warning("Xử lý batch hoàn thành với một số lỗi")
            
            return overall_success
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy xử lý batch: {e}")
            return False
    
    def stop(self):
        """
        Dừng Spark và giải phóng tài nguyên.
        """
        if not self._simulate_spark and hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Đã dừng Spark")


if __name__ == "__main__":
    # Ví dụ sử dụng
    batch_processing = ShopeeBatchProcessing()
    
    try:
        # Chạy xử lý batch
        batch_processing.run_batch_processing()
    finally:
        # Dừng Spark
        batch_processing.stop()
