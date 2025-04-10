#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Collaborative Filtering cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này triển khai thuật toán Collaborative Filtering sử dụng Spark MLlib.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
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
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.evaluation import RegressionEvaluator
except ImportError:
    print("PySpark hoặc các module liên quan không khả dụng. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_collaborative_filtering')

class ShopeeCollaborativeFiltering:
    """
    Lớp ShopeeCollaborativeFiltering triển khai thuật toán Collaborative Filtering.
    """
    
    def __init__(self, app_name='ShopeeCollaborativeFiltering', master='local[*]',
                 input_dir='../data/batch_processed',
                 output_dir='../data/recommendations'):
        """
        Khởi tạo Collaborative Filtering với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            input_dir (str): Thư mục đầu vào chứa dữ liệu đã xử lý
            output_dir (str): Thư mục đầu ra cho kết quả gợi ý
        """
        self.app_name = app_name
        self.master = master
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
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
                .getOrCreate()
            
            logger.info(f"Đã khởi tạo Spark với master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def prepare_data(self, user_behavior_path=None):
        """
        Chuẩn bị dữ liệu cho Collaborative Filtering.
        
        Args:
            user_behavior_path (str, optional): Đường dẫn đến dữ liệu hành vi người dùng
            
        Returns:
            tuple: (training_data, test_data) hoặc None nếu mô phỏng
        """
        if self._simulate_spark:
            logger.info("Đang mô phỏng chuẩn bị dữ liệu")
            return None
        
        try:
            # Xác định đường dẫn đến dữ liệu hành vi người dùng
            if not user_behavior_path:
                user_behavior_path = os.path.join(self.input_dir, "user_behavior_batch_*")
            
            # Đọc dữ liệu
            behavior_df = self.spark.read.parquet(user_behavior_path)
            
            # Chuyển đổi dữ liệu thành định dạng phù hợp cho ALS
            ratings_df = behavior_df.select(
                col("user_id").alias("userId"),
                col("product_id").alias("productId"),
                col("interaction_score").alias("rating")
            )
            
            # Chuyển đổi userId và productId thành số nguyên
            # ALS yêu cầu các ID phải là số nguyên
            from pyspark.sql.functions import monotonically_increasing_id
            
            # Tạo bảng ánh xạ cho userId
            user_indexer = ratings_df.select("userId").distinct() \
                .withColumn("userIdInt", monotonically_increasing_id())
            
            # Tạo bảng ánh xạ cho productId
            product_indexer = ratings_df.select("productId").distinct() \
                .withColumn("productIdInt", monotonically_increasing_id())
            
            # Lưu bảng ánh xạ để sử dụng sau này
            user_indexer.write.mode("overwrite").parquet(os.path.join(self.output_dir, "user_indexer"))
            product_indexer.write.mode("overwrite").parquet(os.path.join(self.output_dir, "product_indexer"))
            
            # Áp dụng ánh xạ vào dữ liệu
            ratings_indexed = ratings_df \
                .join(user_indexer, "userId") \
                .join(product_indexer, "productId") \
                .select("userIdInt", "productIdInt", "rating")
            
            # Chia dữ liệu thành tập huấn luyện và tập kiểm tra
            (training_data, test_data) = ratings_indexed.randomSplit([0.8, 0.2], seed=42)
            
            logger.info(f"Đã chuẩn bị dữ liệu: {training_data.count()} mẫu huấn luyện, {test_data.count()} mẫu kiểm tra")
            return (training_data, test_data)
            
        except Exception as e:
            logger.error(f"Lỗi khi chuẩn bị dữ liệu: {e}")
            return None
    
    def train_als_model(self, training_data, rank=10, max_iter=10, reg_param=0.1, alpha=1.0):
        """
        Huấn luyện mô hình ALS (Alternating Least Squares).
        
        Args:
            training_data: Dữ liệu huấn luyện
            rank (int): Số lượng features ẩn
            max_iter (int): Số lần lặp tối đa
            reg_param (float): Tham số điều chuẩn
            alpha (float): Tham số alpha cho độ tin cậy ngầm
            
        Returns:
            ALS model hoặc None nếu mô phỏng
        """
        if self._simulate_spark:
            logger.info("Đang mô phỏng huấn luyện mô hình ALS")
            return None
        
        try:
            # Khởi tạo mô hình ALS
            als = ALS(
                userCol="userIdInt",
                itemCol="productIdInt",
                ratingCol="rating",
                rank=rank,
                maxIter=max_iter,
                regParam=reg_param,
                alpha=alpha,
                coldStartStrategy="drop",
                implicitPrefs=True  # Sử dụng dữ liệu ngầm (implicit feedback)
            )
            
            # Huấn luyện mô hình
            model = als.fit(training_data)
            
            logger.info(f"Đã huấn luyện mô hình ALS với rank={rank}, maxIter={max_iter}, regParam={reg_param}")
            return model
            
        except Exception as e:
            logger.error(f"Lỗi khi huấn luyện mô hình ALS: {e}")
            return None
    
    def evaluate_model(self, model, test_data):
        """
        Đánh giá mô hình ALS.
        
        Args:
            model: Mô hình ALS đã huấn luyện
            test_data: Dữ liệu kiểm tra
            
        Returns:
            float: RMSE (Root Mean Square Error)
        """
        if self._simulate_spark:
            logger.info("Đang mô phỏng đánh giá mô hình")
            return 0.5  # Giá trị mô phỏng
        
        try:
            # Dự đoán trên tập kiểm tra
            predictions = model.transform(test_data)
            
            # Đánh giá mô hình
            evaluator = RegressionEvaluator(
                metricName="rmse",
                labelCol="rating",
                predictionCol="prediction"
            )
            rmse = evaluator.evaluate(predictions)
            
            logger.info(f"Đánh giá mô hình: RMSE = {rmse}")
            return rmse
            
        except Exception as e:
            logger.error(f"Lỗi khi đánh giá mô hình: {e}")
            return None
    
    def generate_recommendations(self, model, num_recommendations=10):
        """
        Tạo gợi ý sản phẩm cho tất cả người dùng.
        
        Args:
            model: Mô hình ALS đã huấn luyện
            num_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            DataFrame: Gợi ý sản phẩm
        """
        if self._simulate_spark:
            return self._simulate_recommendations()
        
        try:
            # Tạo gợi ý cho tất cả người dùng
            user_recs = model.recommendForAllUsers(num_recommendations)
            
            # Đọc bảng ánh xạ
            user_indexer = self.spark.read.parquet(os.path.join(self.output_dir, "user_indexer"))
            product_indexer = self.spark.read.parquet(os.path.join(self.output_dir, "product_indexer"))
            
            # Chuyển đổi cột recommendations từ mảng thành nhiều hàng
            from pyspark.sql.functions import explode
            user_recs = user_recs.withColumn("recommendation", explode("recommendations"))
            
            # Trích xuất productIdInt và rating từ cột recommendation
            user_recs = user_recs.select(
                "userIdInt",
                user_recs.recommendation.productIdInt.alias("productIdInt"),
                user_recs.recommendation.rating.alias("prediction")
            )
            
            # Áp dụng ánh xạ ngược để lấy userId và productId gốc
            recommendations = user_recs \
                .join(user_indexer, user_recs.userIdInt == user_indexer.userIdInt) \
                .join(product_indexer, user_recs.productIdInt == product_indexer.productIdInt) \
                .select(
                    user_indexer.userId,
                    product_indexer.productId,
                    user_recs.prediction
                )
            
            # Đọc thông tin sản phẩm
            products_path = os.path.join(self.input_dir, "products_batch_*")
            products_df = self.spark.read.parquet(products_path)
            
            # Kết hợp với thông tin sản phẩm
            recommendations = recommendations \
                .join(
                    products_df.select("product_id", "name", "price", "rating", "popularity_score"),
                    recommendations.productId == products_df.product_id
                ) \
                .select(
                    recommendations.userId.alias("user_id"),
                    recommendations.productId.alias("product_id"),
                    products_df.name,
                    products_df.price,
                    products_df.rating,
                    recommendations.prediction.alias("cf_score"),
                    products_df.popularity_score,
                    (recommendations.prediction * products_df.popularity_score).alias("recommendation_score")
                ) \
                .orderBy(col("user_id"), col("recommendation_score").desc())
            
            logger.info(f"Đã tạo {recommendations.count()} gợi ý sản phẩm")
            return recommendations
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý sản phẩm: {e}")
            return None
    
    def save_recommendations(self, recommendations, output_path=None):
        """
        Lưu gợi ý sản phẩm.
        
        Args:
            recommendations: DataFrame gợi ý sản phẩm
            output_path (str, optional): Đường dẫn đầu ra
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_save_recommendations()
        
        try:
            # Xác định đường dẫn đầu ra
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = os.path.join(self.output_dir, f"als_recommendations_{timestamp}")
            
            # Lưu gợi ý
            recommendations.write.parquet(output_path)
            
            logger.info(f"Đã lưu gợi ý sản phẩm vào {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu gợi ý sản phẩm: {e}")
            return False
    
    def _simulate_recommendations(self):
        """
        Mô phỏng tạo gợi ý sản phẩm.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        try:
            logger.info("Đang mô phỏng tạo gợi ý sản phẩm")
            
            # Tạo thư mục đầu ra
            output_dir = os.path.join(self.output_dir, "als_recommendations")
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
                    "cf_score": 0.85,
                    "popularity_score": 5.76,
                    "recommendation_score": 4.896,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user123",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "cf_score": 0.72,
                    "popularity_score": 12.25,
                    "recommendation_score": 8.82,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "cf_score": 0.95,
                    "popularity_score": 12.25,
                    "recommendation_score": 11.6375,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "cf_score": 0.65,
                    "popularity_score": 5.76,
                    "recommendation_score": 3.744,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lưu dữ liệu mô phỏng
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"als_recommendations_{timestamp}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_recommendations, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã mô phỏng tạo và lưu gợi ý sản phẩm vào {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng tạo gợi ý sản phẩm: {e}")
            return False
    
    def _simulate_save_recommendations(self):
        """
        Mô phỏng lưu gợi ý sản phẩm.
        
        Returns:
            bool: True nếu mô phỏng thành công, False nếu thất bại
        """
        return self._simulate_recommendations()
    
    def run_collaborative_filtering(self, rank=10, max_iter=10, reg_param=0.1, alpha=1.0, num_recommendations=10):
        """
        Chạy toàn bộ quy trình Collaborative Filtering.
        
        Args:
            rank (int): Số lượng features ẩn
            max_iter (int): Số lần lặp tối đa
            reg_param (float): Tham số điều chuẩn
            alpha (float): Tham số alpha cho độ tin cậy ngầm
            num_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if self._simulate_spark:
            return self._simulate_recommendations()
        
        try:
            # Chuẩn bị dữ liệu
            data = self.prepare_data()
            if not data:
                return False
            
            training_data, test_data = data
            
            # Huấn luyện mô hình
            model = self.train_als_model(training_data, rank, max_iter, reg_param, alpha)
            if not model:
                return False
            
            # Đánh giá mô hình
            rmse = self.evaluate_model(model, test_data)
            
            # Tạo gợi ý
            recommendations = self.generate_recommendations(model, num_recommendations)
            if recommendations is None:
                return False
            
            # Lưu gợi ý
            success = self.save_recommendations(recommendations)
            
            return success
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy Collaborative Filtering: {e}")
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
    cf = ShopeeCollaborativeFiltering()
    
    try:
        # Chạy Collaborative Filtering
        cf.run_collaborative_filtering()
    finally:
        # Dừng Spark
        cf.stop()
