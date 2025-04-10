#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Serving Layer cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này quản lý Serving Layer với Batch View và Real-time View.
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import threading
import time
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
    from pyspark.streaming import StreamingContext
except ImportError:
    print("PySpark hoặc các module liên quan không khả dụng. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_serving_layer')

class ShopeeServingLayer:
    """
    Lớp ShopeeServingLayer quản lý Serving Layer với Batch View và Real-time View.
    """
    
    def __init__(self, app_name='ShopeeServingLayer', master='local[*]',
                 batch_input_dir='../data/batch_processed',
                 realtime_input_dir='../data/realtime_processed',
                 output_dir='../data/serving_layer',
                 batch_update_interval=3600,  # 1 giờ
                 realtime_update_interval=60):  # 1 phút
        """
        Khởi tạo Serving Layer với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            batch_input_dir (str): Thư mục đầu vào chứa dữ liệu batch đã xử lý
            realtime_input_dir (str): Thư mục đầu vào chứa dữ liệu real-time đã xử lý
            output_dir (str): Thư mục đầu ra cho Serving Layer
            batch_update_interval (int): Khoảng thời gian cập nhật Batch View (giây)
            realtime_update_interval (int): Khoảng thời gian cập nhật Real-time View (giây)
        """
        self.app_name = app_name
        self.master = master
        self.batch_input_dir = batch_input_dir
        self.realtime_input_dir = realtime_input_dir
        self.output_dir = output_dir
        self.batch_update_interval = batch_update_interval
        self.realtime_update_interval = realtime_update_interval
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Tạo thư mục cho Batch View
        self.batch_view_dir = os.path.join(output_dir, 'batch_view')
        if not os.path.exists(self.batch_view_dir):
            os.makedirs(self.batch_view_dir)
        
        # Tạo thư mục cho Real-time View
        self.realtime_view_dir = os.path.join(output_dir, 'realtime_view')
        if not os.path.exists(self.realtime_view_dir):
            os.makedirs(self.realtime_view_dir)
        
        # Tạo thư mục cho Speed Layer
        self.speed_layer_dir = os.path.join(output_dir, 'speed_layer')
        if not os.path.exists(self.speed_layer_dir):
            os.makedirs(self.speed_layer_dir)
        
        # Khởi tạo Spark
        self._initialize_spark()
        
        # Khởi tạo các view
        self.batch_views = {}
        self.realtime_views = {}
        self.speed_layer_data = {}
        
        # Khởi tạo các luồng cập nhật
        self.batch_update_thread = None
        self.realtime_update_thread = None
        self.running = False
    
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
            
            # Khởi tạo StreamingContext
            self.sc = self.spark.sparkContext
            self.ssc = StreamingContext(self.sc, 10)  # 10 giây batch interval
            
            logger.info(f"Đã khởi tạo Spark với master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def start(self):
        """
        Bắt đầu Serving Layer.
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Kiểm tra trạng thái
            if self.running:
                logger.warning("Serving Layer đã đang chạy")
                return True
            
            # Đánh dấu đang chạy
            self.running = True
            
            # Khởi tạo Batch View
            self._initialize_batch_views()
            
            # Khởi tạo Real-time View
            self._initialize_realtime_views()
            
            # Bắt đầu luồng cập nhật Batch View
            self.batch_update_thread = threading.Thread(
                target=self._batch_update_loop,
                daemon=True
            )
            self.batch_update_thread.start()
            
            # Bắt đầu luồng cập nhật Real-time View
            self.realtime_update_thread = threading.Thread(
                target=self._realtime_update_loop,
                daemon=True
            )
            self.realtime_update_thread.start()
            
            # Bắt đầu StreamingContext nếu không mô phỏng
            if not self._simulate_spark:
                self.ssc.start()
            
            logger.info("Đã bắt đầu Serving Layer")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi bắt đầu Serving Layer: {e}")
            self.running = False
            return False
    
    def stop(self):
        """
        Dừng Serving Layer.
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Kiểm tra trạng thái
            if not self.running:
                logger.warning("Serving Layer đã dừng")
                return True
            
            # Đánh dấu dừng
            self.running = False
            
            # Dừng StreamingContext nếu không mô phỏng
            if not self._simulate_spark:
                self.ssc.stop(stopSparkContext=False, stopGraceFully=True)
            
            # Chờ các luồng cập nhật kết thúc
            if self.batch_update_thread:
                self.batch_update_thread.join(timeout=5)
            
            if self.realtime_update_thread:
                self.realtime_update_thread.join(timeout=5)
            
            # Lưu các view
            self._save_batch_views()
            self._save_realtime_views()
            
            # Dừng Spark
            if not self._simulate_spark:
                self.spark.stop()
            
            logger.info("Đã dừng Serving Layer")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi dừng Serving Layer: {e}")
            return False
    
    def _initialize_batch_views(self):
        """
        Khởi tạo Batch View.
        """
        try:
            # Tải các Batch View đã lưu
            self._load_batch_views()
            
            # Cập nhật Batch View
            self._update_batch_views()
            
            logger.info("Đã khởi tạo Batch View")
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Batch View: {e}")
    
    def _initialize_realtime_views(self):
        """
        Khởi tạo Real-time View.
        """
        try:
            # Tải các Real-time View đã lưu
            self._load_realtime_views()
            
            # Cập nhật Real-time View
            self._update_realtime_views()
            
            logger.info("Đã khởi tạo Real-time View")
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Real-time View: {e}")
    
    def _batch_update_loop(self):
        """
        Vòng lặp cập nhật Batch View.
        """
        logger.info("Bắt đầu vòng lặp cập nhật Batch View")
        
        while self.running:
            try:
                # Cập nhật Batch View
                self._update_batch_views()
                
                # Lưu Batch View
                self._save_batch_views()
                
                # Chờ đến lần cập nhật tiếp theo
                for _ in range(self.batch_update_interval):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Lỗi trong vòng lặp cập nhật Batch View: {e}")
                time.sleep(60)  # Chờ 1 phút trước khi thử lại
    
    def _realtime_update_loop(self):
        """
        Vòng lặp cập nhật Real-time View.
        """
        logger.info("Bắt đầu vòng lặp cập nhật Real-time View")
        
        while self.running:
            try:
                # Cập nhật Real-time View
                self._update_realtime_views()
                
                # Lưu Real-time View
                self._save_realtime_views()
                
                # Chờ đến lần cập nhật tiếp theo
                for _ in range(self.realtime_update_interval):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Lỗi trong vòng lặp cập nhật Real-time View: {e}")
                time.sleep(10)  # Chờ 10 giây trước khi thử lại
    
    def _update_batch_views(self):
        """
        Cập nhật Batch View.
        """
        try:
            # Cập nhật Batch View cho sản phẩm
            self._update_products_batch_view()
            
            # Cập nhật Batch View cho đánh giá
            self._update_reviews_batch_view()
            
            # Cập nhật Batch View cho hành vi người dùng
            self._update_user_behavior_batch_view()
            
            # Cập nhật Batch View cho gợi ý sản phẩm
            self._update_recommendations_batch_view()
            
            logger.info("Đã cập nhật Batch View")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Batch View: {e}")
    
    def _update_realtime_views(self):
        """
        Cập nhật Real-time View.
        """
        try:
            # Cập nhật Real-time View cho sản phẩm
            self._update_products_realtime_view()
            
            # Cập nhật Real-time View cho đánh giá
            self._update_reviews_realtime_view()
            
            # Cập nhật Real-time View cho hành vi người dùng
            self._update_user_behavior_realtime_view()
            
            # Cập nhật Real-time View cho gợi ý sản phẩm
            self._update_recommendations_realtime_view()
            
            logger.info("Đã cập nhật Real-time View")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Real-time View: {e}")
    
    def _update_products_batch_view(self):
        """
        Cập nhật Batch View cho sản phẩm.
        """
        if self._simulate_spark:
            # Mô phỏng cập nhật Batch View cho sản phẩm
            self._simulate_update_products_batch_view()
            return
        
        try:
            # Đọc dữ liệu sản phẩm từ Batch Layer
            products_path = os.path.join(self.batch_input_dir, "products_batch_*")
            products_df = self.spark.read.parquet(products_path)
            
            # Tạo view cho top sản phẩm
            top_products = products_df.orderBy(desc("popularity_score")).limit(100)
            
            # Tạo view cho sản phẩm theo danh mục
            products_by_category = products_df.groupBy("category") \
                .agg(
                    count("product_id").alias("product_count"),
                    avg("price_numeric").alias("avg_price"),
                    avg("rating").alias("avg_rating")
                )
            
            # Lưu vào Batch View
            self.batch_views["top_products"] = top_products
            self.batch_views["products_by_category"] = products_by_category
            
            logger.info("Đã cập nhật Batch View cho sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Batch View cho sản phẩm: {e}")
    
    def _update_reviews_batch_view(self):
        """
        Cập nhật Batch View cho đánh giá.
        """
        if self._simulate_spark:
            # Mô phỏng cập nhật Batch View cho đánh giá
            self._simulate_update_reviews_batch_view()
            return
        
        try:
            # Đọc dữ liệu đánh giá từ Batch Layer
            reviews_path = os.path.join(self.batch_input_dir, "reviews_batch_*")
            reviews_df = self.spark.read.parquet(reviews_path)
            
            # Tạo view cho phân tích sentiment
            sentiment_analysis = reviews_df.groupBy("product_id") \
                .agg(
                    avg("sentiment_score").alias("avg_sentiment"),
                    count("product_id").alias("review_count")
                )
            
            # Tạo view cho phân phối rating
            rating_distribution = reviews_df.groupBy("product_id", "rating") \
                .count() \
                .orderBy("product_id", "rating")
            
            # Lưu vào Batch View
            self.batch_views["sentiment_analysis"] = sentiment_analysis
            self.batch_views["rating_distribution"] = rating_distribution
            
            logger.info("Đã cập nhật Batch View cho đánh giá")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Batch View cho đánh giá: {e}")
    
    def _update_user_behavior_batch_view(self):
        """
        Cập nhật Batch View cho hành vi người dùng.
        """
        if self._simulate_spark:
            # Mô phỏng cập nhật Batch View cho hành vi người dùng
            self._simulate_update_user_behavior_batch_view()
            return
        
        try:
            # Đọc dữ liệu hành vi người dùng từ Batch Layer
            behavior_path = os.path.join(self.batch_input_dir, "user_behavior_batch_*")
            behavior_df = self.spark.read.parquet(behavior_path)
            
            # Tạo view cho phân tích hành vi người dùng
            user_behavior_analysis = behavior_df.groupBy("user_id") \
                .agg(
                    sum("action_frequency").alias("total_actions"),
                    avg("interaction_score").alias("avg_interaction")
                )
            
            # Tạo view cho sản phẩm phổ biến dựa trên hành vi
            popular_products = behavior_df.groupBy("product_id") \
                .agg(
                    sum("action_frequency").alias("total_actions"),
                    sum("interaction_score").alias("total_interaction")
                ) \
                .orderBy(desc("total_interaction"))
            
            # Lưu vào Batch View
            self.batch_views["user_behavior_analysis"] = user_behavior_analysis
            self.batch_views["popular_products"] = popular_products
            
            logger.info("Đã cập nhật Batch View cho hành vi người dùng")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Batch View cho hành vi người dùng: {e}")
    
    def _update_recommendations_batch_view(self):
        """
        Cập nhật Batch View cho gợi ý sản phẩm.
        """
        if self._simulate_spark:
            # Mô phỏng cập nhật Batch View cho gợi ý sản phẩm
            self._simulate_update_recommendations_batch_view()
            return
        
        try:
            # Đọc dữ liệu gợi ý sản phẩm từ Batch Layer
            recommendations_path = os.path.join(self.batch_input_dir, "*recommendations_*")
            recommendations_df = self.spark.read.parquet(recommendations_path)
            
            # Tạo view cho gợi ý sản phẩm theo người dùng
            user_recommendations = recommendations_df.orderBy("user_id", desc("recommendation_score"))
            
            # Tạo view cho so sánh các thuật toán gợi ý
            algorithm_comparison = recommendations_df.groupBy("user_id") \
                .pivot("algorithm") \
                .agg(avg("recommendation_score"))
            
            # Lưu vào Batch View
            self.batch_views["user_recommendations"] = user_recommendations
            self.batch_views["algorithm_comparison"] = algorithm_comparison
            
            logger.info("Đã cập nhật Batch View cho gợi ý sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Batch View cho gợi ý sản phẩm: {e}")
    
    def _update_products_realtime_view(self):
        """
        Cập nhật Real-time View cho sản phẩm.
        """
        try:
            # Đọc dữ liệu sản phẩm từ Speed Layer
            products_path = os.path.join(self.realtime_input_dir, "products_realtime_*")
            
            if self._simulate_spark:
                # Mô phỏng đọc dữ liệu
                realtime_products = self._simulate_load_realtime_data('products')
            else:
                # Đọc dữ liệu thực tế
                realtime_products = self.spark.read.parquet(products_path)
            
            # Cập nhật Speed Layer
            if "products" not in self.speed_layer_data:
                self.speed_layer_data["products"] = realtime_products
            else:
                # Kết hợp dữ liệu cũ và mới
                if self._simulate_spark:
                    # Mô phỏng kết hợp
                    self.speed_layer_data["products"] = pd.concat([self.speed_layer_data["products"], realtime_products])
                else:
                    # Kết hợp thực tế
                    self.speed_layer_data["products"] = self.speed_layer_data["products"].union(realtime_products)
            
            # Tạo Real-time View
            if self._simulate_spark:
                # Mô phỏng tạo view
                self.realtime_views["products"] = self.speed_layer_data["products"].copy()
            else:
                # Tạo view thực tế
                self.realtime_views["products"] = self.speed_layer_data["products"]
            
            logger.info("Đã cập nhật Real-time View cho sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Real-time View cho sản phẩm: {e}")
    
    def _update_reviews_realtime_view(self):
        """
        Cập nhật Real-time View cho đánh giá.
        """
        try:
            # Đọc dữ liệu đánh giá từ Speed Layer
            reviews_path = os.path.join(self.realtime_input_dir, "reviews_realtime_*")
            
            if self._simulate_spark:
                # Mô phỏng đọc dữ liệu
                realtime_reviews = self._simulate_load_realtime_data('reviews')
            else:
                # Đọc dữ liệu thực tế
                realtime_reviews = self.spark.read.parquet(reviews_path)
            
            # Cập nhật Speed Layer
            if "reviews" not in self.speed_layer_data:
                self.speed_layer_data["reviews"] = realtime_reviews
            else:
                # Kết hợp dữ liệu cũ và mới
                if self._simulate_spark:
                    # Mô phỏng kết hợp
                    self.speed_layer_data["reviews"] = pd.concat([self.speed_layer_data["reviews"], realtime_reviews])
                else:
                    # Kết hợp thực tế
                    self.speed_layer_data["reviews"] = self.speed_layer_data["reviews"].union(realtime_reviews)
            
            # Tạo Real-time View
            if self._simulate_spark:
                # Mô phỏng tạo view
                self.realtime_views["reviews"] = self.speed_layer_data["reviews"].copy()
            else:
                # Tạo view thực tế
                self.realtime_views["reviews"] = self.speed_layer_data["reviews"]
            
            logger.info("Đã cập nhật Real-time View cho đánh giá")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Real-time View cho đánh giá: {e}")
    
    def _update_user_behavior_realtime_view(self):
        """
        Cập nhật Real-time View cho hành vi người dùng.
        """
        try:
            # Đọc dữ liệu hành vi người dùng từ Speed Layer
            behavior_path = os.path.join(self.realtime_input_dir, "user_behavior_realtime_*")
            
            if self._simulate_spark:
                # Mô phỏng đọc dữ liệu
                realtime_behavior = self._simulate_load_realtime_data('user_behavior')
            else:
                # Đọc dữ liệu thực tế
                realtime_behavior = self.spark.read.parquet(behavior_path)
            
            # Cập nhật Speed Layer
            if "user_behavior" not in self.speed_layer_data:
                self.speed_layer_data["user_behavior"] = realtime_behavior
            else:
                # Kết hợp dữ liệu cũ và mới
                if self._simulate_spark:
                    # Mô phỏng kết hợp
                    self.speed_layer_data["user_behavior"] = pd.concat([self.speed_layer_data["user_behavior"], realtime_behavior])
                else:
                    # Kết hợp thực tế
                    self.speed_layer_data["user_behavior"] = self.speed_layer_data["user_behavior"].union(realtime_behavior)
            
            # Tạo Real-time View
            if self._simulate_spark:
                # Mô phỏng tạo view
                self.realtime_views["user_behavior"] = self.speed_layer_data["user_behavior"].copy()
            else:
                # Tạo view thực tế
                self.realtime_views["user_behavior"] = self.speed_layer_data["user_behavior"]
            
            logger.info("Đã cập nhật Real-time View cho hành vi người dùng")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Real-time View cho hành vi người dùng: {e}")
    
    def _update_recommendations_realtime_view(self):
        """
        Cập nhật Real-time View cho gợi ý sản phẩm.
        """
        try:
            # Đọc dữ liệu gợi ý sản phẩm từ Speed Layer
            recommendations_path = os.path.join(self.realtime_input_dir, "*recommendations_realtime_*")
            
            if self._simulate_spark:
                # Mô phỏng đọc dữ liệu
                realtime_recommendations = self._simulate_load_realtime_data('recommendations')
            else:
                # Đọc dữ liệu thực tế
                realtime_recommendations = self.spark.read.parquet(recommendations_path)
            
            # Cập nhật Speed Layer
            if "recommendations" not in self.speed_layer_data:
                self.speed_layer_data["recommendations"] = realtime_recommendations
            else:
                # Kết hợp dữ liệu cũ và mới
                if self._simulate_spark:
                    # Mô phỏng kết hợp
                    self.speed_layer_data["recommendations"] = pd.concat([self.speed_layer_data["recommendations"], realtime_recommendations])
                else:
                    # Kết hợp thực tế
                    self.speed_layer_data["recommendations"] = self.speed_layer_data["recommendations"].union(realtime_recommendations)
            
            # Tạo Real-time View
            if self._simulate_spark:
                # Mô phỏng tạo view
                self.realtime_views["recommendations"] = self.speed_layer_data["recommendations"].copy()
            else:
                # Tạo view thực tế
                self.realtime_views["recommendations"] = self.speed_layer_data["recommendations"]
            
            logger.info("Đã cập nhật Real-time View cho gợi ý sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật Real-time View cho gợi ý sản phẩm: {e}")
    
    def _simulate_update_products_batch_view(self):
        """
        Mô phỏng cập nhật Batch View cho sản phẩm.
        """
        try:
            # Tạo dữ liệu sản phẩm mô phỏng
            products_data = [
                {
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "price_numeric": 15990000.0,
                    "rating": 4.8,
                    "sold": "1k2",
                    "sold_numeric": 1200.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/123456789",
                    "popularity_score": 5.76,
                    "category": "Điện thoại & Phụ kiện",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "price_numeric": 29990000.0,
                    "rating": 4.9,
                    "sold": "2k5",
                    "sold_numeric": 2500.0,
                    "location": "Hà Nội",
                    "url": "https://shopee.vn/product/987654321",
                    "popularity_score": 12.25,
                    "category": "Điện thoại & Phụ kiện",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "product_id": "123123123",
                    "name": "Xiaomi Mi 11",
                    "price": "12.490.000đ",
                    "price_numeric": 12490000.0,
                    "rating": 4.7,
                    "sold": "950",
                    "sold_numeric": 950.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/123123123",
                    "popularity_score": 4.5,
                    "category": "Điện thoại & Phụ kiện",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "product_id": "456456456",
                    "name": "Laptop Dell XPS 13",
                    "price": "32.990.000đ",
                    "price_numeric": 32990000.0,
                    "rating": 4.8,
                    "sold": "320",
                    "sold_numeric": 320.0,
                    "location": "Hà Nội",
                    "url": "https://shopee.vn/product/456456456",
                    "popularity_score": 3.8,
                    "category": "Máy tính & Laptop",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "product_id": "789789789",
                    "name": "Tai nghe Apple AirPods Pro",
                    "price": "5.990.000đ",
                    "price_numeric": 5990000.0,
                    "rating": 4.9,
                    "sold": "1k8",
                    "sold_numeric": 1800.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/789789789",
                    "popularity_score": 8.82,
                    "category": "Thiết bị âm thanh",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Chuyển đổi thành DataFrame
            products_df = pd.DataFrame(products_data)
            
            # Tạo view cho top sản phẩm
            top_products = products_df.sort_values(by='popularity_score', ascending=False).head(100)
            
            # Tạo view cho sản phẩm theo danh mục
            products_by_category = products_df.groupby('category').agg({
                'product_id': 'count',
                'price_numeric': 'mean',
                'rating': 'mean'
            }).reset_index().rename(columns={
                'product_id': 'product_count',
                'price_numeric': 'avg_price',
                'rating': 'avg_rating'
            })
            
            # Lưu vào Batch View
            self.batch_views["top_products"] = top_products
            self.batch_views["products_by_category"] = products_by_category
            
            logger.info("Đã mô phỏng cập nhật Batch View cho sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng cập nhật Batch View cho sản phẩm: {e}")
    
    def _simulate_update_reviews_batch_view(self):
        """
        Mô phỏng cập nhật Batch View cho đánh giá.
        """
        try:
            # Tạo dữ liệu đánh giá mô phỏng
            reviews_data = [
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
                },
                {
                    "username": "user789",
                    "rating": 4,
                    "comment": "Sản phẩm tốt, nhưng giao hàng hơi chậm.",
                    "comment_length": 38,
                    "time": "2025-03-17",
                    "product_id": "123456789",
                    "sentiment_score": 0.5,
                    "review_weight": 1.2,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "username": "user101",
                    "rating": 5,
                    "comment": "Tuyệt vời, sẽ mua lại lần sau!",
                    "comment_length": 32,
                    "time": "2025-03-18",
                    "product_id": "123123123",
                    "sentiment_score": 1.0,
                    "review_weight": 1.3,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "username": "user202",
                    "rating": 3,
                    "comment": "Sản phẩm tạm được, không quá xuất sắc.",
                    "comment_length": 40,
                    "time": "2025-03-19",
                    "product_id": "456456456",
                    "sentiment_score": 0.0,
                    "review_weight": 1.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Chuyển đổi thành DataFrame
            reviews_df = pd.DataFrame(reviews_data)
            
            # Tạo view cho phân tích sentiment
            sentiment_analysis = reviews_df.groupby('product_id').agg({
                'sentiment_score': 'mean',
                'product_id': 'count'
            }).reset_index().rename(columns={
                'sentiment_score': 'avg_sentiment',
                'product_id': 'review_count'
            })
            
            # Tạo view cho phân phối rating
            rating_distribution = reviews_df.groupby(['product_id', 'rating']).size().reset_index(name='count')
            
            # Lưu vào Batch View
            self.batch_views["sentiment_analysis"] = sentiment_analysis
            self.batch_views["rating_distribution"] = rating_distribution
            
            logger.info("Đã mô phỏng cập nhật Batch View cho đánh giá")
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng cập nhật Batch View cho đánh giá: {e}")
    
    def _simulate_update_user_behavior_batch_view(self):
        """
        Mô phỏng cập nhật Batch View cho hành vi người dùng.
        """
        try:
            # Tạo dữ liệu hành vi người dùng mô phỏng
            behavior_data = [
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
                },
                {
                    "user_id": "user789",
                    "product_id": "123123123",
                    "action_frequency": 3,
                    "interaction_score": 8.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user789",
                    "product_id": "456456456",
                    "action_frequency": 2,
                    "interaction_score": 5.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Chuyển đổi thành DataFrame
            behavior_df = pd.DataFrame(behavior_data)
            
            # Tạo view cho phân tích hành vi người dùng
            user_behavior_analysis = behavior_df.groupby('user_id').agg({
                'action_frequency': 'sum',
                'interaction_score': 'mean'
            }).reset_index().rename(columns={
                'action_frequency': 'total_actions',
                'interaction_score': 'avg_interaction'
            })
            
            # Tạo view cho sản phẩm phổ biến dựa trên hành vi
            popular_products = behavior_df.groupby('product_id').agg({
                'action_frequency': 'sum',
                'interaction_score': 'sum'
            }).reset_index().rename(columns={
                'action_frequency': 'total_actions',
                'interaction_score': 'total_interaction'
            }).sort_values(by='total_interaction', ascending=False)
            
            # Lưu vào Batch View
            self.batch_views["user_behavior_analysis"] = user_behavior_analysis
            self.batch_views["popular_products"] = popular_products
            
            logger.info("Đã mô phỏng cập nhật Batch View cho hành vi người dùng")
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng cập nhật Batch View cho hành vi người dùng: {e}")
    
    def _simulate_update_recommendations_batch_view(self):
        """
        Mô phỏng cập nhật Batch View cho gợi ý sản phẩm.
        """
        try:
            # Tạo dữ liệu gợi ý sản phẩm mô phỏng
            recommendations_data = [
                {
                    "user_id": "user123",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "cf_score": 0.85,
                    "mf_score": 0.78,
                    "hybrid_score": 0.82,
                    "algorithm": "hybrid",
                    "popularity_score": 5.76,
                    "recommendation_score": 4.7232,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user123",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "cf_score": 0.72,
                    "mf_score": 0.65,
                    "hybrid_score": 0.68,
                    "algorithm": "hybrid",
                    "popularity_score": 12.25,
                    "recommendation_score": 8.33,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "cf_score": 0.95,
                    "mf_score": 0.88,
                    "hybrid_score": 0.92,
                    "algorithm": "hybrid",
                    "popularity_score": 12.25,
                    "recommendation_score": 11.27,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user456",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "cf_score": 0.65,
                    "mf_score": 0.72,
                    "hybrid_score": 0.68,
                    "algorithm": "hybrid",
                    "popularity_score": 5.76,
                    "recommendation_score": 3.9168,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Chuyển đổi thành DataFrame
            recommendations_df = pd.DataFrame(recommendations_data)
            
            # Tạo view cho gợi ý sản phẩm theo người dùng
            user_recommendations = recommendations_df.sort_values(by=['user_id', 'recommendation_score'], ascending=[True, False])
            
            # Tạo view cho so sánh các thuật toán gợi ý
            algorithm_comparison = recommendations_df.pivot_table(
                index='user_id',
                columns='algorithm',
                values='recommendation_score',
                aggfunc='mean'
            ).reset_index()
            
            # Lưu vào Batch View
            self.batch_views["user_recommendations"] = user_recommendations
            self.batch_views["algorithm_comparison"] = algorithm_comparison
            
            logger.info("Đã mô phỏng cập nhật Batch View cho gợi ý sản phẩm")
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng cập nhật Batch View cho gợi ý sản phẩm: {e}")
    
    def _simulate_load_realtime_data(self, data_type):
        """
        Mô phỏng tải dữ liệu real-time.
        
        Args:
            data_type (str): Loại dữ liệu ('products', 'reviews', 'user_behavior', 'recommendations')
            
        Returns:
            DataFrame: DataFrame mô phỏng
        """
        # Tạo dữ liệu mô phỏng
        if data_type == 'products':
            # Tạo dữ liệu sản phẩm mô phỏng
            data = [
                {
                    "product_id": "111222333",
                    "name": "Đồng hồ thông minh Apple Watch Series 7",
                    "price": "10.990.000đ",
                    "price_numeric": 10990000.0,
                    "rating": 4.9,
                    "sold": "250",
                    "sold_numeric": 250.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/111222333",
                    "popularity_score": 6.12,
                    "category": "Đồng hồ thông minh",
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
        elif data_type == 'reviews':
            # Tạo dữ liệu đánh giá mô phỏng
            data = [
                {
                    "username": "user505",
                    "rating": 5,
                    "comment": "Sản phẩm mới nhận, rất hài lòng với chất lượng!",
                    "comment_length": 50,
                    "time": datetime.now().strftime("%Y-%m-%d"),
                    "product_id": "111222333",
                    "sentiment_score": 1.0,
                    "review_weight": 1.5,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
        elif data_type == 'user_behavior':
            # Tạo dữ liệu hành vi người dùng mô phỏng
            data = [
                {
                    "user_id": "user123",
                    "product_id": "111222333",
                    "action_frequency": 2,
                    "interaction_score": 5.0,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
        elif data_type == 'recommendations':
            # Tạo dữ liệu gợi ý sản phẩm mô phỏng
            data = [
                {
                    "user_id": "user123",
                    "product_id": "111222333",
                    "name": "Đồng hồ thông minh Apple Watch Series 7",
                    "price": "10.990.000đ",
                    "rating": 4.9,
                    "cf_score": 0.88,
                    "mf_score": 0.82,
                    "hybrid_score": 0.85,
                    "algorithm": "hybrid",
                    "popularity_score": 6.12,
                    "recommendation_score": 5.202,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
        else:
            logger.error(f"Loại dữ liệu không hợp lệ: {data_type}")
            return pd.DataFrame()
        
        # Chuyển đổi thành DataFrame
        df = pd.DataFrame(data)
        
        logger.info(f"Đã mô phỏng tải dữ liệu real-time {data_type}: {len(data)} bản ghi")
        return df
    
    def _load_batch_views(self):
        """
        Tải Batch View đã lưu.
        """
        try:
            # Tìm tất cả các file JSON trong thư mục Batch View
            batch_view_files = [f for f in os.listdir(self.batch_view_dir) if f.endswith('.json')]
            
            # Tải từng file
            for file in batch_view_files:
                # Lấy tên view
                view_name = file.split('.')[0]
                
                # Đọc file
                with open(os.path.join(self.batch_view_dir, file), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Chuyển đổi thành DataFrame
                if self._simulate_spark:
                    self.batch_views[view_name] = pd.DataFrame(data)
                else:
                    self.batch_views[view_name] = self.spark.createDataFrame(data)
            
            logger.info(f"Đã tải {len(batch_view_files)} Batch View")
            
        except Exception as e:
            logger.error(f"Lỗi khi tải Batch View: {e}")
    
    def _load_realtime_views(self):
        """
        Tải Real-time View đã lưu.
        """
        try:
            # Tìm tất cả các file JSON trong thư mục Real-time View
            realtime_view_files = [f for f in os.listdir(self.realtime_view_dir) if f.endswith('.json')]
            
            # Tải từng file
            for file in realtime_view_files:
                # Lấy tên view
                view_name = file.split('.')[0]
                
                # Đọc file
                with open(os.path.join(self.realtime_view_dir, file), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Chuyển đổi thành DataFrame
                if self._simulate_spark:
                    self.realtime_views[view_name] = pd.DataFrame(data)
                else:
                    self.realtime_views[view_name] = self.spark.createDataFrame(data)
            
            logger.info(f"Đã tải {len(realtime_view_files)} Real-time View")
            
        except Exception as e:
            logger.error(f"Lỗi khi tải Real-time View: {e}")
    
    def _save_batch_views(self):
        """
        Lưu Batch View.
        """
        try:
            # Lưu từng view
            for view_name, view_data in self.batch_views.items():
                # Xác định đường dẫn đầu ra
                output_file = os.path.join(self.batch_view_dir, f"{view_name}.json")
                
                # Chuyển đổi thành JSON
                if self._simulate_spark:
                    # Nếu là pandas DataFrame
                    view_data.to_json(output_file, orient='records', lines=False)
                else:
                    # Nếu là Spark DataFrame
                    view_data.write.json(output_file, mode='overwrite')
            
            logger.info(f"Đã lưu {len(self.batch_views)} Batch View")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu Batch View: {e}")
    
    def _save_realtime_views(self):
        """
        Lưu Real-time View.
        """
        try:
            # Lưu từng view
            for view_name, view_data in self.realtime_views.items():
                # Xác định đường dẫn đầu ra
                output_file = os.path.join(self.realtime_view_dir, f"{view_name}.json")
                
                # Chuyển đổi thành JSON
                if self._simulate_spark:
                    # Nếu là pandas DataFrame
                    view_data.to_json(output_file, orient='records', lines=False)
                else:
                    # Nếu là Spark DataFrame
                    view_data.write.json(output_file, mode='overwrite')
            
            logger.info(f"Đã lưu {len(self.realtime_views)} Real-time View")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu Real-time View: {e}")
    
    def get_merged_view(self, view_name):
        """
        Lấy view kết hợp từ Batch View và Real-time View.
        
        Args:
            view_name (str): Tên view
            
        Returns:
            DataFrame: View kết hợp
        """
        try:
            # Kiểm tra view có tồn tại không
            if view_name not in self.batch_views and view_name not in self.realtime_views:
                logger.error(f"View {view_name} không tồn tại")
                return None
            
            # Lấy Batch View
            batch_view = self.batch_views.get(view_name)
            
            # Lấy Real-time View
            realtime_view = self.realtime_views.get(view_name)
            
            # Kết hợp view
            if batch_view is None:
                return realtime_view
            elif realtime_view is None:
                return batch_view
            else:
                # Kết hợp Batch View và Real-time View
                if self._simulate_spark:
                    # Mô phỏng kết hợp
                    merged_view = pd.concat([batch_view, realtime_view])
                else:
                    # Kết hợp thực tế
                    merged_view = batch_view.union(realtime_view)
                
                return merged_view
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy view kết hợp {view_name}: {e}")
            return None
    
    def query_view(self, view_name, query_func):
        """
        Truy vấn view.
        
        Args:
            view_name (str): Tên view
            query_func (function): Hàm truy vấn
            
        Returns:
            DataFrame: Kết quả truy vấn
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view(view_name)
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            result = query_func(merged_view)
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi truy vấn view {view_name}: {e}")
            return None
    
    def get_top_products(self, limit=10):
        """
        Lấy danh sách sản phẩm hàng đầu.
        
        Args:
            limit (int): Số lượng sản phẩm tối đa
            
        Returns:
            DataFrame: Danh sách sản phẩm
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view("top_products")
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            if self._simulate_spark:
                # Mô phỏng truy vấn
                result = merged_view.sort_values(by='popularity_score', ascending=False).head(limit)
            else:
                # Truy vấn thực tế
                result = merged_view.orderBy(desc("popularity_score")).limit(limit)
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách sản phẩm hàng đầu: {e}")
            return None
    
    def get_product_recommendations(self, user_id, limit=10):
        """
        Lấy gợi ý sản phẩm cho người dùng.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng gợi ý tối đa
            
        Returns:
            DataFrame: Danh sách gợi ý sản phẩm
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view("user_recommendations")
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            if self._simulate_spark:
                # Mô phỏng truy vấn
                result = merged_view[merged_view['user_id'] == user_id] \
                    .sort_values(by='recommendation_score', ascending=False) \
                    .head(limit)
            else:
                # Truy vấn thực tế
                result = merged_view.filter(col("user_id") == user_id) \
                    .orderBy(desc("recommendation_score")) \
                    .limit(limit)
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy gợi ý sản phẩm cho người dùng {user_id}: {e}")
            return None
    
    def get_product_sentiment(self, product_id):
        """
        Lấy phân tích sentiment cho sản phẩm.
        
        Args:
            product_id (str): ID của sản phẩm
            
        Returns:
            dict: Phân tích sentiment
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view("sentiment_analysis")
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            if self._simulate_spark:
                # Mô phỏng truy vấn
                result = merged_view[merged_view['product_id'] == product_id]
                
                if result.empty:
                    return None
                
                return result.iloc[0].to_dict()
            else:
                # Truy vấn thực tế
                result = merged_view.filter(col("product_id") == product_id)
                
                if result.count() == 0:
                    return None
                
                return result.first().asDict()
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy phân tích sentiment cho sản phẩm {product_id}: {e}")
            return None
    
    def get_user_behavior_analysis(self, user_id):
        """
        Lấy phân tích hành vi người dùng.
        
        Args:
            user_id (str): ID của người dùng
            
        Returns:
            dict: Phân tích hành vi
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view("user_behavior_analysis")
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            if self._simulate_spark:
                # Mô phỏng truy vấn
                result = merged_view[merged_view['user_id'] == user_id]
                
                if result.empty:
                    return None
                
                return result.iloc[0].to_dict()
            else:
                # Truy vấn thực tế
                result = merged_view.filter(col("user_id") == user_id)
                
                if result.count() == 0:
                    return None
                
                return result.first().asDict()
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy phân tích hành vi người dùng {user_id}: {e}")
            return None
    
    def compare_recommendation_algorithms(self, user_id):
        """
        So sánh các thuật toán gợi ý cho người dùng.
        
        Args:
            user_id (str): ID của người dùng
            
        Returns:
            dict: Kết quả so sánh
        """
        try:
            # Lấy view kết hợp
            merged_view = self.get_merged_view("algorithm_comparison")
            
            if merged_view is None:
                return None
            
            # Thực thi truy vấn
            if self._simulate_spark:
                # Mô phỏng truy vấn
                result = merged_view[merged_view['user_id'] == user_id]
                
                if result.empty:
                    return None
                
                return result.iloc[0].to_dict()
            else:
                # Truy vấn thực tế
                result = merged_view.filter(col("user_id") == user_id)
                
                if result.count() == 0:
                    return None
                
                return result.first().asDict()
            
        except Exception as e:
            logger.error(f"Lỗi khi so sánh các thuật toán gợi ý cho người dùng {user_id}: {e}")
            return None


if __name__ == "__main__":
    # Ví dụ sử dụng
    serving_layer = ShopeeServingLayer()
    
    try:
        # Bắt đầu Serving Layer
        serving_layer.start()
        
        # Lấy danh sách sản phẩm hàng đầu
        top_products = serving_layer.get_top_products(5)
        print(f"Top 5 sản phẩm: {top_products}")
        
        # Lấy gợi ý sản phẩm cho người dùng
        user_recommendations = serving_layer.get_product_recommendations("user123", 3)
        print(f"Gợi ý cho user123: {user_recommendations}")
        
        # Lấy phân tích sentiment cho sản phẩm
        product_sentiment = serving_layer.get_product_sentiment("123456789")
        print(f"Phân tích sentiment cho sản phẩm 123456789: {product_sentiment}")
        
        # Lấy phân tích hành vi người dùng
        user_behavior = serving_layer.get_user_behavior_analysis("user123")
        print(f"Phân tích hành vi người dùng user123: {user_behavior}")
        
        # So sánh các thuật toán gợi ý
        algorithm_comparison = serving_layer.compare_recommendation_algorithms("user123")
        print(f"So sánh thuật toán gợi ý cho user123: {algorithm_comparison}")
    finally:
        # Dừng Serving Layer
        serving_layer.stop()
