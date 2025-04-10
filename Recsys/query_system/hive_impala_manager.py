#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hive/Impala Manager cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này quản lý việc truy vấn dữ liệu sử dụng Hive/Impala.
"""

import os
import json
import logging
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
    from impala.dbapi import connect as impala_connect
    from impala.util import as_pandas
except ImportError:
    print("PySpark, Impala hoặc các module liên quan không khả dụng. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_hive_impala')

class ShopeeHiveImpalaManager:
    """
    Lớp ShopeeHiveImpalaManager quản lý việc truy vấn dữ liệu sử dụng Hive/Impala.
    """
    
    def __init__(self, app_name='ShopeeHiveImpalaManager', master='local[*]',
                 input_dir='../data/batch_processed',
                 output_dir='../data/query_results',
                 hive_host='localhost', hive_port=10000,
                 impala_host='localhost', impala_port=21050):
        """
        Khởi tạo Hive/Impala Manager với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            input_dir (str): Thư mục đầu vào chứa dữ liệu đã xử lý
            output_dir (str): Thư mục đầu ra cho kết quả truy vấn
            hive_host (str): Host của Hive server
            hive_port (int): Port của Hive server
            impala_host (str): Host của Impala server
            impala_port (int): Port của Impala server
        """
        self.app_name = app_name
        self.master = master
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.impala_host = impala_host
        self.impala_port = impala_port
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Khởi tạo Spark
        self._initialize_spark()
        
        # Khởi tạo kết nối Impala
        self._initialize_impala()
    
    def _initialize_spark(self):
        """
        Khởi tạo Spark với Hive support.
        """
        try:
            # Tạo SparkConf
            conf = SparkConf().setAppName(self.app_name).setMaster(self.master)
            
            # Cấu hình bổ sung
            conf.set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            
            # Khởi tạo SparkSession với Hive support
            self.spark = SparkSession.builder \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info(f"Đã khởi tạo Spark với Hive support, master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def _initialize_impala(self):
        """
        Khởi tạo kết nối Impala.
        """
        try:
            # Thử kết nối đến Impala
            self.impala_conn = impala_connect(
                host=self.impala_host,
                port=self.impala_port
            )
            
            logger.info(f"Đã kết nối đến Impala server {self.impala_host}:{self.impala_port}")
            self._simulate_impala = False
            
        except Exception as e:
            logger.error(f"Lỗi khi kết nối đến Impala: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Impala.")
            self._simulate_impala = True
    
    def create_hive_tables(self):
        """
        Tạo các bảng Hive từ dữ liệu đã xử lý.
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if self._simulate_spark:
            logger.info("Đang mô phỏng tạo bảng Hive")
            return True
        
        try:
            # Tạo database nếu chưa tồn tại
            self.spark.sql("CREATE DATABASE IF NOT EXISTS shopee")
            
            # Sử dụng database
            self.spark.sql("USE shopee")
            
            # Tạo bảng products
            products_path = os.path.join(self.input_dir, "products_batch_*")
            products_df = self.spark.read.parquet(products_path)
            
            products_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .saveAsTable("shopee.products")
            
            # Tạo bảng reviews
            reviews_path = os.path.join(self.input_dir, "reviews_batch_*")
            reviews_df = self.spark.read.parquet(reviews_path)
            
            reviews_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .saveAsTable("shopee.reviews")
            
            # Tạo bảng user_behavior
            behavior_path = os.path.join(self.input_dir, "user_behavior_batch_*")
            behavior_df = self.spark.read.parquet(behavior_path)
            
            behavior_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .saveAsTable("shopee.user_behavior")
            
            # Tạo bảng recommendations
            recommendations_path = os.path.join(self.input_dir, "*recommendations_*")
            recommendations_df = self.spark.read.parquet(recommendations_path)
            
            recommendations_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .saveAsTable("shopee.recommendations")
            
            logger.info("Đã tạo các bảng Hive thành công")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng Hive: {e}")
            return False
    
    def refresh_impala_metadata(self):
        """
        Làm mới metadata của Impala.
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if self._simulate_impala:
            logger.info("Đang mô phỏng làm mới metadata của Impala")
            return True
        
        try:
            # Tạo cursor
            cursor = self.impala_conn.cursor()
            
            # Làm mới metadata
            cursor.execute("INVALIDATE METADATA")
            
            # Đóng cursor
            cursor.close()
            
            logger.info("Đã làm mới metadata của Impala")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi làm mới metadata của Impala: {e}")
            return False
    
    def execute_hive_query(self, query):
        """
        Thực thi truy vấn Hive.
        
        Args:
            query (str): Truy vấn Hive
            
        Returns:
            DataFrame: Kết quả truy vấn
        """
        if self._simulate_spark:
            return self._simulate_execute_query(query)
        
        try:
            # Thực thi truy vấn
            result = self.spark.sql(query)
            
            logger.info(f"Đã thực thi truy vấn Hive: {query}")
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi thực thi truy vấn Hive: {e}")
            return None
    
    def execute_impala_query(self, query):
        """
        Thực thi truy vấn Impala.
        
        Args:
            query (str): Truy vấn Impala
            
        Returns:
            DataFrame: Kết quả truy vấn
        """
        if self._simulate_impala:
            return self._simulate_execute_query(query)
        
        try:
            # Tạo cursor
            cursor = self.impala_conn.cursor()
            
            # Thực thi truy vấn
            cursor.execute(query)
            
            # Chuyển đổi kết quả thành DataFrame
            result = as_pandas(cursor)
            
            # Đóng cursor
            cursor.close()
            
            logger.info(f"Đã thực thi truy vấn Impala: {query}")
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi thực thi truy vấn Impala: {e}")
            return None
    
    def _simulate_execute_query(self, query):
        """
        Mô phỏng thực thi truy vấn.
        
        Args:
            query (str): Truy vấn
            
        Returns:
            DataFrame: Kết quả truy vấn mô phỏng
        """
        logger.info(f"Đang mô phỏng thực thi truy vấn: {query}")
        
        # Phân tích truy vấn đơn giản
        query_lower = query.lower()
        
        # Mô phỏng kết quả dựa trên truy vấn
        if "product" in query_lower:
            return self._simulate_load_data('products')
        elif "review" in query_lower:
            return self._simulate_load_data('reviews')
        elif "user_behavior" in query_lower or "behavior" in query_lower:
            return self._simulate_load_data('user_behavior')
        elif "recommendation" in query_lower:
            return self._simulate_load_data('recommendations')
        else:
            # Trả về DataFrame trống
            return pd.DataFrame()
    
    def _simulate_load_data(self, data_type):
        """
        Mô phỏng tải dữ liệu.
        
        Args:
            data_type (str): Loại dữ liệu ('products', 'reviews', 'user_behavior', 'recommendations')
            
        Returns:
            DataFrame: DataFrame mô phỏng
        """
        logger.info(f"Đang mô phỏng tải dữ liệu {data_type}")
        
        # Tạo dữ liệu mô phỏng
        if data_type == 'products':
            # Tạo dữ liệu sản phẩm mô phỏng
            data = [
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
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
        elif data_type == 'reviews':
            # Tạo dữ liệu đánh giá mô phỏng
            data = [
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
        elif data_type == 'user_behavior':
            # Tạo dữ liệu hành vi người dùng mô phỏng
            data = [
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
        elif data_type == 'recommendations':
            # Tạo dữ liệu gợi ý sản phẩm mô phỏng
            data = [
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
        else:
            logger.error(f"Loại dữ liệu không hợp lệ: {data_type}")
            return None
        
        # Chuyển đổi thành DataFrame
        df = pd.DataFrame(data)
        
        logger.info(f"Đã mô phỏng tải dữ liệu {data_type}: {len(data)} bản ghi")
        return df
    
    def save_query_result(self, df, output_name, format='json'):
        """
        Lưu kết quả truy vấn.
        
        Args:
            df: DataFrame kết quả
            output_name (str): Tên file đầu ra
            format (str): Định dạng đầu ra ('json', 'csv', 'parquet')
            
        Returns:
            str: Đường dẫn đến file đầu ra
        """
        try:
            # Xác định đường dẫn đầu ra
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_dir, f"{output_name}_{timestamp}.{format}")
            
            # Lưu kết quả theo định dạng
            if format == 'json':
                if isinstance(df, pd.DataFrame):
                    df.to_json(output_file, orient='records', lines=True)
                else:
                    # Nếu là Spark DataFrame
                    df.write.json(output_file)
            elif format == 'csv':
                if isinstance(df, pd.DataFrame):
                    df.to_csv(output_file, index=False)
                else:
                    # Nếu là Spark DataFrame
                    df.write.csv(output_file, header=True)
            elif format == 'parquet':
                if isinstance(df, pd.DataFrame):
                    df.to_parquet(output_file)
                else:
                    # Nếu là Spark DataFrame
                    df.write.parquet(output_file)
            else:
                logger.error(f"Định dạng không hợp lệ: {format}")
                return None
            
            logger.info(f"Đã lưu kết quả truy vấn vào {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu kết quả truy vấn: {e}")
            return None
    
    def get_top_products_hive(self, limit=10):
        """
        Lấy danh sách sản phẩm hàng đầu dựa trên điểm phổ biến sử dụng Hive.
        
        Args:
            limit (int): Số lượng sản phẩm tối đa
            
        Returns:
            DataFrame: Danh sách sản phẩm
        """
        # Thực thi truy vấn Hive
        query = f"""
        SELECT * FROM shopee.products
        ORDER BY popularity_score DESC
        LIMIT {limit}
        """
        
        return self.execute_hive_query(query)
    
    def get_top_products_impala(self, limit=10):
        """
        Lấy danh sách sản phẩm hàng đầu dựa trên điểm phổ biến sử dụng Impala.
        
        Args:
            limit (int): Số lượng sản phẩm tối đa
            
        Returns:
            DataFrame: Danh sách sản phẩm
        """
        # Thực thi truy vấn Impala
        query = f"""
        SELECT * FROM shopee.products
        ORDER BY popularity_score DESC
        LIMIT {limit}
        """
        
        return self.execute_impala_query(query)
    
    def get_product_recommendations_hive(self, user_id, limit=10):
        """
        Lấy gợi ý sản phẩm cho người dùng sử dụng Hive.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng gợi ý tối đa
            
        Returns:
            DataFrame: Danh sách gợi ý sản phẩm
        """
        # Thực thi truy vấn Hive
        query = f"""
        SELECT * FROM shopee.recommendations
        WHERE user_id = '{user_id}'
        ORDER BY recommendation_score DESC
        LIMIT {limit}
        """
        
        return self.execute_hive_query(query)
    
    def get_product_recommendations_impala(self, user_id, limit=10):
        """
        Lấy gợi ý sản phẩm cho người dùng sử dụng Impala.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng gợi ý tối đa
            
        Returns:
            DataFrame: Danh sách gợi ý sản phẩm
        """
        # Thực thi truy vấn Impala
        query = f"""
        SELECT * FROM shopee.recommendations
        WHERE user_id = '{user_id}'
        ORDER BY recommendation_score DESC
        LIMIT {limit}
        """
        
        return self.execute_impala_query(query)
    
    def get_product_reviews_hive(self, product_id, limit=10):
        """
        Lấy đánh giá của sản phẩm sử dụng Hive.
        
        Args:
            product_id (str): ID của sản phẩm
            limit (int): Số lượng đánh giá tối đa
            
        Returns:
            DataFrame: Danh sách đánh giá
        """
        # Thực thi truy vấn Hive
        query = f"""
        SELECT * FROM shopee.reviews
        WHERE product_id = '{product_id}'
        ORDER BY review_weight DESC, time DESC
        LIMIT {limit}
        """
        
        return self.execute_hive_query(query)
    
    def get_product_reviews_impala(self, product_id, limit=10):
        """
        Lấy đánh giá của sản phẩm sử dụng Impala.
        
        Args:
            product_id (str): ID của sản phẩm
            limit (int): Số lượng đánh giá tối đa
            
        Returns:
            DataFrame: Danh sách đánh giá
        """
        # Thực thi truy vấn Impala
        query = f"""
        SELECT * FROM shopee.reviews
        WHERE product_id = '{product_id}'
        ORDER BY review_weight DESC, time DESC
        LIMIT {limit}
        """
        
        return self.execute_impala_query(query)
    
    def get_user_behavior_hive(self, user_id, limit=10):
        """
        Lấy hành vi của người dùng sử dụng Hive.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng hành vi tối đa
            
        Returns:
            DataFrame: Danh sách hành vi
        """
        # Thực thi truy vấn Hive
        query = f"""
        SELECT * FROM shopee.user_behavior
        WHERE user_id = '{user_id}'
        ORDER BY interaction_score DESC
        LIMIT {limit}
        """
        
        return self.execute_hive_query(query)
    
    def get_user_behavior_impala(self, user_id, limit=10):
        """
        Lấy hành vi của người dùng sử dụng Impala.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng hành vi tối đa
            
        Returns:
            DataFrame: Danh sách hành vi
        """
        # Thực thi truy vấn Impala
        query = f"""
        SELECT * FROM shopee.user_behavior
        WHERE user_id = '{user_id}'
        ORDER BY interaction_score DESC
        LIMIT {limit}
        """
        
        return self.execute_impala_query(query)
    
    def compare_query_performance(self, query):
        """
        So sánh hiệu suất truy vấn giữa Hive và Impala.
        
        Args:
            query (str): Truy vấn SQL
            
        Returns:
            dict: Kết quả so sánh
        """
        try:
            # Đo thời gian thực thi truy vấn Hive
            hive_start_time = datetime.now()
            hive_result = self.execute_hive_query(query)
            hive_end_time = datetime.now()
            hive_duration = (hive_end_time - hive_start_time).total_seconds()
            
            # Đo thời gian thực thi truy vấn Impala
            impala_start_time = datetime.now()
            impala_result = self.execute_impala_query(query)
            impala_end_time = datetime.now()
            impala_duration = (impala_end_time - impala_start_time).total_seconds()
            
            # Kết quả so sánh
            comparison = {
                'query': query,
                'hive_duration': hive_duration,
                'impala_duration': impala_duration,
                'speedup': hive_duration / impala_duration if impala_duration > 0 else float('inf')
            }
            
            logger.info(f"So sánh hiệu suất truy vấn: Hive {hive_duration:.2f}s, Impala {impala_duration:.2f}s, Speedup {comparison['speedup']:.2f}x")
            return comparison
            
        except Exception as e:
            logger.error(f"Lỗi khi so sánh hiệu suất truy vấn: {e}")
            return {
                'query': query,
                'hive_duration': 0,
                'impala_duration': 0,
                'speedup': 1.0,
                'error': str(e)
            }
    
    def stop(self):
        """
        Dừng Spark và đóng kết nối Impala.
        """
        # Dừng Spark
        if not self._simulate_spark and hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Đã dừng Spark")
        
        # Đóng kết nối Impala
        if not self._simulate_impala and hasattr(self, 'impala_conn'):
            self.impala_conn.close()
            logger.info("Đã đóng kết nối Impala")


if __name__ == "__main__":
    # Ví dụ sử dụng
    hive_impala_manager = ShopeeHiveImpalaManager()
    
    try:
        # Tạo các bảng Hive
        hive_impala_manager.create_hive_tables()
        
        # Làm mới metadata của Impala
        hive_impala_manager.refresh_impala_metadata()
        
        # Lấy danh sách sản phẩm hàng đầu sử dụng Hive
        top_products_hive = hive_impala_manager.get_top_products_hive(5)
        print(f"Top 5 sản phẩm (Hive): {top_products_hive}")
        
        # Lấy danh sách sản phẩm hàng đầu sử dụng Impala
        top_products_impala = hive_impala_manager.get_top_products_impala(5)
        print(f"Top 5 sản phẩm (Impala): {top_products_impala}")
        
        # So sánh hiệu suất truy vấn
        comparison = hive_impala_manager.compare_query_performance("""
        SELECT * FROM shopee.products
        ORDER BY popularity_score DESC
        LIMIT 10
        """)
        print(f"So sánh hiệu suất truy vấn: {comparison}")
    finally:
        # Dừng Spark và đóng kết nối Impala
        hive_impala_manager.stop()
