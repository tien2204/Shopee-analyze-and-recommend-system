#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark SQL Manager cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này quản lý việc truy vấn dữ liệu sử dụng Spark SQL.
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
except ImportError:
    print("PySpark hoặc các module liên quan không khả dụng. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_spark_sql')

class ShopeeSQLManager:
    """
    Lớp ShopeeSQLManager quản lý việc truy vấn dữ liệu sử dụng Spark SQL.
    """
    
    def __init__(self, app_name='ShopeeSQLManager', master='local[*]',
                 input_dir='../data/batch_processed',
                 output_dir='../data/query_results'):
        """
        Khởi tạo Spark SQL Manager với các tùy chọn cấu hình.
        
        Args:
            app_name (str): Tên ứng dụng Spark
            master (str): URL của Spark master
            input_dir (str): Thư mục đầu vào chứa dữ liệu đã xử lý
            output_dir (str): Thư mục đầu ra cho kết quả truy vấn
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
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info(f"Đã khởi tạo Spark với master {self.master}")
            self._simulate_spark = False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Spark.")
            self._simulate_spark = True
    
    def load_data(self, data_type):
        """
        Tải dữ liệu từ các nguồn khác nhau.
        
        Args:
            data_type (str): Loại dữ liệu ('products', 'reviews', 'user_behavior', 'recommendations')
            
        Returns:
            DataFrame: DataFrame chứa dữ liệu
        """
        if self._simulate_spark:
            return self._simulate_load_data(data_type)
        
        try:
            # Xác định đường dẫn đến dữ liệu
            if data_type == 'products':
                data_path = os.path.join(self.input_dir, "products_batch_*")
            elif data_type == 'reviews':
                data_path = os.path.join(self.input_dir, "reviews_batch_*")
            elif data_type == 'user_behavior':
                data_path = os.path.join(self.input_dir, "user_behavior_batch_*")
            elif data_type == 'recommendations':
                # Tìm tất cả các loại gợi ý
                data_path = os.path.join(self.input_dir, "*recommendations_*")
            else:
                logger.error(f"Loại dữ liệu không hợp lệ: {data_type}")
                return None
            
            # Đọc dữ liệu
            df = self.spark.read.parquet(data_path)
            
            logger.info(f"Đã tải dữ liệu {data_type}: {df.count()} bản ghi")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu {data_type}: {e}")
            return self._simulate_load_data(data_type)
    
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
        if not self._simulate_spark:
            df = self.spark.createDataFrame(data)
        else:
            df = pd.DataFrame(data)
        
        logger.info(f"Đã mô phỏng tải dữ liệu {data_type}: {len(data)} bản ghi")
        return df
    
    def create_temp_view(self, df, view_name):
        """
        Tạo temporary view từ DataFrame.
        
        Args:
            df: DataFrame
            view_name (str): Tên view
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if self._simulate_spark:
            logger.info(f"Đang mô phỏng tạo temporary view {view_name}")
            return True
        
        try:
            # Tạo temporary view
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Đã tạo temporary view {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo temporary view {view_name}: {e}")
            return False
    
    def execute_query(self, query):
        """
        Thực thi truy vấn SQL.
        
        Args:
            query (str): Truy vấn SQL
            
        Returns:
            DataFrame: Kết quả truy vấn
        """
        if self._simulate_spark:
            return self._simulate_execute_query(query)
        
        try:
            # Thực thi truy vấn
            result = self.spark.sql(query)
            
            logger.info(f"Đã thực thi truy vấn: {query}")
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi thực thi truy vấn: {e}")
            return None
    
    def _simulate_execute_query(self, query):
        """
        Mô phỏng thực thi truy vấn SQL.
        
        Args:
            query (str): Truy vấn SQL
            
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
        if self._simulate_spark:
            return self._simulate_save_query_result(df, output_name, format)
        
        try:
            # Xác định đường dẫn đầu ra
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"{output_name}_{timestamp}")
            
            # Lưu kết quả theo định dạng
            if format == 'json':
                df.write.json(output_path)
                output_file = f"{output_path}.json"
            elif format == 'csv':
                df.write.csv(output_path, header=True)
                output_file = f"{output_path}.csv"
            elif format == 'parquet':
                df.write.parquet(output_path)
                output_file = f"{output_path}.parquet"
            else:
                logger.error(f"Định dạng không hợp lệ: {format}")
                return None
            
            logger.info(f"Đã lưu kết quả truy vấn vào {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu kết quả truy vấn: {e}")
            return None
    
    def _simulate_save_query_result(self, df, output_name, format='json'):
        """
        Mô phỏng lưu kết quả truy vấn.
        
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
                    # Nếu là list of dict
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(df, f, ensure_ascii=False, indent=4)
            elif format == 'csv':
                if isinstance(df, pd.DataFrame):
                    df.to_csv(output_file, index=False)
                else:
                    # Nếu là list of dict
                    pd.DataFrame(df).to_csv(output_file, index=False)
            elif format == 'parquet':
                if isinstance(df, pd.DataFrame):
                    df.to_parquet(output_file)
                else:
                    # Nếu là list of dict
                    pd.DataFrame(df).to_parquet(output_file)
            else:
                logger.error(f"Định dạng không hợp lệ: {format}")
                return None
            
            logger.info(f"Đã mô phỏng lưu kết quả truy vấn vào {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng lưu kết quả truy vấn: {e}")
            return None
    
    def get_top_products(self, limit=10):
        """
        Lấy danh sách sản phẩm hàng đầu dựa trên điểm phổ biến.
        
        Args:
            limit (int): Số lượng sản phẩm tối đa
            
        Returns:
            DataFrame: Danh sách sản phẩm
        """
        # Tải dữ liệu sản phẩm
        products_df = self.load_data('products')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(products_df, pd.DataFrame):
                result = products_df.sort_values(by='popularity_score', ascending=False).head(limit)
            else:
                result = sorted(products_df, key=lambda x: x.get('popularity_score', 0), reverse=True)[:limit]
            
            return result
        else:
            # Tạo temporary view
            self.create_temp_view(products_df, 'products')
            
            # Thực thi truy vấn
            query = f"""
            SELECT * FROM products
            ORDER BY popularity_score DESC
            LIMIT {limit}
            """
            
            return self.execute_query(query)
    
    def get_product_recommendations(self, user_id, limit=10):
        """
        Lấy gợi ý sản phẩm cho người dùng.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng gợi ý tối đa
            
        Returns:
            DataFrame: Danh sách gợi ý sản phẩm
        """
        # Tải dữ liệu gợi ý
        recommendations_df = self.load_data('recommendations')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(recommendations_df, pd.DataFrame):
                result = recommendations_df[recommendations_df['user_id'] == user_id] \
                    .sort_values(by='recommendation_score', ascending=False) \
                    .head(limit)
            else:
                result = [r for r in recommendations_df if r.get('user_id') == user_id]
                result = sorted(result, key=lambda x: x.get('recommendation_score', 0), reverse=True)[:limit]
            
            return result
        else:
            # Tạo temporary view
            self.create_temp_view(recommendations_df, 'recommendations')
            
            # Thực thi truy vấn
            query = f"""
            SELECT * FROM recommendations
            WHERE user_id = '{user_id}'
            ORDER BY recommendation_score DESC
            LIMIT {limit}
            """
            
            return self.execute_query(query)
    
    def get_product_reviews(self, product_id, limit=10):
        """
        Lấy đánh giá của sản phẩm.
        
        Args:
            product_id (str): ID của sản phẩm
            limit (int): Số lượng đánh giá tối đa
            
        Returns:
            DataFrame: Danh sách đánh giá
        """
        # Tải dữ liệu đánh giá
        reviews_df = self.load_data('reviews')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(reviews_df, pd.DataFrame):
                result = reviews_df[reviews_df['product_id'] == product_id] \
                    .sort_values(by=['review_weight', 'time'], ascending=[False, False]) \
                    .head(limit)
            else:
                result = [r for r in reviews_df if r.get('product_id') == product_id]
                result = sorted(result, key=lambda x: (x.get('review_weight', 0), x.get('time', '')), reverse=True)[:limit]
            
            return result
        else:
            # Tạo temporary view
            self.create_temp_view(reviews_df, 'reviews')
            
            # Thực thi truy vấn
            query = f"""
            SELECT * FROM reviews
            WHERE product_id = '{product_id}'
            ORDER BY review_weight DESC, time DESC
            LIMIT {limit}
            """
            
            return self.execute_query(query)
    
    def get_user_behavior(self, user_id, limit=10):
        """
        Lấy hành vi của người dùng.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng hành vi tối đa
            
        Returns:
            DataFrame: Danh sách hành vi
        """
        # Tải dữ liệu hành vi người dùng
        behavior_df = self.load_data('user_behavior')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(behavior_df, pd.DataFrame):
                result = behavior_df[behavior_df['user_id'] == user_id] \
                    .sort_values(by='interaction_score', ascending=False) \
                    .head(limit)
            else:
                result = [r for r in behavior_df if r.get('user_id') == user_id]
                result = sorted(result, key=lambda x: x.get('interaction_score', 0), reverse=True)[:limit]
            
            return result
        else:
            # Tạo temporary view
            self.create_temp_view(behavior_df, 'user_behavior')
            
            # Thực thi truy vấn
            query = f"""
            SELECT * FROM user_behavior
            WHERE user_id = '{user_id}'
            ORDER BY interaction_score DESC
            LIMIT {limit}
            """
            
            return self.execute_query(query)
    
    def get_product_details(self, product_id):
        """
        Lấy thông tin chi tiết của sản phẩm.
        
        Args:
            product_id (str): ID của sản phẩm
            
        Returns:
            dict: Thông tin sản phẩm
        """
        # Tải dữ liệu sản phẩm
        products_df = self.load_data('products')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(products_df, pd.DataFrame):
                result = products_df[products_df['product_id'] == product_id]
                if not result.empty:
                    return result.iloc[0].to_dict()
            else:
                for product in products_df:
                    if product.get('product_id') == product_id:
                        return product
            
            return None
        else:
            # Tạo temporary view
            self.create_temp_view(products_df, 'products')
            
            # Thực thi truy vấn
            query = f"""
            SELECT * FROM products
            WHERE product_id = '{product_id}'
            """
            
            result = self.execute_query(query)
            
            if result and result.count() > 0:
                return result.first().asDict()
            else:
                return None
    
    def compare_recommendation_algorithms(self, user_id, limit=10):
        """
        So sánh kết quả của các thuật toán gợi ý khác nhau.
        
        Args:
            user_id (str): ID của người dùng
            limit (int): Số lượng gợi ý tối đa
            
        Returns:
            dict: Kết quả so sánh
        """
        # Tải dữ liệu gợi ý
        recommendations_df = self.load_data('recommendations')
        
        if self._simulate_spark:
            # Mô phỏng truy vấn
            if isinstance(recommendations_df, pd.DataFrame):
                user_recs = recommendations_df[recommendations_df['user_id'] == user_id]
                
                # Phân loại theo thuật toán
                als_recs = user_recs[user_recs['cf_score'].notnull()] \
                    .sort_values(by='cf_score', ascending=False) \
                    .head(limit)
                
                mf_recs = user_recs[user_recs['mf_score'].notnull()] \
                    .sort_values(by='mf_score', ascending=False) \
                    .head(limit)
                
                hybrid_recs = user_recs[user_recs['hybrid_score'].notnull()] \
                    .sort_values(by='hybrid_score', ascending=False) \
                    .head(limit)
                
                return {
                    'als': als_recs.to_dict('records') if not als_recs.empty else [],
                    'mf': mf_recs.to_dict('records') if not mf_recs.empty else [],
                    'hybrid': hybrid_recs.to_dict('records') if not hybrid_recs.empty else []
                }
            else:
                user_recs = [r for r in recommendations_df if r.get('user_id') == user_id]
                
                # Phân loại theo thuật toán
                als_recs = [r for r in user_recs if 'cf_score' in r]
                als_recs = sorted(als_recs, key=lambda x: x.get('cf_score', 0), reverse=True)[:limit]
                
                mf_recs = [r for r in user_recs if 'mf_score' in r]
                mf_recs = sorted(mf_recs, key=lambda x: x.get('mf_score', 0), reverse=True)[:limit]
                
                hybrid_recs = [r for r in user_recs if 'hybrid_score' in r]
                hybrid_recs = sorted(hybrid_recs, key=lambda x: x.get('hybrid_score', 0), reverse=True)[:limit]
                
                return {
                    'als': als_recs,
                    'mf': mf_recs,
                    'hybrid': hybrid_recs
                }
        else:
            # Tạo temporary view
            self.create_temp_view(recommendations_df, 'recommendations')
            
            # Thực thi truy vấn cho ALS
            als_query = f"""
            SELECT * FROM recommendations
            WHERE user_id = '{user_id}' AND cf_score IS NOT NULL
            ORDER BY cf_score DESC
            LIMIT {limit}
            """
            
            als_result = self.execute_query(als_query)
            
            # Thực thi truy vấn cho MF
            mf_query = f"""
            SELECT * FROM recommendations
            WHERE user_id = '{user_id}' AND mf_score IS NOT NULL
            ORDER BY mf_score DESC
            LIMIT {limit}
            """
            
            mf_result = self.execute_query(mf_query)
            
            # Thực thi truy vấn cho Hybrid
            hybrid_query = f"""
            SELECT * FROM recommendations
            WHERE user_id = '{user_id}' AND hybrid_score IS NOT NULL
            ORDER BY hybrid_score DESC
            LIMIT {limit}
            """
            
            hybrid_result = self.execute_query(hybrid_query)
            
            return {
                'als': [row.asDict() for row in als_result.collect()] if als_result else [],
                'mf': [row.asDict() for row in mf_result.collect()] if mf_result else [],
                'hybrid': [row.asDict() for row in hybrid_result.collect()] if hybrid_result else []
            }
    
    def stop(self):
        """
        Dừng Spark và giải phóng tài nguyên.
        """
        if not self._simulate_spark and hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Đã dừng Spark")


if __name__ == "__main__":
    # Ví dụ sử dụng
    sql_manager = ShopeeSQLManager()
    
    try:
        # Lấy danh sách sản phẩm hàng đầu
        top_products = sql_manager.get_top_products(5)
        print(f"Top 5 sản phẩm: {top_products}")
        
        # Lấy gợi ý sản phẩm cho người dùng
        user_recommendations = sql_manager.get_product_recommendations("user123", 3)
        print(f"Gợi ý cho user123: {user_recommendations}")
        
        # So sánh các thuật toán gợi ý
        comparison = sql_manager.compare_recommendation_algorithms("user123")
        print(f"So sánh thuật toán: {comparison}")
    finally:
        # Dừng Spark
        sql_manager.stop()
