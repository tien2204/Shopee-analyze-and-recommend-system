#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for Shopee Recommendation System

Mô-đun này kiểm tra các thành phần của hệ thống gợi ý sản phẩm Shopee.
"""

import os
import sys
import json
import time
import unittest
import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process

# Thêm thư mục cha vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import các module từ hệ thống
try:
    from data_collection.shopee_scraper import ShopeeScraper
    from data_storage.hdfs_manager import HDFSManager
    from data_processing.spark_streaming import SparkStreamProcessor
    from data_processing.batch_processing import BatchProcessor
    from data_storage.elasticsearch_manager import ElasticsearchManager
    from recommendation_algorithms.collaborative_filtering import CollaborativeFiltering
    from recommendation_algorithms.matrix_factorization import MatrixFactorization
    from recommendation_algorithms.hybrid_methods import HybridRecommender
    from query_system.spark_sql_manager import ShopeeSQLManager
    from query_system.serving_layer import ShopeeServingLayer
except ImportError as e:
    print(f"Không thể import các module từ hệ thống: {e}")
    print("Đang chuyển sang chế độ kiểm tra API")

# Cấu hình Flask app
FLASK_HOST = "127.0.0.1"
FLASK_PORT = 5000
FLASK_URL = f"http://{FLASK_HOST}:{FLASK_PORT}"

# Tạo dữ liệu mẫu
def generate_sample_data():
    """
    Tạo dữ liệu mẫu cho việc kiểm thử.
    
    Returns:
        dict: Dữ liệu mẫu
    """
    # Dữ liệu sản phẩm
    products = [
        {
            'product_id': '123456789',
            'name': 'Samsung Galaxy S21',
            'price': '15.990.000đ',
            'price_numeric': 15990000,
            'rating': 4.8,
            'sold': '1k2',
            'location': 'TP. Hồ Chí Minh',
            'url': 'https://shopee.vn/product/123456789',
            'popularity_score': 5.76,
            'category': 'Điện thoại & Phụ kiện',
            'brand': 'Samsung',
            'description': 'Samsung Galaxy S21 5G là chiếc điện thoại Android cao cấp với màn hình Dynamic AMOLED 2X, chip Exynos 2100, camera 64MP và pin 4000mAh.',
            'image_url': '/static/images/samsung.jpg'
        },
        {
            'product_id': '987654321',
            'name': 'iPhone 13 Pro Max',
            'price': '29.990.000đ',
            'price_numeric': 29990000,
            'rating': 4.9,
            'sold': '2k5',
            'location': 'Hà Nội',
            'url': 'https://shopee.vn/product/987654321',
            'popularity_score': 12.25,
            'category': 'Điện thoại & Phụ kiện',
            'brand': 'Apple',
            'description': 'iPhone 13 Pro Max là chiếc điện thoại cao cấp nhất của Apple với màn hình Super Retina XDR, chip A15 Bionic, camera 12MP và pin dung lượng lớn.',
            'image_url': '/static/images/iphone13.jpg'
        }
    ]
    
    # Dữ liệu đánh giá
    reviews = [
        {
            'review_id': '1',
            'product_id': '123456789',
            'username': 'user123',
            'rating': 5,
            'comment': 'Sản phẩm rất tốt, đóng gói cẩn thận, giao hàng nhanh!',
            'time': '2025-03-15',
            'sentiment_score': 1.0
        },
        {
            'review_id': '2',
            'product_id': '987654321',
            'username': 'user456',
            'rating': 2,
            'comment': 'Sản phẩm không như mô tả, chất lượng kém.',
            'time': '2025-03-16',
            'sentiment_score': -1.0
        }
    ]
    
    # Dữ liệu người dùng
    users = [
        {
            'user_id': 'user123',
            'name': 'Nguyễn Văn A',
            'email': 'nguyenvana@example.com',
            'registration_date': '2024-01-15'
        },
        {
            'user_id': 'user456',
            'name': 'Trần Thị B',
            'email': 'tranthib@example.com',
            'registration_date': '2024-02-20'
        }
    ]
    
    # Dữ liệu hành vi người dùng
    user_behaviors = [
        {
            'behavior_id': '1',
            'user_id': 'user123',
            'product_id': '123456789',
            'action_type': 'view',
            'action_frequency': 3,
            'interaction_score': 9.0,
            'timestamp': '2025-03-20T10:15:30'
        },
        {
            'behavior_id': '2',
            'user_id': 'user123',
            'product_id': '987654321',
            'action_type': 'view',
            'action_frequency': 1,
            'interaction_score': 1.0,
            'timestamp': '2025-03-21T14:25:10'
        },
        {
            'behavior_id': '3',
            'user_id': 'user456',
            'product_id': '987654321',
            'action_type': 'purchase',
            'action_frequency': 4,
            'interaction_score': 14.0,
            'timestamp': '2025-03-22T09:30:45'
        }
    ]
    
    return {
        'products': products,
        'reviews': reviews,
        'users': users,
        'user_behaviors': user_behaviors
    }

# Lưu dữ liệu mẫu vào file
def save_sample_data(data, output_dir='./data'):
    """
    Lưu dữ liệu mẫu vào file.
    
    Args:
        data (dict): Dữ liệu mẫu
        output_dir (str): Thư mục đầu ra
    """
    os.makedirs(output_dir, exist_ok=True)
    
    for key, items in data.items():
        output_file = os.path.join(output_dir, f"{key}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        
        # Tạo file CSV
        df = pd.DataFrame(items)
        csv_file = os.path.join(output_dir, f"{key}.csv")
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"Đã lưu {len(items)} bản ghi vào {output_file} và {csv_file}")

# Khởi động Flask app
def start_flask_app():
    """
    Khởi động Flask app trong một process riêng.
    
    Returns:
        Process: Process chạy Flask app
    """
    from web_interface.app import app
    
    def run_app():
        app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)
    
    process = Process(target=run_app)
    process.start()
    
    # Đợi Flask app khởi động
    time.sleep(2)
    
    return process

# Kiểm tra API
def test_api_endpoints():
    """
    Kiểm tra các API endpoint của Flask app.
    
    Returns:
        dict: Kết quả kiểm tra
    """
    endpoints = [
        '/api/products/top',
        '/api/products/123456789',
        '/api/recommendations/user123',
        '/api/algorithms/comparison/user123',
        '/api/categories',
        '/api/user/behavior/user123'
    ]
    
    results = {}
    
    for endpoint in endpoints:
        url = f"{FLASK_URL}{endpoint}"
        try:
            response = requests.get(url)
            results[endpoint] = {
                'status_code': response.status_code,
                'success': response.status_code == 200,
                'response_time': response.elapsed.total_seconds(),
                'content_length': len(response.content)
            }
            
            if response.status_code == 200:
                results[endpoint]['data'] = response.json()
        except Exception as e:
            results[endpoint] = {
                'status_code': None,
                'success': False,
                'error': str(e)
            }
    
    return results

# Kiểm tra hiệu suất
def test_performance(num_requests=100, concurrency=10):
    """
    Kiểm tra hiệu suất của API.
    
    Args:
        num_requests (int): Số lượng request
        concurrency (int): Số lượng request đồng thời
        
    Returns:
        dict: Kết quả kiểm tra hiệu suất
    """
    endpoints = [
        '/api/products/top',
        '/api/products/123456789',
        '/api/recommendations/user123'
    ]
    
    results = {}
    
    for endpoint in endpoints:
        url = f"{FLASK_URL}{endpoint}"
        response_times = []
        success_count = 0
        error_count = 0
        
        def make_request():
            try:
                start_time = time.time()
                response = requests.get(url)
                end_time = time.time()
                
                return {
                    'success': response.status_code == 200,
                    'response_time': end_time - start_time,
                    'status_code': response.status_code
                }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e)
                }
        
        # Thực hiện các request đồng thời
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            request_results = list(executor.map(lambda _: make_request(), range(num_requests)))
        
        # Phân tích kết quả
        for result in request_results:
            if result.get('success', False):
                success_count += 1
                response_times.append(result.get('response_time', 0))
            else:
                error_count += 1
        
        # Tính toán thống kê
        if response_times:
            avg_response_time = sum(response_times) / len(response_times)
            min_response_time = min(response_times)
            max_response_time = max(response_times)
            p95_response_time = np.percentile(response_times, 95)
            
            results[endpoint] = {
                'success_rate': success_count / num_requests,
                'avg_response_time': avg_response_time,
                'min_response_time': min_response_time,
                'max_response_time': max_response_time,
                'p95_response_time': p95_response_time,
                'requests_per_second': success_count / sum(response_times)
            }
        else:
            results[endpoint] = {
                'success_rate': 0,
                'error': 'Không có request thành công'
            }
    
    return results

# Kiểm tra thuật toán gợi ý
def test_recommendation_algorithms():
    """
    Kiểm tra các thuật toán gợi ý.
    
    Returns:
        dict: Kết quả kiểm tra
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        data = generate_sample_data()
        
        # Tạo DataFrame
        products_df = pd.DataFrame(data['products'])
        reviews_df = pd.DataFrame(data['reviews'])
        users_df = pd.DataFrame(data['users'])
        user_behaviors_df = pd.DataFrame(data['user_behaviors'])
        
        # Tạo ma trận tương tác người dùng-sản phẩm
        user_product_matrix = user_behaviors_df.pivot_table(
            index='user_id',
            columns='product_id',
            values='interaction_score',
            fill_value=0
        )
        
        # Kiểm tra Collaborative Filtering
        try:
            cf = CollaborativeFiltering()
            cf_start_time = time.time()
            cf_recommendations = cf.recommend(user_id='user123', user_product_matrix=user_product_matrix)
            cf_end_time = time.time()
            
            results['collaborative_filtering'] = {
                'success': True,
                'execution_time': cf_end_time - cf_start_time,
                'recommendations': cf_recommendations
            }
        except Exception as e:
            results['collaborative_filtering'] = {
                'success': False,
                'error': str(e)
            }
        
        # Kiểm tra Matrix Factorization
        try:
            mf = MatrixFactorization()
            mf_start_time = time.time()
            mf_recommendations = mf.recommend(user_id='user123', user_product_matrix=user_product_matrix)
            mf_end_time = time.time()
            
            results['matrix_factorization'] = {
                'success': True,
                'execution_time': mf_end_time - mf_start_time,
                'recommendations': mf_recommendations
            }
        except Exception as e:
            results['matrix_factorization'] = {
                'success': False,
                'error': str(e)
            }
        
        # Kiểm tra Hybrid Methods
        try:
            hybrid = HybridRecommender()
            hybrid_start_time = time.time()
            hybrid_recommendations = hybrid.recommend(
                user_id='user123',
                user_product_matrix=user_product_matrix,
                products_df=products_df
            )
            hybrid_end_time = time.time()
            
            results['hybrid_methods'] = {
                'success': True,
                'execution_time': hybrid_end_time - hybrid_start_time,
                'recommendations': hybrid_recommendations
            }
        except Exception as e:
            results['hybrid_methods'] = {
                'success': False,
                'error': str(e)
            }
    except Exception as e:
        results['error'] = str(e)
    
    return results

# Kiểm tra hệ thống truy vấn
def test_query_system():
    """
    Kiểm tra hệ thống truy vấn.
    
    Returns:
        dict: Kết quả kiểm tra
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        data = generate_sample_data()
        
        # Tạo DataFrame
        products_df = pd.DataFrame(data['products'])
        reviews_df = pd.DataFrame(data['reviews'])
        users_df = pd.DataFrame(data['users'])
        user_behaviors_df = pd.DataFrame(data['user_behaviors'])
        
        # Kiểm tra Spark SQL Manager
        try:
            sql_manager = ShopeeSQLManager()
            sql_manager_start_time = time.time()
            top_products = sql_manager.get_top_products(limit=5)
            sql_manager_end_time = time.time()
            
            results['spark_sql_manager'] = {
                'success': True,
                'execution_time': sql_manager_end_time - sql_manager_start_time,
                'top_products': top_products.to_dict('records') if hasattr(top_products, 'to_dict') else None
            }
        except Exception as e:
            results['spark_sql_manager'] = {
                'success': False,
                'error': str(e)
            }
        
        # Kiểm tra Serving Layer
        try:
            serving_layer = ShopeeServingLayer()
            serving_layer_start_time = time.time()
            serving_layer.start()
            
            # Thêm dữ liệu vào batch view
            serving_layer.add_to_batch_view('products', products_df)
            serving_layer.add_to_batch_view('reviews', reviews_df)
            
            # Thêm dữ liệu vào real-time view
            serving_layer.add_to_realtime_view('user_behaviors', user_behaviors_df)
            
            # Truy vấn dữ liệu
            merged_view = serving_layer.get_merged_view('products')
            
            serving_layer_end_time = time.time()
            
            results['serving_layer'] = {
                'success': True,
                'execution_time': serving_layer_end_time - serving_layer_start_time,
                'merged_view': merged_view.to_dict('records') if hasattr(merged_view, 'to_dict') else None
            }
            
            # Dừng serving layer
            serving_layer.stop()
        except Exception as e:
            results['serving_layer'] = {
                'success': False,
                'error': str(e)
            }
    except Exception as e:
        results['error'] = str(e)
    
    return results

# Kiểm tra tích hợp
def test_integration():
    """
    Kiểm tra tích hợp giữa các thành phần.
    
    Returns:
        dict: Kết quả kiểm tra
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        data = generate_sample_data()
        
        # Lưu dữ liệu mẫu
        save_sample_data(data)
        
        # Kiểm tra luồng dữ liệu từ thu thập đến gợi ý
        
        # 1. Thu thập dữ liệu (mô phỏng)
        results['data_collection'] = {
            'success': True,
  
(Content truncated due to size limit. Use line ranges to read in chunks)