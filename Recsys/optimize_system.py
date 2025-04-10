#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tối ưu hóa hệ thống gợi ý sản phẩm Shopee

Mô-đun này thực hiện các tối ưu hóa cho hệ thống gợi ý sản phẩm Shopee.
"""

import os
import sys
import time
import json
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, cpu_count

# Thêm thư mục cha vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import các module từ hệ thống
try:
    from recommendation_algorithms.collaborative_filtering import CollaborativeFiltering
    from recommendation_algorithms.matrix_factorization import MatrixFactorization
    from recommendation_algorithms.hybrid_methods import HybridRecommender
    from query_system.spark_sql_manager import ShopeeSQLManager
    from query_system.serving_layer import ShopeeServingLayer
except ImportError as e:
    print(f"Không thể import các module từ hệ thống: {e}")

# Tối ưu hóa thuật toán Collaborative Filtering
def optimize_collaborative_filtering():
    """
    Tối ưu hóa thuật toán Collaborative Filtering.
    
    Returns:
        dict: Kết quả tối ưu hóa
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        user_ids = [f'user{i}' for i in range(1, 101)]
        product_ids = [f'product{i}' for i in range(1, 201)]
        
        # Tạo ma trận tương tác người dùng-sản phẩm thưa thớt
        interactions = []
        for user_id in user_ids:
            # Mỗi người dùng tương tác với khoảng 10% sản phẩm
            num_interactions = np.random.randint(10, 30)
            selected_products = np.random.choice(product_ids, num_interactions, replace=False)
            
            for product_id in selected_products:
                interaction_score = np.random.uniform(1, 5)
                interactions.append({
                    'user_id': user_id,
                    'product_id': product_id,
                    'interaction_score': interaction_score
                })
        
        interactions_df = pd.DataFrame(interactions)
        
        # Tạo ma trận tương tác
        user_product_matrix = interactions_df.pivot_table(
            index='user_id',
            columns='product_id',
            values='interaction_score',
            fill_value=0
        )
        
        # Tạo đối tượng Collaborative Filtering
        cf = CollaborativeFiltering()
        
        # Thử nghiệm các tham số khác nhau
        param_combinations = [
            {'k': 5, 'sim_threshold': 0.2},
            {'k': 10, 'sim_threshold': 0.2},
            {'k': 20, 'sim_threshold': 0.2},
            {'k': 5, 'sim_threshold': 0.3},
            {'k': 10, 'sim_threshold': 0.3},
            {'k': 20, 'sim_threshold': 0.3},
            {'k': 5, 'sim_threshold': 0.4},
            {'k': 10, 'sim_threshold': 0.4},
            {'k': 20, 'sim_threshold': 0.4}
        ]
        
        param_results = []
        
        for params in param_combinations:
            # Đặt tham số
            cf.k = params['k']
            cf.sim_threshold = params['sim_threshold']
            
            # Đo thời gian thực thi
            start_time = time.time()
            
            # Tính toán ma trận tương đồng
            cf.compute_similarity_matrix(user_product_matrix)
            
            # Tạo gợi ý cho một số người dùng
            test_users = np.random.choice(user_ids, 10, replace=False)
            recommendations = {}
            
            for user_id in test_users:
                user_recommendations = cf.recommend(user_id, user_product_matrix)
                recommendations[user_id] = user_recommendations
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Đánh giá kết quả
            avg_num_recommendations = np.mean([len(recs) for recs in recommendations.values()])
            
            param_results.append({
                'params': params,
                'execution_time': execution_time,
                'avg_num_recommendations': avg_num_recommendations
            })
        
        # Tìm tham số tốt nhất
        best_params = min(param_results, key=lambda x: x['execution_time'])
        
        results = {
            'success': True,
            'best_params': best_params['params'],
            'best_execution_time': best_params['execution_time'],
            'all_results': param_results
        }
    except Exception as e:
        results = {
            'success': False,
            'error': str(e)
        }
    
    return results

# Tối ưu hóa thuật toán Matrix Factorization
def optimize_matrix_factorization():
    """
    Tối ưu hóa thuật toán Matrix Factorization.
    
    Returns:
        dict: Kết quả tối ưu hóa
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        user_ids = [f'user{i}' for i in range(1, 101)]
        product_ids = [f'product{i}' for i in range(1, 201)]
        
        # Tạo ma trận tương tác người dùng-sản phẩm thưa thớt
        interactions = []
        for user_id in user_ids:
            # Mỗi người dùng tương tác với khoảng 10% sản phẩm
            num_interactions = np.random.randint(10, 30)
            selected_products = np.random.choice(product_ids, num_interactions, replace=False)
            
            for product_id in selected_products:
                interaction_score = np.random.uniform(1, 5)
                interactions.append({
                    'user_id': user_id,
                    'product_id': product_id,
                    'interaction_score': interaction_score
                })
        
        interactions_df = pd.DataFrame(interactions)
        
        # Tạo ma trận tương tác
        user_product_matrix = interactions_df.pivot_table(
            index='user_id',
            columns='product_id',
            values='interaction_score',
            fill_value=0
        )
        
        # Tạo đối tượng Matrix Factorization
        mf = MatrixFactorization()
        
        # Thử nghiệm các tham số khác nhau
        param_combinations = [
            {'n_factors': 10, 'n_epochs': 20, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 20, 'n_epochs': 20, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 30, 'n_epochs': 20, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 10, 'n_epochs': 50, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 20, 'n_epochs': 50, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 30, 'n_epochs': 50, 'learning_rate': 0.01, 'regularization': 0.1},
            {'n_factors': 10, 'n_epochs': 20, 'learning_rate': 0.005, 'regularization': 0.1},
            {'n_factors': 20, 'n_epochs': 20, 'learning_rate': 0.005, 'regularization': 0.1},
            {'n_factors': 30, 'n_epochs': 20, 'learning_rate': 0.005, 'regularization': 0.1}
        ]
        
        param_results = []
        
        for params in param_combinations:
            # Đặt tham số
            mf.n_factors = params['n_factors']
            mf.n_epochs = params['n_epochs']
            mf.learning_rate = params['learning_rate']
            mf.regularization = params['regularization']
            
            # Đo thời gian thực thi
            start_time = time.time()
            
            # Huấn luyện mô hình
            mf.train(user_product_matrix)
            
            # Tạo gợi ý cho một số người dùng
            test_users = np.random.choice(user_ids, 10, replace=False)
            recommendations = {}
            
            for user_id in test_users:
                user_recommendations = mf.recommend(user_id, user_product_matrix)
                recommendations[user_id] = user_recommendations
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Đánh giá kết quả
            avg_num_recommendations = np.mean([len(recs) for recs in recommendations.values()])
            
            param_results.append({
                'params': params,
                'execution_time': execution_time,
                'avg_num_recommendations': avg_num_recommendations
            })
        
        # Tìm tham số tốt nhất
        best_params = min(param_results, key=lambda x: x['execution_time'])
        
        results = {
            'success': True,
            'best_params': best_params['params'],
            'best_execution_time': best_params['execution_time'],
            'all_results': param_results
        }
    except Exception as e:
        results = {
            'success': False,
            'error': str(e)
        }
    
    return results

# Tối ưu hóa thuật toán Hybrid Methods
def optimize_hybrid_methods():
    """
    Tối ưu hóa thuật toán Hybrid Methods.
    
    Returns:
        dict: Kết quả tối ưu hóa
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        user_ids = [f'user{i}' for i in range(1, 101)]
        product_ids = [f'product{i}' for i in range(1, 201)]
        
        # Tạo ma trận tương tác người dùng-sản phẩm thưa thớt
        interactions = []
        for user_id in user_ids:
            # Mỗi người dùng tương tác với khoảng 10% sản phẩm
            num_interactions = np.random.randint(10, 30)
            selected_products = np.random.choice(product_ids, num_interactions, replace=False)
            
            for product_id in selected_products:
                interaction_score = np.random.uniform(1, 5)
                interactions.append({
                    'user_id': user_id,
                    'product_id': product_id,
                    'interaction_score': interaction_score
                })
        
        interactions_df = pd.DataFrame(interactions)
        
        # Tạo ma trận tương tác
        user_product_matrix = interactions_df.pivot_table(
            index='user_id',
            columns='product_id',
            values='interaction_score',
            fill_value=0
        )
        
        # Tạo dữ liệu sản phẩm
        products = []
        for product_id in product_ids:
            category = np.random.choice(['Điện thoại & Phụ kiện', 'Máy tính & Laptop', 'Thiết bị âm thanh'])
            brand = np.random.choice(['Apple', 'Samsung', 'Xiaomi', 'Dell', 'Sony'])
            price = np.random.uniform(1000000, 30000000)
            
            products.append({
                'product_id': product_id,
                'name': f'Sản phẩm {product_id}',
                'category': category,
                'brand': brand,
                'price_numeric': price,
                'description': f'Mô tả sản phẩm {product_id}'
            })
        
        products_df = pd.DataFrame(products)
        
        # Tạo đối tượng Hybrid Recommender
        hybrid = HybridRecommender()
        
        # Thử nghiệm các tham số khác nhau
        param_combinations = [
            {'cf_weight': 0.3, 'mf_weight': 0.3, 'cb_weight': 0.4},
            {'cf_weight': 0.4, 'mf_weight': 0.3, 'cb_weight': 0.3},
            {'cf_weight': 0.3, 'mf_weight': 0.4, 'cb_weight': 0.3},
            {'cf_weight': 0.5, 'mf_weight': 0.25, 'cb_weight': 0.25},
            {'cf_weight': 0.25, 'mf_weight': 0.5, 'cb_weight': 0.25},
            {'cf_weight': 0.25, 'mf_weight': 0.25, 'cb_weight': 0.5},
            {'cf_weight': 0.6, 'mf_weight': 0.2, 'cb_weight': 0.2},
            {'cf_weight': 0.2, 'mf_weight': 0.6, 'cb_weight': 0.2},
            {'cf_weight': 0.2, 'mf_weight': 0.2, 'cb_weight': 0.6}
        ]
        
        param_results = []
        
        for params in param_combinations:
            # Đặt tham số
            hybrid.cf_weight = params['cf_weight']
            hybrid.mf_weight = params['mf_weight']
            hybrid.cb_weight = params['cb_weight']
            
            # Đo thời gian thực thi
            start_time = time.time()
            
            # Tạo gợi ý cho một số người dùng
            test_users = np.random.choice(user_ids, 10, replace=False)
            recommendations = {}
            
            for user_id in test_users:
                user_recommendations = hybrid.recommend(user_id, user_product_matrix, products_df)
                recommendations[user_id] = user_recommendations
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Đánh giá kết quả
            avg_num_recommendations = np.mean([len(recs) for recs in recommendations.values()])
            
            param_results.append({
                'params': params,
                'execution_time': execution_time,
                'avg_num_recommendations': avg_num_recommendations
            })
        
        # Tìm tham số tốt nhất
        best_params = min(param_results, key=lambda x: x['execution_time'])
        
        results = {
            'success': True,
            'best_params': best_params['params'],
            'best_execution_time': best_params['execution_time'],
            'all_results': param_results
        }
    except Exception as e:
        results = {
            'success': False,
            'error': str(e)
        }
    
    return results

# Tối ưu hóa Serving Layer
def optimize_serving_layer():
    """
    Tối ưu hóa Serving Layer.
    
    Returns:
        dict: Kết quả tối ưu hóa
    """
    results = {}
    
    try:
        # Tạo dữ liệu mẫu
        num_products = 1000
        num_reviews = 5000
        num_user_behaviors = 10000
        
        # Tạo dữ liệu sản phẩm
        products = []
        for i in range(num_products):
            product_id = f'product{i}'
            category = np.random.choice(['Điện thoại & Phụ kiện', 'Máy tính & Laptop', 'Thiết bị âm thanh'])
            brand = np.random.choice(['Apple', 'Samsung', 'Xiaomi', 'Dell', 'Sony'])
            price = np.random.uniform(1000000, 30000000)
            
            products.append({
                'product_id': product_id,
                'name': f'Sản phẩm {product_id}',
                'category': category,
                'brand': brand,
                'price_numeric': price,
                'rating': np.random.uniform(3, 5),
                'sold': np.random.randint(100, 5000),
                'popularity_score': np.random.uniform(1, 10)
            })
        
        products_df = pd.DataFrame(products)
        
        # Tạo dữ liệu đánh giá
        reviews = []
        for i in range(num_reviews):
            product_id = f'product{np.random.randint(0, num_products)}'
            user_id = f'user{np.random.randint(0, 100)}'
            
            reviews.append({
                'review_id': f'review{i}',
                'product_id': product_id,
                'user_id': user_id,
                'rating': np.random.randint(1, 6),
                'comment': f'Đánh giá {i}',
                'sentiment_score': np.random.uniform(-1, 1)
            })
        
        reviews_df = pd.DataFrame(reviews)
        
        # Tạo dữ liệu hành vi người dùng
        user_behaviors = []
        for i in range(num_user_behaviors):
            product_id = f'product{np.random.randint(0, num_products)}'
            user_id = f'user{np.random.randint(0, 100)}'
            
            user_behaviors.append({
                'behavior_id': f'behavior{i}',
                'user_id': user_id,
                'product_id': product_id,
                'action_type': np.random.choice(['view', 'add_to_cart', 'purchase']),
                'action_frequency': np.random.randint(1, 10),
                'interaction_score': np.random.uniform(1, 10)
            })
        
        user_behaviors_df = pd.DataFrame(user_behaviors)
        
        # Tạo đối tượng Serving Layer
        serving_layer = ShopeeServingLayer()
        
        # Thử nghiệm các cấu hình khác nhau
        config_combinations = [
            {'batch_size': 100, 'cache_size': 100, 'num_workers': 1}
(Content truncated due to size limit. Use line ranges to read in chunks)