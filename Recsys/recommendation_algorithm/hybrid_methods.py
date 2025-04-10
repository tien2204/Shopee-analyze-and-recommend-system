#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hybrid Methods cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này triển khai thuật toán Hybrid Methods kết hợp Content-Based Filtering và Collaborative Filtering.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import csr_matrix, hstack

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_hybrid_methods')

class ShopeeHybridMethods:
    """
    Lớp ShopeeHybridMethods triển khai thuật toán Hybrid Methods.
    """
    
    def __init__(self, input_dir='../data/batch_processed',
                 output_dir='../data/recommendations',
                 cf_weight=0.7, cb_weight=0.3):
        """
        Khởi tạo Hybrid Methods với các tùy chọn cấu hình.
        
        Args:
            input_dir (str): Thư mục đầu vào chứa dữ liệu đã xử lý
            output_dir (str): Thư mục đầu ra cho kết quả gợi ý
            cf_weight (float): Trọng số cho Collaborative Filtering
            cb_weight (float): Trọng số cho Content-Based Filtering
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.cf_weight = cf_weight
        self.cb_weight = cb_weight
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def load_data(self):
        """
        Tải dữ liệu cho Hybrid Methods.
        
        Returns:
            tuple: (products_df, user_behavior_df) hoặc None nếu thất bại
        """
        try:
            # Tải dữ liệu sản phẩm
            products_df = self._load_products()
            
            # Tải dữ liệu hành vi người dùng
            user_behavior_df = self._load_user_behavior()
            
            # Kiểm tra dữ liệu
            if products_df is None or user_behavior_df is None:
                logger.warning("Không thể tải dữ liệu")
                return self._simulate_data()
            
            logger.info(f"Đã tải dữ liệu: {len(products_df)} sản phẩm, {len(user_behavior_df)} hành vi người dùng")
            return (products_df, user_behavior_df)
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu: {e}")
            return self._simulate_data()
    
    def _load_products(self):
        """
        Tải dữ liệu sản phẩm.
        
        Returns:
            DataFrame: Dữ liệu sản phẩm hoặc None nếu thất bại
        """
        try:
            # Tìm file JSON mới nhất trong thư mục products_batch
            products_dir = os.path.join(self.input_dir, "products_batch")
            if os.path.exists(products_dir):
                json_files = [f for f in os.listdir(products_dir) if f.endswith('.json')]
                if json_files:
                    latest_file = sorted(json_files)[-1]
                    products_path = os.path.join(products_dir, latest_file)
                    
                    # Đọc dữ liệu
                    with open(products_path, 'r', encoding='utf-8') as f:
                        products_data = json.load(f)
                    
                    # Chuyển đổi thành DataFrame
                    products_df = pd.DataFrame(products_data)
                    
                    return products_df
            
            logger.warning("Không tìm thấy dữ liệu sản phẩm")
            return None
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu sản phẩm: {e}")
            return None
    
    def _load_user_behavior(self):
        """
        Tải dữ liệu hành vi người dùng.
        
        Returns:
            DataFrame: Dữ liệu hành vi người dùng hoặc None nếu thất bại
        """
        try:
            # Tìm file JSON mới nhất trong thư mục user_behavior_batch
            behavior_dir = os.path.join(self.input_dir, "user_behavior_batch")
            if os.path.exists(behavior_dir):
                json_files = [f for f in os.listdir(behavior_dir) if f.endswith('.json')]
                if json_files:
                    latest_file = sorted(json_files)[-1]
                    behavior_path = os.path.join(behavior_dir, latest_file)
                    
                    # Đọc dữ liệu
                    with open(behavior_path, 'r', encoding='utf-8') as f:
                        behavior_data = json.load(f)
                    
                    # Chuyển đổi thành DataFrame
                    behavior_df = pd.DataFrame(behavior_data)
                    
                    return behavior_df
            
            logger.warning("Không tìm thấy dữ liệu hành vi người dùng")
            return None
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu hành vi người dùng: {e}")
            return None
    
    def _simulate_data(self):
        """
        Mô phỏng dữ liệu cho Hybrid Methods.
        
        Returns:
            tuple: (products_df, user_behavior_df)
        """
        logger.info("Đang mô phỏng dữ liệu cho Hybrid Methods")
        
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
                "brand": "Samsung",
                "description": "Samsung Galaxy S21 5G là chiếc điện thoại Android cao cấp với màn hình Dynamic AMOLED 2X, chip Exynos 2100, camera 64MP và pin 4000mAh."
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
                "brand": "Apple",
                "description": "iPhone 13 Pro Max là chiếc điện thoại cao cấp nhất của Apple với màn hình Super Retina XDR, chip A15 Bionic, camera 12MP và pin dung lượng lớn."
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
                "brand": "Xiaomi",
                "description": "Xiaomi Mi 11 là flagship với màn hình AMOLED 6.81 inch, chip Snapdragon 888, camera 108MP và pin 4600mAh với sạc nhanh 55W."
            },
            {
                "product_id": "456456456",
                "name": "OPPO Find X3 Pro",
                "price": "19.990.000đ",
                "price_numeric": 19990000.0,
                "rating": 4.6,
                "sold": "820",
                "sold_numeric": 820.0,
                "location": "Đà Nẵng",
                "url": "https://shopee.vn/product/456456456",
                "popularity_score": 3.8,
                "category": "Điện thoại & Phụ kiện",
                "brand": "OPPO",
                "description": "OPPO Find X3 Pro là điện thoại cao cấp với màn hình AMOLED 6.7 inch, chip Snapdragon 888, camera 50MP và pin 4500mAh với sạc nhanh 65W."
            },
            {
                "product_id": "789789789",
                "name": "Vivo X60 Pro",
                "price": "18.990.000đ",
                "price_numeric": 18990000.0,
                "rating": 4.5,
                "sold": "720",
                "sold_numeric": 720.0,
                "location": "Hà Nội",
                "url": "https://shopee.vn/product/789789789",
                "popularity_score": 3.2,
                "category": "Điện thoại & Phụ kiện",
                "brand": "Vivo",
                "description": "Vivo X60 Pro là điện thoại cao cấp với màn hình AMOLED 6.56 inch, chip Exynos 1080, camera ZEISS 48MP và pin 4200mAh với sạc nhanh 33W."
            }
        ]
        
        # Tạo dữ liệu hành vi người dùng mô phỏng
        user_behavior_data = [
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
        products_df = pd.DataFrame(products_data)
        user_behavior_df = pd.DataFrame(user_behavior_data)
        
        logger.info(f"Đã mô phỏng dữ liệu: {len(products_df)} sản phẩm, {len(user_behavior_df)} hành vi người dùng")
        return (products_df, user_behavior_df)
    
    def build_content_based_model(self, products_df):
        """
        Xây dựng mô hình Content-Based Filtering.
        
        Args:
            products_df: DataFrame chứa thông tin sản phẩm
            
        Returns:
            tuple: (tfidf_matrix, tfidf_vectorizer, product_indices)
        """
        try:
            # Tạo chuỗi đặc trưng cho mỗi sản phẩm
            products_df['features'] = ''
            
            # Thêm tên sản phẩm
            if 'name' in products_df.columns:
                products_df['features'] += products_df['name'].fillna('') + ' '
            
            # Thêm thương hiệu
            if 'brand' in products_df.columns:
                products_df['features'] += products_df['brand'].fillna('') + ' '
            
            # Thêm danh mục
            if 'category' in products_df.columns:
                products_df['features'] += products_df['category'].fillna('') + ' '
            
            # Thêm mô tả
            if 'description' in products_df.columns:
                products_df['features'] += products_df['description'].fillna('')
            
            # Nếu không có các cột trên, sử dụng tên sản phẩm
            if products_df['features'].str.strip().eq('').any():
                products_df.loc[products_df['features'].str.strip().eq(''), 'features'] = products_df['name']
            
            # Tạo TF-IDF Vectorizer
            tfidf_vectorizer = TfidfVectorizer(
                stop_words='english',
                max_features=5000,
                ngram_range=(1, 2)
            )
            
            # Tạo ma trận TF-IDF
            tfidf_matrix = tfidf_vectorizer.fit_transform(products_df['features'])
            
            # Tạo ánh xạ từ product_id sang chỉ số
            product_indices = pd.Series(products_df.index, index=products_df['product_id']).to_dict()
            
            logger.info(f"Đã xây dựng mô hình Content-Based với {tfidf_matrix.shape[1]} đặc trưng")
            return (tfidf_matrix, tfidf_vectorizer, product_indices)
            
        except Exception as e:
            logger.error(f"Lỗi khi xây dựng mô hình Content-Based: {e}")
            return None
    
    def build_collaborative_filtering_model(self, user_behavior_df):
        """
        Xây dựng mô hình Collaborative Filtering.
        
        Args:
            user_behavior_df: DataFrame chứa thông tin hành vi người dùng
            
        Returns:
            tuple: (user_product_matrix, user_indices, product_indices)
        """
        try:
            # Tạo ánh xạ từ user_id sang chỉ số
            user_ids = user_behavior_df['user_id'].unique()
            user_id_to_idx = {user_id: i for i, user_id in enumerate(user_ids)}
            
            # Tạo ánh xạ từ product_id sang chỉ số
            product_ids = user_behavior_df['product_id'].unique()
            product_id_to_idx = {product_id: i for i, product_id in enumerate(product_ids)}
            
            # Tạo ma trận thưa
            rows = []
            cols = []
            data = []
            
            for _, row in user_behavior_df.iterrows():
                user_idx = user_id_to_idx[row['user_id']]
                product_idx = product_id_to_idx[row['product_id']]
                interaction_score = row['interaction_score']
                
                rows.append(user_idx)
                cols.append(product_idx)
                data.append(interaction_score)
            
            # Tạo ma trận CSR
            user_product_matrix = csr_matrix((data, (rows, cols)), shape=(len(user_ids), len(product_ids)))
            
            # Tạo ánh xạ ngược
            user_indices = {i: user_id for user_id, i in user_id_to_idx.items()}
            product_indices = {i: product_id for product_id, i in product_id_to_idx.items()}
            
            logger.info(f"Đã xây dựng mô hình Collaborative Filtering với {len(user_ids)} người dùng và {len(product_ids)} sản phẩm")
            return (user_product_matrix, user_indices, product_indices)
            
        except Exception as e:
            logger.error(f"Lỗi khi xây dựng mô hình Collaborative Filtering: {e}")
            return None
    
    def generate_content_based_recommendations(self, product_id, tfidf_matrix, product_indices, products_df, n_recommendations=10):
        """
        Tạo gợi ý sản phẩm dựa trên Content-Based Filtering.
        
        Args:
            product_id: ID của sản phẩm
            tfidf_matrix: Ma trận TF-IDF
            product_indices: Ánh xạ từ product_id sang chỉ số
            products_df: DataFrame chứa thông tin sản phẩm
            n_recommendations (int): Số lượng gợi ý
            
        Returns:
            list: Danh sách ID sản phẩm được gợi ý
        """
        try:
            # Kiểm tra product_id có trong ánh xạ không
            if product_id not in product_indices:
                logger.warning(f"Product ID {product_id} không tồn tại trong dữ liệu")
                return []
            
            # Lấy chỉ số của sản phẩm
            idx = product_indices[product_id]
            
            # Tính toán cosine similarity
            sim_scores = cosine_similarity(tfidf_matrix[idx:idx+1], tfidf_matrix).flatten()
            
            # Lấy chỉ số của các sản phẩm tương tự (trừ chính nó)
            sim_indices = sim_scores.argsort()[::-1][1:n_recommendations+1]
            
            # Lấy ID của các sản phẩm tương tự
            similar_products = []
            for i in sim_indices:
                product_id = products_df.iloc[i]['product_id']
                similarity = sim_scores[i]
                similar_products.append((product_id, similarity))
            
            return similar_products
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý Content-Based: {e}")
            return []
    
    def generate_collaborative_filtering_recommendations(self, user_id, user_product_matrix, user_id_to_idx, product_indices, n_recommendations=10):
        """
        Tạo gợi ý sản phẩm dựa trên Collaborative Filtering.
        
        Args:
            user_id: ID của người dùng
            user_product_matrix: Ma trận user-product
            user_id_to_idx: Ánh xạ từ user_id sang chỉ số
            product_indices: Ánh xạ từ chỉ số sang product_id
            n_recommendations (int): Số lượng gợi ý
            
        Returns:
            list: Danh sách ID sản phẩm được gợi ý
        """
        try:
            # Kiểm tra user_id có trong ánh xạ không
            if user_id not in user_id_to_idx:
                logger.warning(f"User ID {user_id} không tồn tại trong dữ liệu")
                return []
            
            # Lấy chỉ số của người dùng
            user_idx = user_id_to_idx[user_id]
            
            # Lấy vector tương tác của người dùng
            user_interactions = user_product_matrix[user_idx].toarray().flatten()
            
            # Tính toán similarity giữa người dùng này và tất cả người dùng khác
            user_similarity = cosine_similarity(user_product_matrix[user_idx], user_product_matrix).flatten()
            
            # Loại bỏ chính người dùng đó
            user_similarity[user_idx] = 0
            
            # Lấy top k người dùng tương tự nhất
            k = 10
            similar_users_indices = user_similarity.argsort()[::-1][:k]
            similar_users_scores = user_similarity[similar_users_indices]
            
            # Tính toán điểm số dự đoán cho các sản phẩm
            predicted_ratings = np.zeros(user_product_matrix.shape[1])
            
            for i, idx in enumerate(similar_users_indices):
                if similar_users_scores[i] > 0:
                    predicted_ratings += similar_users_scores[i] * user_product_matrix[idx].toarray().flatten()
            
            # Loại bỏ các sản phẩm đã tương tác
            predicted_ratings[user_interactions > 0] = 0
            
            # Lấy top n sản phẩm có điểm cao nhất
            top_product_indices = predicted_ratings.argsort()[::-1][:n_recommendations]
            
            # Lấy ID của các sản phẩm được gợi ý
            recommended_products = []
            for idx in top_product_indices:
                if predicted_ratings[idx] > 0:
                    product_id = product_indices[idx]
                    score = predicted_ratings[idx]
                    recommended_products.append((product_id, score))
            
            return recommended_products
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý Collaborative Filtering: {e}")
            return []
    
    def generate_hybrid_recommendations(self, user_id, products_df, user_behavior_df, n_recommendations=10):
        """
        Tạo gợi ý sản phẩm dựa trên Hybrid Methods.
        
        Args:
            user_id: ID của người dùng
            products_df: DataFrame chứa thông tin sản phẩm
            user_behavior_df: DataFrame chứa thông tin hành vi người dùng
            n_recommendations (int): Số lượng gợi ý
            
        Returns:
            list: Danh sách gợi ý sản phẩm
        """
        try:
            # Kiểm tra user_id có trong dữ liệu không
            if user_id not in user_behavior_df['user_id'].values:
                logger.warning(f"User ID {user_id} không tồn tại trong dữ liệu")
                return self._simulate_hybrid_recommendations(user_id, n_recommendations)
            
            # Xây dựng mô hình Content-Based
            content_based_model = self.build_content_based_model(products_df)
            if not content_based_model:
                return self._simulate_hybrid_recommendations(user_id, n_recommendations)
            
            tfidf_matrix, _, product_indices_cb = content_based_model
            
            # Xây dựng mô hình Collaborative Filtering
            collaborative_filtering_model = self.build_collaborative_filtering_model(user_behavior_df)
            if not collaborative_filtering_model:
                return self._simulate_hybrid_recommendations(user_id, n_recommendations)
            
            user_product_matrix, user_indices, product_indices_cf = collaborative_filtering_model
            
            # Tạo ánh xạ từ user_id sang chỉ số
            user_id_to_idx = {user_id: i for i, user_id in user_indices.items()}
            
            # Lấy các sản phẩm đã tương tác của người dùng
            user_products = user_behavior_df[user_behavior_df['user_id'] == user_id]['product_id'].values
            
            # Tạo gợi ý Content-Based cho mỗi sản phẩm đã tương tác
            cb_recommendations = {}
            for product_id in user_products:
                similar_products = self.generate_content_based_recommendations(
                    product_id, tfidf_matrix, product_indices_cb, products_df, n_recommendations
                )
                
                for rec_product_id, similarity in similar_products:
                    if rec_product_id not in cb_recommendations:
                        cb_recommendations[rec_product_id] = similarity
                    else:
                        cb_recommendations[rec_product_id] = max(cb_recommendations[rec_product_id], similarity)
            
            # Tạo gợi ý Collaborative Filtering
            cf_recommendations = {}
            cf_recs = self.generate_collaborative_filtering_recommendations(
                user_id, user_product_matrix, user_id_to_idx, product_indices_cf, n_recommendations
            )
            
            for product_id, score in cf_recs:
                cf_recommendations[product_id] = score
            
            # Kết hợp các gợi ý
            hybrid_scores = {}
            
            # Chuẩn hóa điểm Content-Based
            if cb_recommendations:
                max_cb_score = max(cb_recommendations.values())
                for product_id, score in cb_recommendations.items():
                    cb_recommendations[product_id] = score / max_cb_score if max_cb_score > 0 else 0
            
            # Chuẩn hóa điểm Collaborative Filtering
            if cf_recommendations:
                max_cf_score = max(cf_recommendations.values())
                for product_id, score in cf_recommendations.items():
                    cf_recommendations[product_id] = score / max_cf_score if max_cf_score > 0 else 0
            
            # Tính điểm kết hợp
            all_product_ids = set(cb_recommendations.keys()) | set(cf_recommendations.keys())
            
            for product_id in all_product_ids:
                cb_score = cb_recommendations.get(product_id, 0)
                cf_score = cf_recommendations.get(product_id, 0)
                
                # Tính điểm kết hợp theo trọng số
                hybrid_score = self.cf_weight * cf_score + self.cb_weight * cb_score
                
                hybrid_scores[product_id] = hybrid_score
            
            # Loại bỏ các sản phẩm đã tương tác
            for product_id in user_products:
                if product_id in hybrid_scores:
                    del hybrid_scores[product_id]
            
            # Sắp xếp theo điểm giảm dần
            sorted_recommendations = sorted(hybrid_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Lấy top n gợi ý
            top_recommendations = sorted_recommendations[:n_recommendations]
            
            # Tạo danh sách gợi ý với thông tin đầy đủ
            recommendations = []
            
            for product_id, hybrid_score in top_recommendations:
                # Lấy thông tin sản phẩm
                product_info = products_df[products_df['product_id'] == product_id]
                
                if not product_info.empty:
                    product_info = product_info.iloc[0]
                    
                    # Tạo đối tượng gợi ý
                    recommendation = {
                        'user_id': user_id,
                        'product_id': product_id,
                        'name': product_info.get('name', f"Product {product_id}"),
                        'price': product_info.get('price', "Unknown"),
                        'rating': product_info.get('rating', 0.0),
                        'cb_score': cb_recommendations.get(product_id, 0),
                        'cf_score': cf_recommendations.get(product_id, 0),
                        'hybrid_score': hybrid_score,
                        'popularity_score': product_info.get('popularity_score', 1.0),
                        'recommendation_score': hybrid_score * product_info.get('popularity_score', 1.0),
                        'processed_date': datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    recommendations.append(recommendation)
            
            # Sắp xếp lại theo recommendation_score
            recommendations.sort(key=lambda x: x['recommendation_score'], reverse=True)
            
            logger.info(f"Đã tạo {len(recommendations)} gợi ý hybrid cho người dùng {user_id}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý Hybrid: {e}")
            return self._simulate_hybrid_recommendations(user_id, n_recommendations)
    
    def _simulate_hybrid_recommendations(self, user_id, n_recommendations=10):
        """
        Mô phỏng tạo gợi ý sản phẩm dựa trên Hybrid Methods.
        
        Args:
            user_id: ID của người dùng
            n_recommendations (int): Số lượng gợi ý
            
        Returns:
            list: Danh sách gợi ý sản phẩm mô phỏng
        """
        logger.info(f"Đang mô phỏng tạo gợi ý hybrid cho người dùng {user_id}")
        
        # Thông tin sản phẩm mô phỏng
        sample_products = {
            '123456789': {
                'name': "Samsung Galaxy S21",
                'price': "15.990.000đ",
                'rating': 4.8,
                'popularity_score': 5.76
            },
            '987654321': {
                'name': "iPhone 13 Pro Max",
                'price': "29.990.000đ",
                'rating': 4.9,
                'popularity_score': 12.25
            },
            '123123123': {
                'name': "Xiaomi Mi 11",
                'price': "12.490.000đ",
                'rating': 4.7,
                'popularity_score': 4.5
            },
            '456456456': {
                'name': "OPPO Find X3 Pro",
                'price': "19.990.000đ",
                'rating': 4.6,
                'popularity_score': 3.8
            },
            '789789789': {
                'name': "Vivo X60 Pro",
                'price': "18.990.000đ",
                'rating': 4.5,
                'popularity_score': 3.2
            }
        }
        
        # Tạo gợi ý mô phỏng
        import random
        recommendations = []
        
        # Chọn ngẫu nhiên n sản phẩm
        product_ids = list(sample_products.keys())
        selected_products = random.sample(product_ids, min(n_recommendations, len(product_ids)))
        
        for product_id in selected_products:
            # Tạo điểm số ngẫu nhiên
            cb_score = random.uniform(0.5, 1.0)
            cf_score = random.uniform(0.5, 1.0)
            
            # Tính điểm kết hợp
            hybrid_score = self.cf_weight * cf_score + self.cb_weight * cb_score
            
            # Lấy thông tin sản phẩm
            product_info = sample_products[product_id]
            
            # Tính điểm gợi ý kết hợp
            recommendation_score = hybrid_score * product_info['popularity_score']
            
            # Tạo đối tượng gợi ý
            recommendation = {
                'user_id': user_id,
                'product_id': product_id,
                'name': product_info['name'],
                'price': product_info['price'],
                'rating': product_info['rating'],
                'cb_score': cb_score,
                'cf_score': cf_score,
                'hybrid_score': hybrid_score,
                'popularity_score': product_info['popularity_score'],
                'recommendation_score': recommendation_score,
                'processed_date': datetime.now().strftime("%Y-%m-%d")
            }
            
            recommendations.append(recommendation)
        
        # Sắp xếp theo recommendation_score
        recommendations.sort(key=lambda x: x['recommendation_score'], reverse=True)
        
        logger.info(f"Đã mô phỏng tạo {len(recommendations)} gợi ý hybrid cho người dùng {user_id}")
        return recommendations
    
    def generate_recommendations_for_all_users(self, products_df, user_behavior_df, n_recommendations=10):
        """
        Tạo gợi ý sản phẩm cho tất cả người dùng.
        
        Args:
            products_df: DataFrame chứa thông tin sản phẩm
            user_behavior_df: DataFrame chứa thông tin hành vi người dùng
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            list: Danh sách gợi ý sản phẩm cho tất cả người dùng
        """
        try:
            # Lấy danh sách người dùng
            user_ids = user_behavior_df['user_id'].unique()
            
            # Tạo gợi ý cho từng người dùng
            all_recommendations = []
            
            for user_id in user_ids:
                user_recommendations = self.generate_hybrid_recommendations(
                    user_id, products_df, user_behavior_df, n_recommendations
                )
                
                all_recommendations.extend(user_recommendations)
            
            logger.info(f"Đã tạo tổng cộng {len(all_recommendations)} gợi ý hybrid cho {len(user_ids)} người dùng")
            return all_recommendations
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý cho tất cả người dùng: {e}")
            return []
    
    def save_recommendations(self, recommendations, output_path=None):
        """
        Lưu gợi ý sản phẩm.
        
        Args:
            recommendations: Danh sách gợi ý sản phẩm
            output_path (str, optional): Đường dẫn đầu ra
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        try:
            # Xác định đường dẫn đầu ra
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = os.path.join(self.output_dir, f"hybrid_recommendations_{timestamp}.json")
            
            # Lưu gợi ý
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(recommendations, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã lưu gợi ý sản phẩm vào {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu gợi ý sản phẩm: {e}")
            return False
    
    def run_hybrid_methods(self, n_recommendations=10):
        """
        Chạy toàn bộ quy trình Hybrid Methods.
        
        Args:
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Tải dữ liệu
            data = self.load_data()
            if not data:
                return False
            
            products_df, user_behavior_df = data
            
            # Tạo gợi ý cho tất cả người dùng
            recommendations = self.generate_recommendations_for_all_users(
                products_df, user_behavior_df, n_recommendations
            )
            
            if not recommendations:
                return False
            
            # Lưu gợi ý
            success = self.save_recommendations(recommendations)
            
            return success
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy Hybrid Methods: {e}")
            return False


if __name__ == "__main__":
    # Ví dụ sử dụng
    hybrid = ShopeeHybridMethods()
    
    # Chạy Hybrid Methods
    hybrid.run_hybrid_methods(10)
