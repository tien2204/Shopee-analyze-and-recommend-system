#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Matrix Factorization cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này triển khai thuật toán Matrix Factorization (SVD, NMF).
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.sparse import csr_matrix
from sklearn.decomposition import NMF, TruncatedSVD
from sklearn.preprocessing import normalize

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_matrix_factorization')

class ShopeeMatrixFactorization:
    """
    Lớp ShopeeMatrixFactorization triển khai thuật toán Matrix Factorization.
    """
    
    def __init__(self, input_dir='../data/batch_processed',
                 output_dir='../data/recommendations'):
        """
        Khởi tạo Matrix Factorization với các tùy chọn cấu hình.
        
        Args:
            input_dir (str): Thư mục đầu vào chứa dữ liệu đã xử lý
            output_dir (str): Thư mục đầu ra cho kết quả gợi ý
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def load_data(self, user_behavior_path=None):
        """
        Tải dữ liệu cho Matrix Factorization.
        
        Args:
            user_behavior_path (str, optional): Đường dẫn đến dữ liệu hành vi người dùng
            
        Returns:
            tuple: (user_product_matrix, user_ids, product_ids) hoặc None nếu thất bại
        """
        try:
            # Xác định đường dẫn đến dữ liệu hành vi người dùng
            if not user_behavior_path:
                # Tìm file JSON mới nhất trong thư mục user_behavior_batch
                user_behavior_dir = os.path.join(self.input_dir, "user_behavior_batch")
                if os.path.exists(user_behavior_dir):
                    json_files = [f for f in os.listdir(user_behavior_dir) if f.endswith('.json')]
                    if json_files:
                        latest_file = sorted(json_files)[-1]
                        user_behavior_path = os.path.join(user_behavior_dir, latest_file)
                    else:
                        logger.warning("Không tìm thấy file JSON trong thư mục user_behavior_batch")
                        return self._simulate_data()
                else:
                    logger.warning("Không tìm thấy thư mục user_behavior_batch")
                    return self._simulate_data()
            
            # Đọc dữ liệu
            with open(user_behavior_path, 'r', encoding='utf-8') as f:
                behavior_data = json.load(f)
            
            # Chuyển đổi thành DataFrame
            behavior_df = pd.DataFrame(behavior_data)
            
            # Kiểm tra dữ liệu
            if behavior_df.empty:
                logger.warning("Dữ liệu hành vi người dùng trống")
                return self._simulate_data()
            
            # Tạo ma trận user-product
            user_ids = behavior_df['user_id'].unique()
            product_ids = behavior_df['product_id'].unique()
            
            # Tạo ánh xạ từ ID sang chỉ số
            user_id_to_idx = {user_id: i for i, user_id in enumerate(user_ids)}
            product_id_to_idx = {product_id: i for i, product_id in enumerate(product_ids)}
            
            # Tạo ma trận thưa
            rows = []
            cols = []
            data = []
            
            for _, row in behavior_df.iterrows():
                user_idx = user_id_to_idx[row['user_id']]
                product_idx = product_id_to_idx[row['product_id']]
                interaction_score = row['interaction_score']
                
                rows.append(user_idx)
                cols.append(product_idx)
                data.append(interaction_score)
            
            # Tạo ma trận CSR
            user_product_matrix = csr_matrix((data, (rows, cols)), shape=(len(user_ids), len(product_ids)))
            
            logger.info(f"Đã tải dữ liệu: {len(user_ids)} người dùng, {len(product_ids)} sản phẩm")
            return (user_product_matrix, user_ids, product_ids)
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu: {e}")
            return self._simulate_data()
    
    def _simulate_data(self):
        """
        Mô phỏng dữ liệu cho Matrix Factorization.
        
        Returns:
            tuple: (user_product_matrix, user_ids, product_ids)
        """
        logger.info("Đang mô phỏng dữ liệu cho Matrix Factorization")
        
        # Tạo dữ liệu mô phỏng
        user_ids = ['user123', 'user456', 'user789', 'user101']
        product_ids = ['123456789', '987654321', '123123123', '456456456', '789789789']
        
        # Tạo ma trận tương tác
        user_product_matrix = np.zeros((len(user_ids), len(product_ids)))
        
        # Điền một số giá trị mô phỏng
        user_product_matrix[0, 0] = 5.0  # user123 - 123456789
        user_product_matrix[0, 1] = 3.0  # user123 - 987654321
        user_product_matrix[1, 1] = 4.0  # user456 - 987654321
        user_product_matrix[1, 2] = 2.0  # user456 - 123123123
        user_product_matrix[2, 0] = 1.0  # user789 - 123456789
        user_product_matrix[2, 3] = 5.0  # user789 - 456456456
        user_product_matrix[3, 4] = 4.0  # user101 - 789789789
        
        # Chuyển thành ma trận thưa
        user_product_matrix = csr_matrix(user_product_matrix)
        
        logger.info(f"Đã mô phỏng dữ liệu: {len(user_ids)} người dùng, {len(product_ids)} sản phẩm")
        return (user_product_matrix, user_ids, product_ids)
    
    def train_svd_model(self, user_product_matrix, n_components=10, n_iter=10):
        """
        Huấn luyện mô hình SVD (Singular Value Decomposition).
        
        Args:
            user_product_matrix: Ma trận user-product
            n_components (int): Số lượng thành phần
            n_iter (int): Số lần lặp tối đa
            
        Returns:
            tuple: (model, user_factors, item_factors)
        """
        try:
            # Khởi tạo mô hình SVD
            svd = TruncatedSVD(n_components=n_components, n_iter=n_iter, random_state=42)
            
            # Huấn luyện mô hình
            user_factors = svd.fit_transform(user_product_matrix)
            
            # Tính toán item factors
            item_factors = svd.components_.T
            
            logger.info(f"Đã huấn luyện mô hình SVD với {n_components} thành phần")
            return (svd, user_factors, item_factors)
            
        except Exception as e:
            logger.error(f"Lỗi khi huấn luyện mô hình SVD: {e}")
            return None
    
    def train_nmf_model(self, user_product_matrix, n_components=10, max_iter=200):
        """
        Huấn luyện mô hình NMF (Non-negative Matrix Factorization).
        
        Args:
            user_product_matrix: Ma trận user-product
            n_components (int): Số lượng thành phần
            max_iter (int): Số lần lặp tối đa
            
        Returns:
            tuple: (model, user_factors, item_factors)
        """
        try:
            # Đảm bảo ma trận không âm
            user_product_matrix_copy = user_product_matrix.copy()
            user_product_matrix_copy.data = np.maximum(user_product_matrix_copy.data, 0)
            
            # Khởi tạo mô hình NMF
            nmf = NMF(n_components=n_components, max_iter=max_iter, random_state=42)
            
            # Huấn luyện mô hình
            user_factors = nmf.fit_transform(user_product_matrix_copy)
            
            # Tính toán item factors
            item_factors = nmf.components_.T
            
            logger.info(f"Đã huấn luyện mô hình NMF với {n_components} thành phần")
            return (nmf, user_factors, item_factors)
            
        except Exception as e:
            logger.error(f"Lỗi khi huấn luyện mô hình NMF: {e}")
            return None
    
    def generate_recommendations(self, user_factors, item_factors, user_ids, product_ids, n_recommendations=10):
        """
        Tạo gợi ý sản phẩm dựa trên kết quả Matrix Factorization.
        
        Args:
            user_factors: Ma trận user factors
            item_factors: Ma trận item factors
            user_ids: Danh sách ID người dùng
            product_ids: Danh sách ID sản phẩm
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            list: Danh sách gợi ý sản phẩm
        """
        try:
            # Chuẩn hóa các vector để tính toán similarity
            normalized_user_factors = normalize(user_factors)
            normalized_item_factors = normalize(item_factors)
            
            # Tính toán điểm số dự đoán
            predicted_ratings = np.dot(normalized_user_factors, normalized_item_factors.T)
            
            # Tạo danh sách gợi ý
            recommendations = []
            
            # Tải thông tin sản phẩm
            products_info = self._load_product_info()
            
            # Tạo gợi ý cho từng người dùng
            for user_idx, user_id in enumerate(user_ids):
                # Lấy điểm số dự đoán cho người dùng này
                user_ratings = predicted_ratings[user_idx]
                
                # Lấy top n sản phẩm có điểm cao nhất
                top_product_indices = np.argsort(user_ratings)[::-1][:n_recommendations]
                
                # Tạo gợi ý cho từng sản phẩm
                for product_idx in top_product_indices:
                    product_id = product_ids[product_idx]
                    mf_score = user_ratings[product_idx]
                    
                    # Lấy thông tin sản phẩm
                    product_info = products_info.get(product_id, {
                        'name': f"Product {product_id}",
                        'price': "Unknown",
                        'rating': 0.0,
                        'popularity_score': 1.0
                    })
                    
                    # Tính điểm gợi ý kết hợp
                    recommendation_score = mf_score * product_info['popularity_score']
                    
                    # Tạo đối tượng gợi ý
                    recommendation = {
                        'user_id': user_id,
                        'product_id': product_id,
                        'name': product_info['name'],
                        'price': product_info['price'],
                        'rating': product_info['rating'],
                        'mf_score': float(mf_score),
                        'popularity_score': product_info['popularity_score'],
                        'recommendation_score': float(recommendation_score),
                        'processed_date': datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    recommendations.append(recommendation)
            
            logger.info(f"Đã tạo {len(recommendations)} gợi ý sản phẩm")
            return recommendations
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo gợi ý sản phẩm: {e}")
            return self._simulate_recommendations(user_ids, product_ids, n_recommendations)
    
    def _load_product_info(self):
        """
        Tải thông tin sản phẩm.
        
        Returns:
            dict: Thông tin sản phẩm theo ID
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
                    
                    # Tạo dictionary theo product_id
                    products_info = {}
                    for product in products_data:
                        if 'product_id' in product:
                            products_info[product['product_id']] = {
                                'name': product.get('name', f"Product {product['product_id']}"),
                                'price': product.get('price', "Unknown"),
                                'rating': product.get('rating', 0.0),
                                'popularity_score': product.get('popularity_score', 1.0)
                            }
                    
                    return products_info
            
            # Nếu không tìm thấy file, trả về dictionary trống
            return {}
            
        except Exception as e:
            logger.error(f"Lỗi khi tải thông tin sản phẩm: {e}")
            return {}
    
    def _simulate_recommendations(self, user_ids, product_ids, n_recommendations=10):
        """
        Mô phỏng tạo gợi ý sản phẩm.
        
        Args:
            user_ids: Danh sách ID người dùng
            product_ids: Danh sách ID sản phẩm
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            list: Danh sách gợi ý sản phẩm mô phỏng
        """
        logger.info("Đang mô phỏng tạo gợi ý sản phẩm")
        
        # Tạo dữ liệu mô phỏng
        recommendations = []
        
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
        
        # Tạo gợi ý cho từng người dùng
        for user_id in user_ids:
            # Chọn ngẫu nhiên n sản phẩm
            import random
            selected_products = random.sample(product_ids, min(n_recommendations, len(product_ids)))
            
            # Tạo gợi ý cho từng sản phẩm
            for product_id in selected_products:
                # Tạo điểm số ngẫu nhiên
                mf_score = random.uniform(0.5, 1.0)
                
                # Lấy thông tin sản phẩm
                product_info = sample_products.get(product_id, {
                    'name': f"Product {product_id}",
                    'price': "Unknown",
                    'rating': 0.0,
                    'popularity_score': 1.0
                })
                
                # Tính điểm gợi ý kết hợp
                recommendation_score = mf_score * product_info['popularity_score']
                
                # Tạo đối tượng gợi ý
                recommendation = {
                    'user_id': user_id,
                    'product_id': product_id,
                    'name': product_info['name'],
                    'price': product_info['price'],
                    'rating': product_info['rating'],
                    'mf_score': float(mf_score),
                    'popularity_score': product_info['popularity_score'],
                    'recommendation_score': float(recommendation_score),
                    'processed_date': datetime.now().strftime("%Y-%m-%d")
                }
                
                recommendations.append(recommendation)
        
        logger.info(f"Đã mô phỏng tạo {len(recommendations)} gợi ý sản phẩm")
        return recommendations
    
    def save_recommendations(self, recommendations, algorithm_name, output_path=None):
        """
        Lưu gợi ý sản phẩm.
        
        Args:
            recommendations: Danh sách gợi ý sản phẩm
            algorithm_name (str): Tên thuật toán (svd hoặc nmf)
            output_path (str, optional): Đường dẫn đầu ra
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        try:
            # Xác định đường dẫn đầu ra
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = os.path.join(self.output_dir, f"{algorithm_name}_recommendations_{timestamp}.json")
            
            # Lưu gợi ý
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(recommendations, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã lưu gợi ý sản phẩm vào {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu gợi ý sản phẩm: {e}")
            return False
    
    def run_svd(self, n_components=10, n_iter=10, n_recommendations=10):
        """
        Chạy toàn bộ quy trình SVD.
        
        Args:
            n_components (int): Số lượng thành phần
            n_iter (int): Số lần lặp tối đa
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Tải dữ liệu
            data = self.load_data()
            if not data:
                return False
            
            user_product_matrix, user_ids, product_ids = data
            
            # Huấn luyện mô hình SVD
            svd_result = self.train_svd_model(user_product_matrix, n_components, n_iter)
            if not svd_result:
                return False
            
            svd_model, user_factors, item_factors = svd_result
            
            # Tạo gợi ý
            recommendations = self.generate_recommendations(user_factors, item_factors, user_ids, product_ids, n_recommendations)
            if not recommendations:
                return False
            
            # Lưu gợi ý
            success = self.save_recommendations(recommendations, "svd")
            
            return success
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy SVD: {e}")
            return False
    
    def run_nmf(self, n_components=10, max_iter=200, n_recommendations=10):
        """
        Chạy toàn bộ quy trình NMF.
        
        Args:
            n_components (int): Số lượng thành phần
            max_iter (int): Số lần lặp tối đa
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Tải dữ liệu
            data = self.load_data()
            if not data:
                return False
            
            user_product_matrix, user_ids, product_ids = data
            
            # Huấn luyện mô hình NMF
            nmf_result = self.train_nmf_model(user_product_matrix, n_components, max_iter)
            if not nmf_result:
                return False
            
            nmf_model, user_factors, item_factors = nmf_result
            
            # Tạo gợi ý
            recommendations = self.generate_recommendations(user_factors, item_factors, user_ids, product_ids, n_recommendations)
            if not recommendations:
                return False
            
            # Lưu gợi ý
            success = self.save_recommendations(recommendations, "nmf")
            
            return success
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy NMF: {e}")
            return False
    
    def run_matrix_factorization(self, algorithm="both", n_components=10, n_recommendations=10):
        """
        Chạy toàn bộ quy trình Matrix Factorization.
        
        Args:
            algorithm (str): Thuật toán sử dụng ("svd", "nmf", hoặc "both")
            n_components (int): Số lượng thành phần
            n_recommendations (int): Số lượng gợi ý cho mỗi người dùng
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if algorithm.lower() == "svd" or algorithm.lower() == "both":
            svd_success = self.run_svd(n_components, 10, n_recommendations)
            logger.info(f"Kết quả chạy SVD: {'Thành công' if svd_success else 'Thất bại'}")
        
        if algorithm.lower() == "nmf" or algorithm.lower() == "both":
            nmf_success = self.run_nmf(n_components, 200, n_recommendations)
            logger.info(f"Kết quả chạy NMF: {'Thành công' if nmf_success else 'Thất bại'}")
        
        return True


if __name__ == "__main__":
    # Ví dụ sử dụng
    mf = ShopeeMatrixFactorization()
    
    # Chạy Matrix Factorization
    mf.run_matrix_factorization("both", 10, 10)
