#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flask App cho hệ thống gợi ý sản phẩm Shopee

Mô-đun này triển khai giao diện web cho hệ thống gợi ý sản phẩm Shopee.
"""

import os
import sys
import json
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_cors import CORS

# Thêm thư mục cha vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import các module từ hệ thống
try:
    from query_system.spark_sql_manager import ShopeeSQLManager
    from query_system.serving_layer import ShopeeServingLayer
except ImportError:
    print("Không thể import các module từ hệ thống. Đang chuyển sang chế độ mô phỏng.")

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_web_interface')

# Khởi tạo Flask app
app = Flask(__name__)
CORS(app)  # Cho phép Cross-Origin Resource Sharing

# Cấu hình
app.config['SECRET_KEY'] = 'shopee-recommendation-system'
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Khởi tạo các manager
try:
    sql_manager = ShopeeSQLManager()
    serving_layer = ShopeeServingLayer()
    serving_layer.start()
    use_simulation = False
except Exception as e:
    logger.error(f"Lỗi khi khởi tạo các manager: {e}")
    logger.warning("Đang chuyển sang chế độ mô phỏng.")
    use_simulation = True

# Dữ liệu mô phỏng
def get_simulated_data(data_type, **kwargs):
    """
    Lấy dữ liệu mô phỏng.
    
    Args:
        data_type (str): Loại dữ liệu
        **kwargs: Các tham số bổ sung
        
    Returns:
        dict: Dữ liệu mô phỏng
    """
    if data_type == 'top_products':
        return {
            'products': [
                {
                    'product_id': '987654321',
                    'name': 'iPhone 13 Pro Max',
                    'price': '29.990.000đ',
                    'rating': 4.9,
                    'sold': '2k5',
                    'location': 'Hà Nội',
                    'url': 'https://shopee.vn/product/987654321',
                    'popularity_score': 12.25,
                    'image_url': '/static/images/iphone13.jpg'
                },
                {
                    'product_id': '789789789',
                    'name': 'Tai nghe Apple AirPods Pro',
                    'price': '5.990.000đ',
                    'rating': 4.9,
                    'sold': '1k8',
                    'location': 'TP. Hồ Chí Minh',
                    'url': 'https://shopee.vn/product/789789789',
                    'popularity_score': 8.82,
                    'image_url': '/static/images/airpods.jpg'
                },
                {
                    'product_id': '123456789',
                    'name': 'Samsung Galaxy S21',
                    'price': '15.990.000đ',
                    'rating': 4.8,
                    'sold': '1k2',
                    'location': 'TP. Hồ Chí Minh',
                    'url': 'https://shopee.vn/product/123456789',
                    'popularity_score': 5.76,
                    'image_url': '/static/images/samsung.jpg'
                },
                {
                    'product_id': '123123123',
                    'name': 'Xiaomi Mi 11',
                    'price': '12.490.000đ',
                    'rating': 4.7,
                    'sold': '950',
                    'location': 'TP. Hồ Chí Minh',
                    'url': 'https://shopee.vn/product/123123123',
                    'popularity_score': 4.5,
                    'image_url': '/static/images/xiaomi.jpg'
                },
                {
                    'product_id': '456456456',
                    'name': 'Laptop Dell XPS 13',
                    'price': '32.990.000đ',
                    'rating': 4.8,
                    'sold': '320',
                    'location': 'Hà Nội',
                    'url': 'https://shopee.vn/product/456456456',
                    'popularity_score': 3.8,
                    'image_url': '/static/images/dell.jpg'
                }
            ]
        }
    elif data_type == 'product_detail':
        product_id = kwargs.get('product_id', '123456789')
        
        products = {
            '123456789': {
                'product_id': '123456789',
                'name': 'Samsung Galaxy S21',
                'price': '15.990.000đ',
                'rating': 4.8,
                'sold': '1k2',
                'location': 'TP. Hồ Chí Minh',
                'url': 'https://shopee.vn/product/123456789',
                'popularity_score': 5.76,
                'category': 'Điện thoại & Phụ kiện',
                'brand': 'Samsung',
                'description': 'Samsung Galaxy S21 5G là chiếc điện thoại Android cao cấp với màn hình Dynamic AMOLED 2X, chip Exynos 2100, camera 64MP và pin 4000mAh.',
                'image_url': '/static/images/samsung.jpg',
                'reviews': [
                    {
                        'username': 'user123',
                        'rating': 5,
                        'comment': 'Sản phẩm rất tốt, đóng gói cẩn thận, giao hàng nhanh!',
                        'time': '2025-03-15',
                        'sentiment_score': 1.0
                    },
                    {
                        'username': 'user789',
                        'rating': 4,
                        'comment': 'Sản phẩm tốt, nhưng giao hàng hơi chậm.',
                        'time': '2025-03-17',
                        'sentiment_score': 0.5
                    }
                ]
            },
            '987654321': {
                'product_id': '987654321',
                'name': 'iPhone 13 Pro Max',
                'price': '29.990.000đ',
                'rating': 4.9,
                'sold': '2k5',
                'location': 'Hà Nội',
                'url': 'https://shopee.vn/product/987654321',
                'popularity_score': 12.25,
                'category': 'Điện thoại & Phụ kiện',
                'brand': 'Apple',
                'description': 'iPhone 13 Pro Max là chiếc điện thoại cao cấp nhất của Apple với màn hình Super Retina XDR, chip A15 Bionic, camera 12MP và pin dung lượng lớn.',
                'image_url': '/static/images/iphone13.jpg',
                'reviews': [
                    {
                        'username': 'user456',
                        'rating': 2,
                        'comment': 'Sản phẩm không như mô tả, chất lượng kém.',
                        'time': '2025-03-16',
                        'sentiment_score': -1.0
                    }
                ]
            },
            '123123123': {
                'product_id': '123123123',
                'name': 'Xiaomi Mi 11',
                'price': '12.490.000đ',
                'rating': 4.7,
                'sold': '950',
                'location': 'TP. Hồ Chí Minh',
                'url': 'https://shopee.vn/product/123123123',
                'popularity_score': 4.5,
                'category': 'Điện thoại & Phụ kiện',
                'brand': 'Xiaomi',
                'description': 'Xiaomi Mi 11 là flagship với màn hình AMOLED 6.81 inch, chip Snapdragon 888, camera 108MP và pin 4600mAh với sạc nhanh 55W.',
                'image_url': '/static/images/xiaomi.jpg',
                'reviews': [
                    {
                        'username': 'user101',
                        'rating': 5,
                        'comment': 'Tuyệt vời, sẽ mua lại lần sau!',
                        'time': '2025-03-18',
                        'sentiment_score': 1.0
                    }
                ]
            },
            '456456456': {
                'product_id': '456456456',
                'name': 'Laptop Dell XPS 13',
                'price': '32.990.000đ',
                'rating': 4.8,
                'sold': '320',
                'location': 'Hà Nội',
                'url': 'https://shopee.vn/product/456456456',
                'popularity_score': 3.8,
                'category': 'Máy tính & Laptop',
                'brand': 'Dell',
                'description': 'Dell XPS 13 là laptop cao cấp với màn hình InfinityEdge, chip Intel Core i7, RAM 16GB và SSD 512GB.',
                'image_url': '/static/images/dell.jpg',
                'reviews': [
                    {
                        'username': 'user202',
                        'rating': 3,
                        'comment': 'Sản phẩm tạm được, không quá xuất sắc.',
                        'time': '2025-03-19',
                        'sentiment_score': 0.0
                    }
                ]
            },
            '789789789': {
                'product_id': '789789789',
                'name': 'Tai nghe Apple AirPods Pro',
                'price': '5.990.000đ',
                'rating': 4.9,
                'sold': '1k8',
                'location': 'TP. Hồ Chí Minh',
                'url': 'https://shopee.vn/product/789789789',
                'popularity_score': 8.82,
                'category': 'Thiết bị âm thanh',
                'brand': 'Apple',
                'description': 'AirPods Pro là tai nghe không dây cao cấp của Apple với tính năng chống ồn chủ động, chế độ xuyên âm và chất lượng âm thanh tuyệt vời.',
                'image_url': '/static/images/airpods.jpg',
                'reviews': [
                    {
                        'username': 'user303',
                        'rating': 5,
                        'comment': 'Chất lượng âm thanh tuyệt vời, rất đáng tiền!',
                        'time': '2025-03-20',
                        'sentiment_score': 1.0
                    }
                ]
            }
        }
        
        return products.get(product_id, products['123456789'])
    elif data_type == 'user_recommendations':
        user_id = kwargs.get('user_id', 'user123')
        
        recommendations = {
            'user123': [
                {
                    'product_id': '789789789',
                    'name': 'Tai nghe Apple AirPods Pro',
                    'price': '5.990.000đ',
                    'rating': 4.9,
                    'recommendation_score': 9.5,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/airpods.jpg'
                },
                {
                    'product_id': '456456456',
                    'name': 'Laptop Dell XPS 13',
                    'price': '32.990.000đ',
                    'rating': 4.8,
                    'recommendation_score': 8.2,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/dell.jpg'
                },
                {
                    'product_id': '123123123',
                    'name': 'Xiaomi Mi 11',
                    'price': '12.490.000đ',
                    'rating': 4.7,
                    'recommendation_score': 7.8,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/xiaomi.jpg'
                }
            ],
            'user456': [
                {
                    'product_id': '123123123',
                    'name': 'Xiaomi Mi 11',
                    'price': '12.490.000đ',
                    'rating': 4.7,
                    'recommendation_score': 9.2,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/xiaomi.jpg'
                },
                {
                    'product_id': '789789789',
                    'name': 'Tai nghe Apple AirPods Pro',
                    'price': '5.990.000đ',
                    'rating': 4.9,
                    'recommendation_score': 8.7,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/airpods.jpg'
                },
                {
                    'product_id': '123456789',
                    'name': 'Samsung Galaxy S21',
                    'price': '15.990.000đ',
                    'rating': 4.8,
                    'recommendation_score': 7.5,
                    'algorithm': 'hybrid',
                    'image_url': '/static/images/samsung.jpg'
                }
            ]
        }
        
        return {'recommendations': recommendations.get(user_id, recommendations['user123'])}
    elif data_type == 'algorithm_comparison':
        user_id = kwargs.get('user_id', 'user123')
        
        comparisons = {
            'user123': {
                'user_id': 'user123',
                'cf': 0.85,
                'mf': 0.78,
                'hybrid': 0.92,
                'products': [
                    {
                        'product_id': '789789789',
                        'name': 'Tai nghe Apple AirPods Pro',
                        'cf_score': 0.82,
                        'mf_score': 0.75,
                        'hybrid_score': 0.88
                    },
                    {
                        'product_id': '456456456',
                        'name': 'Laptop Dell XPS 13',
                        'cf_score': 0.78,
                        'mf_score': 0.72,
                        'hybrid_score': 0.85
                    },
                    {
                        'product_id': '123123123',
                        'name': 'Xiaomi Mi 11',
                        'cf_score': 0.75,
                        'mf_score': 0.80,
                        'hybrid_score': 0.82
                    }
                ]
            },
            'user456': {
                'user_id': 'user456',
                'cf': 0.92,
                'mf': 0.85,
                'hybrid': 0.95,
                'products': [
                    {
                        'product_id': '123123123',
                        'name': 'Xiaomi Mi 11',
                        'cf_score': 0.90,
                        'mf_score': 0.82,
                        'hybrid_score': 0.92
                    },
                    {
                        'product_id': '789789789',
                        'name': 'Tai nghe Apple AirPods Pro',
                        'cf_score': 0.85,
                        'mf_score': 0.80,
                        'hybrid_score': 0.88
                    },
                    {
                        'product_id': '123456789',
                        'name': 'Samsung Galaxy S21',
                        'cf_score': 0.80,
                        'mf_score': 0.75,
                        'hybrid_score': 0.85
                    }
                ]
            }
        }
        
        return comparisons.get(user_id, comparisons['user123'])
    elif data_type == 'categories':
        return {
            'categories': [
                {
                    'name': 'Điện thoại & Phụ kiện',
                    'product_count': 3,
                    'avg_price': 19490000.0,
                    'avg_rating': 4.8
                },
                {
                    'name': 'Máy tính & Laptop',
                    'product_count': 1,
                    'avg_price': 32990000.0,
                    'avg_rating': 4.8
                },
                {
                    'name': 'Thiết bị âm thanh',
                    'product_count': 1,
                    'avg_price': 5990000.0,
                    'avg_rating': 4.9
                }
            ]
        }
    elif data_type == 'user_behavior':
        user_id = kwargs.get('user_id', 'user123')
        
        behaviors = {
            'user123': {
                'user_id': 'user123',
                'total_actions': 4,
                'avg_interaction': 5.0,
                'products': [
                    {
                        'product_id': '123456789',
                        'name': 'Samsung Galaxy S21',
                        'action_frequency': 3,
                        'interaction_score': 9.0
                  
(Content truncated due to size limit. Use line ranges to read in chunks)