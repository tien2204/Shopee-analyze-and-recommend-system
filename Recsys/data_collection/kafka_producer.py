#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer cho dữ liệu Shopee

Mô-đun này đẩy dữ liệu từ web scraper vào Kafka để xử lý theo thời gian thực.
"""

import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_kafka_producer')

class ShopeeKafkaProducer:
    """
    Lớp ShopeeKafkaProducer gửi dữ liệu sản phẩm Shopee đến Kafka.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topic_prefix='shopee'):
        """
        Khởi tạo Kafka producer với các tùy chọn cấu hình.
        
        Args:
            bootstrap_servers (str): Danh sách các Kafka broker
            topic_prefix (str): Tiền tố cho các Kafka topic
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.producer = None
        
        # Các topic mặc định
        self.topics = {
            'products': f"{topic_prefix}_products",
            'reviews': f"{topic_prefix}_reviews",
            'user_behavior': f"{topic_prefix}_user_behavior"
        }
        
        # Khởi tạo Kafka producer
        self._initialize_producer()
    
    def _initialize_producer(self):
        """
        Khởi tạo Kafka producer với cấu hình phù hợp.
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Đảm bảo tin nhắn được ghi vào tất cả các replica
                retries=3,   # Số lần thử lại nếu gửi thất bại
                linger_ms=10,  # Thời gian chờ gộp các tin nhắn
                batch_size=16384  # Kích thước batch tối đa
            )
            logger.info(f"Kafka producer đã được khởi tạo thành công với bootstrap servers: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka producer: {e}")
            raise
    
    def send_product_data(self, product_data, key=None):
        """
        Gửi dữ liệu sản phẩm đến Kafka topic.
        
        Args:
            product_data (dict): Dữ liệu sản phẩm
            key (str, optional): Khóa cho tin nhắn Kafka
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        return self._send_data(self.topics['products'], product_data, key)
    
    def send_review_data(self, review_data, key=None):
        """
        Gửi dữ liệu đánh giá sản phẩm đến Kafka topic.
        
        Args:
            review_data (dict): Dữ liệu đánh giá
            key (str, optional): Khóa cho tin nhắn Kafka
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        return self._send_data(self.topics['reviews'], review_data, key)
    
    def send_user_behavior_data(self, behavior_data, key=None):
        """
        Gửi dữ liệu hành vi người dùng đến Kafka topic.
        
        Args:
            behavior_data (dict): Dữ liệu hành vi người dùng
            key (str, optional): Khóa cho tin nhắn Kafka
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        return self._send_data(self.topics['user_behavior'], behavior_data, key)
    
    def _send_data(self, topic, data, key=None):
        """
        Gửi dữ liệu đến Kafka topic.
        
        Args:
            topic (str): Tên Kafka topic
            data (dict): Dữ liệu cần gửi
            key (str, optional): Khóa cho tin nhắn Kafka
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        if not self.producer:
            logger.error("Kafka producer chưa được khởi tạo")
            return False
        
        try:
            # Thêm timestamp nếu chưa có
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Gửi dữ liệu đến Kafka
            future = self.producer.send(topic, value=data, key=key)
            
            # Đợi kết quả để đảm bảo tin nhắn được gửi thành công
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Đã gửi dữ liệu đến topic {topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Lỗi khi gửi dữ liệu đến Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Lỗi không xác định khi gửi dữ liệu: {e}")
            return False
    
    def send_batch_from_file(self, file_path, topic=None, key_field=None):
        """
        Đọc dữ liệu từ file JSON và gửi theo batch đến Kafka.
        
        Args:
            file_path (str): Đường dẫn đến file JSON
            topic (str, optional): Tên topic cụ thể, nếu không sẽ dùng topic mặc định dựa vào tên file
            key_field (str, optional): Tên trường dùng làm khóa cho tin nhắn
            
        Returns:
            tuple: (Số lượng tin nhắn thành công, tổng số tin nhắn)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Xác định topic dựa vào tên file nếu không được chỉ định
            if not topic:
                if 'product' in file_path.lower():
                    topic = self.topics['products']
                elif 'review' in file_path.lower():
                    topic = self.topics['reviews']
                elif 'user' in file_path.lower() or 'behavior' in file_path.lower():
                    topic = self.topics['user_behavior']
                else:
                    topic = self.topics['products']  # Mặc định
            
            success_count = 0
            total_count = 0
            
            # Xử lý dữ liệu dạng list
            if isinstance(data, list):
                total_count = len(data)
                for item in data:
                    key = item.get(key_field) if key_field and key_field in item else None
                    if self._send_data(topic, item, key):
                        success_count += 1
            # Xử lý dữ liệu dạng dict
            elif isinstance(data, dict):
                total_count = 1
                key = data.get(key_field) if key_field and key_field in data else None
                if self._send_data(topic, data, key):
                    success_count += 1
            
            logger.info(f"Đã gửi {success_count}/{total_count} tin nhắn từ file {file_path} đến topic {topic}")
            return (success_count, total_count)
            
        except Exception as e:
            logger.error(f"Lỗi khi gửi dữ liệu từ file {file_path}: {e}")
            return (0, 0)
    
    def close(self):
        """
        Đóng Kafka producer và giải phóng tài nguyên.
        """
        if self.producer:
            self.producer.flush()  # Đảm bảo tất cả tin nhắn đã được gửi
            self.producer.close()
            logger.info("Kafka producer đã được đóng")


if __name__ == "__main__":
    # Ví dụ sử dụng
    producer = ShopeeKafkaProducer()
    
    try:
        # Gửi dữ liệu sản phẩm mẫu
        sample_product = {
            "name": "Samsung Galaxy S21",
            "price": "15.990.000đ",
            "rating": 4.8,
            "sold": "1k2",
            "location": "TP. Hồ Chí Minh"
        }
        producer.send_product_data(sample_product, key="samsung_s21")
        
        # Gửi dữ liệu từ file (nếu có)
        data_dir = "../data"
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith('.json'):
                    producer.send_batch_from_file(os.path.join(data_dir, file))
    
    finally:
        # Đóng producer
        producer.close()
