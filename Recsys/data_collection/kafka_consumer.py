#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Consumer cho dữ liệu Shopee

Mô-đun này đọc dữ liệu từ Kafka và xử lý theo thời gian thực.
"""

import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_kafka_consumer')

class ShopeeKafkaConsumer:
    """
    Lớp ShopeeKafkaConsumer đọc dữ liệu sản phẩm Shopee từ Kafka.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topic_prefix='shopee', 
                 group_id='shopee_consumer_group', output_dir='../data'):
        """
        Khởi tạo Kafka consumer với các tùy chọn cấu hình.
        
        Args:
            bootstrap_servers (str): Danh sách các Kafka broker
            topic_prefix (str): Tiền tố cho các Kafka topic
            group_id (str): ID của consumer group
            output_dir (str): Thư mục lưu trữ dữ liệu
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.group_id = group_id
        self.output_dir = output_dir
        self.consumers = {}
        
        # Các topic mặc định
        self.topics = {
            'products': f"{topic_prefix}_products",
            'reviews': f"{topic_prefix}_reviews",
            'user_behavior': f"{topic_prefix}_user_behavior"
        }
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def _initialize_consumer(self, topics, group_id=None):
        """
        Khởi tạo Kafka consumer với cấu hình phù hợp.
        
        Args:
            topics (list): Danh sách các topic cần đọc
            group_id (str, optional): ID của consumer group
            
        Returns:
            KafkaConsumer: Đối tượng Kafka consumer
        """
        try:
            if not group_id:
                group_id = self.group_id
            
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',  # Bắt đầu từ tin nhắn đầu tiên nếu không có offset
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000  # Tự động commit offset mỗi 5 giây
            )
            
            logger.info(f"Kafka consumer đã được khởi tạo thành công cho topics {topics} với group_id {group_id}")
            return consumer
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka consumer: {e}")
            raise
    
    def consume_products(self, callback=None, batch_size=100, timeout_ms=1000):
        """
        Đọc dữ liệu sản phẩm từ Kafka topic.
        
        Args:
            callback (function, optional): Hàm callback xử lý dữ liệu
            batch_size (int): Số lượng tin nhắn tối đa trong một batch
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            list: Danh sách dữ liệu sản phẩm
        """
        return self._consume_data([self.topics['products']], callback, batch_size, timeout_ms)
    
    def consume_reviews(self, callback=None, batch_size=100, timeout_ms=1000):
        """
        Đọc dữ liệu đánh giá sản phẩm từ Kafka topic.
        
        Args:
            callback (function, optional): Hàm callback xử lý dữ liệu
            batch_size (int): Số lượng tin nhắn tối đa trong một batch
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            list: Danh sách dữ liệu đánh giá
        """
        return self._consume_data([self.topics['reviews']], callback, batch_size, timeout_ms)
    
    def consume_user_behavior(self, callback=None, batch_size=100, timeout_ms=1000):
        """
        Đọc dữ liệu hành vi người dùng từ Kafka topic.
        
        Args:
            callback (function, optional): Hàm callback xử lý dữ liệu
            batch_size (int): Số lượng tin nhắn tối đa trong một batch
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            list: Danh sách dữ liệu hành vi người dùng
        """
        return self._consume_data([self.topics['user_behavior']], callback, batch_size, timeout_ms)
    
    def consume_all(self, callback=None, batch_size=100, timeout_ms=1000):
        """
        Đọc dữ liệu từ tất cả các Kafka topic.
        
        Args:
            callback (function, optional): Hàm callback xử lý dữ liệu
            batch_size (int): Số lượng tin nhắn tối đa trong một batch
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            dict: Từ điển dữ liệu theo topic
        """
        topics = list(self.topics.values())
        return self._consume_data(topics, callback, batch_size, timeout_ms)
    
    def _consume_data(self, topics, callback=None, batch_size=100, timeout_ms=1000):
        """
        Đọc dữ liệu từ Kafka topic.
        
        Args:
            topics (list): Danh sách các topic cần đọc
            callback (function, optional): Hàm callback xử lý dữ liệu
            batch_size (int): Số lượng tin nhắn tối đa trong một batch
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            list or dict: Dữ liệu đọc được từ Kafka
        """
        # Tạo key cho consumer
        topics_key = '_'.join(topics)
        
        # Khởi tạo consumer nếu chưa tồn tại
        if topics_key not in self.consumers:
            self.consumers[topics_key] = self._initialize_consumer(topics)
        
        consumer = self.consumers[topics_key]
        
        # Đọc dữ liệu
        try:
            # Nếu chỉ có một topic, trả về list
            if len(topics) == 1:
                messages = []
                message_count = 0
                
                while message_count < batch_size:
                    # Poll tin nhắn từ Kafka
                    records = consumer.poll(timeout_ms=timeout_ms, max_records=batch_size - message_count)
                    
                    if not records:
                        break
                    
                    for topic_partition, partition_messages in records.items():
                        for message in partition_messages:
                            # Lấy dữ liệu từ tin nhắn
                            data = message.value
                            
                            # Gọi callback nếu có
                            if callback:
                                callback(data, message.topic, message.key)
                            
                            messages.append(data)
                            message_count += 1
                
                logger.info(f"Đã đọc {message_count} tin nhắn từ topic {topics[0]}")
                return messages
            
            # Nếu có nhiều topic, trả về dict
            else:
                messages_by_topic = {topic: [] for topic in topics}
                message_count = 0
                
                while message_count < batch_size:
                    # Poll tin nhắn từ Kafka
                    records = consumer.poll(timeout_ms=timeout_ms, max_records=batch_size - message_count)
                    
                    if not records:
                        break
                    
                    for topic_partition, partition_messages in records.items():
                        topic = topic_partition.topic
                        
                        for message in partition_messages:
                            # Lấy dữ liệu từ tin nhắn
                            data = message.value
                            
                            # Gọi callback nếu có
                            if callback:
                                callback(data, topic, message.key)
                            
                            messages_by_topic[topic].append(data)
                            message_count += 1
                
                logger.info(f"Đã đọc tổng cộng {message_count} tin nhắn từ {len(topics)} topics")
                return messages_by_topic
                
        except Exception as e:
            logger.error(f"Lỗi khi đọc dữ liệu từ Kafka: {e}")
            return [] if len(topics) == 1 else {topic: [] for topic in topics}
    
    def consume_and_save(self, topics=None, output_format='json', max_messages=1000, timeout_ms=10000):
        """
        Đọc dữ liệu từ Kafka và lưu vào file.
        
        Args:
            topics (list, optional): Danh sách các topic cần đọc, nếu None sẽ đọc tất cả
            output_format (str): Định dạng đầu ra ('json' hoặc 'csv')
            max_messages (int): Số lượng tin nhắn tối đa cần đọc
            timeout_ms (int): Thời gian chờ tối đa (ms) cho mỗi poll
            
        Returns:
            dict: Thông tin về số lượng tin nhắn đã lưu theo topic
        """
        if not topics:
            topics = list(self.topics.values())
        elif isinstance(topics, str):
            topics = [topics]
        
        # Hàm callback để lưu dữ liệu
        data_by_topic = {topic: [] for topic in topics}
        
        def save_callback(data, topic, key):
            data_by_topic[topic].append(data)
        
        # Đọc dữ liệu
        self._consume_data(topics, save_callback, max_messages, timeout_ms)
        
        # Lưu dữ liệu vào file
        result = {}
        for topic, data in data_by_topic.items():
            if not data:
                result[topic] = 0
                continue
            
            # Tạo tên file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{topic}_{timestamp}"
            
            # Lưu theo định dạng
            if output_format.lower() == 'json':
                file_path = os.path.join(self.output_dir, f"{filename}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
            elif output_format.lower() == 'csv':
                file_path = os.path.join(self.output_dir, f"{filename}.csv")
                df = pd.DataFrame(data)
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            result[topic] = len(data)
            logger.info(f"Đã lưu {len(data)} tin nhắn từ topic {topic} vào file {file_path}")
        
        return result
    
    def start_continuous_consumer(self, topics=None, save_interval=60, output_format='json'):
        """
        Bắt đầu consumer liên tục, lưu dữ liệu theo định kỳ.
        
        Args:
            topics (list, optional): Danh sách các topic cần đọc, nếu None sẽ đọc tất cả
            save_interval (int): Khoảng thời gian (giây) giữa các lần lưu
            output_format (str): Định dạng đầu ra ('json' hoặc 'csv')
        """
        if not topics:
            topics = list(self.topics.values())
        elif isinstance(topics, str):
            topics = [topics]
        
        logger.info(f"Bắt đầu consumer liên tục cho topics {topics}")
        
        try:
            while True:
                # Đọc và lưu dữ liệu
                result = self.consume_and_save(topics, output_format)
                
                # Hiển thị thông tin
                total_messages = sum(result.values())
                logger.info(f"Đã lưu tổng cộng {total_messages} tin nhắn từ {len(topics)} topics")
                
                # Đợi đến lần lưu tiếp theo
                logger.info(f"Đợi {save_interval} giây đến lần lưu tiếp theo...")
                time.sleep(save_interval)
                
        except KeyboardInterrupt:
            logger.info("Đã nhận tín hiệu dừng, đang thoát...")
        except Exception as e:
            logger.error(f"Lỗi trong quá trình consumer liên tục: {e}")
        finally:
            self.close()
    
    def close(self):
        """
        Đóng tất cả Kafka consumer và giải phóng tài nguyên.
        """
        for topic_key, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Đã đóng consumer cho {topic_key}")
            except Exception as e:
                logger.error(f"Lỗi khi đóng consumer cho {topic_key}: {e}")


if __name__ == "__main__":
    # Ví dụ sử dụng
    consumer = ShopeeKafkaConsumer()
    
    try:
        # Đọc và lưu dữ liệu từ tất cả các topic
        consumer.consume_and_save()
        
        # Hoặc bắt đầu consumer liên tục
        # consumer.start_continuous_consumer(save_interval=30)
    
    finally:
        # Đóng consumer
        consumer.close()
