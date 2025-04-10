#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Elasticsearch Manager cho dữ liệu Shopee

Mô-đun này quản lý việc lưu trữ và truy vấn dữ liệu trên Elasticsearch.
"""

import os
import json
import logging
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, NotFoundError

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_elasticsearch')

class ShopeeElasticsearch:
    """
    Lớp ShopeeElasticsearch quản lý việc lưu trữ và truy vấn dữ liệu trên Elasticsearch.
    """
    
    def __init__(self, hosts=['localhost:9200'], index_prefix='shopee',
                 local_data_dir='../data/batch_processed'):
        """
        Khởi tạo Elasticsearch với các tùy chọn cấu hình.
        
        Args:
            hosts (list): Danh sách các Elasticsearch host
            index_prefix (str): Tiền tố cho các Elasticsearch index
            local_data_dir (str): Thư mục dữ liệu cục bộ
        """
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.local_data_dir = local_data_dir
        
        # Các index
        self.indices = {
            'products': f"{index_prefix}_products",
            'reviews': f"{index_prefix}_reviews",
            'user_behavior': f"{index_prefix}_user_behavior",
            'recommendations': f"{index_prefix}_recommendations"
        }
        
        # Khởi tạo Elasticsearch client
        self._initialize_elasticsearch()
    
    def _initialize_elasticsearch(self):
        """
        Khởi tạo Elasticsearch client và kiểm tra kết nối.
        """
        try:
            # Khởi tạo Elasticsearch client
            self.es = Elasticsearch(self.hosts)
            
            # Kiểm tra kết nối
            if self.es.ping():
                logger.info(f"Đã kết nối thành công đến Elasticsearch: {self.hosts}")
                self._simulate_es = False
            else:
                logger.warning(f"Không thể kết nối đến Elasticsearch: {self.hosts}")
                logger.warning("Đang chuyển sang chế độ mô phỏng Elasticsearch.")
                self._simulate_es = True
                
        except ConnectionError as e:
            logger.error(f"Lỗi kết nối đến Elasticsearch: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Elasticsearch.")
            self._simulate_es = True
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Elasticsearch: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng Elasticsearch.")
            self._simulate_es = True
    
    def create_index(self, index_name, mappings=None):
        """
        Tạo Elasticsearch index với mappings.
        
        Args:
            index_name (str): Tên index
            mappings (dict, optional): Cấu trúc mappings cho index
            
        Returns:
            bool: True nếu tạo thành công, False nếu thất bại
        """
        if self._simulate_es:
            return self._simulate_create_index(index_name, mappings)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Kiểm tra index đã tồn tại chưa
            if self.es.indices.exists(index=full_index_name):
                logger.info(f"Index {full_index_name} đã tồn tại")
                return True
            
            # Tạo index với mappings
            if mappings:
                body = {"mappings": mappings}
                result = self.es.indices.create(index=full_index_name, body=body)
            else:
                result = self.es.indices.create(index=full_index_name)
            
            if result.get('acknowledged', False):
                logger.info(f"Đã tạo index {full_index_name}")
                return True
            else:
                logger.error(f"Không thể tạo index {full_index_name}: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi tạo index {index_name}: {e}")
            return False
    
    def delete_index(self, index_name):
        """
        Xóa Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            
        Returns:
            bool: True nếu xóa thành công, False nếu thất bại
        """
        if self._simulate_es:
            return self._simulate_delete_index(index_name)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Kiểm tra index có tồn tại không
            if not self.es.indices.exists(index=full_index_name):
                logger.warning(f"Index {full_index_name} không tồn tại")
                return True
            
            # Xóa index
            result = self.es.indices.delete(index=full_index_name)
            
            if result.get('acknowledged', False):
                logger.info(f"Đã xóa index {full_index_name}")
                return True
            else:
                logger.error(f"Không thể xóa index {full_index_name}: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi xóa index {index_name}: {e}")
            return False
    
    def index_document(self, index_name, doc_id, document):
        """
        Lưu document vào Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            document (dict): Nội dung document
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        if self._simulate_es:
            return self._simulate_index_document(index_name, doc_id, document)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Lưu document
            result = self.es.index(index=full_index_name, id=doc_id, body=document)
            
            if result.get('result') in ['created', 'updated']:
                logger.debug(f"Đã lưu document {doc_id} vào index {full_index_name}")
                return True
            else:
                logger.error(f"Không thể lưu document {doc_id} vào index {full_index_name}: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi lưu document {doc_id} vào index {index_name}: {e}")
            return False
    
    def bulk_index(self, index_name, documents, id_field=None):
        """
        Lưu nhiều document vào Elasticsearch index theo batch.
        
        Args:
            index_name (str): Tên index
            documents (list): Danh sách các document
            id_field (str, optional): Tên trường dùng làm ID
            
        Returns:
            tuple: (Số lượng thành công, tổng số)
        """
        if self._simulate_es:
            return self._simulate_bulk_index(index_name, documents, id_field)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Tạo danh sách các action
            actions = []
            for i, doc in enumerate(documents):
                # Xác định ID
                if id_field and id_field in doc:
                    doc_id = doc[id_field]
                else:
                    doc_id = i
                
                # Tạo action
                action = {
                    "_index": full_index_name,
                    "_id": doc_id,
                    "_source": doc
                }
                actions.append(action)
            
            # Thực hiện bulk index
            if actions:
                success, failed = 0, 0
                
                # Chia nhỏ thành các batch để tránh quá tải
                batch_size = 1000
                for i in range(0, len(actions), batch_size):
                    batch = actions[i:i+batch_size]
                    result = helpers.bulk(self.es, batch, stats_only=True)
                    success += result[0]
                    failed += result[1]
                
                logger.info(f"Đã lưu {success}/{len(actions)} document vào index {full_index_name}")
                return (success, len(actions))
            else:
                logger.warning(f"Không có document nào để lưu vào index {full_index_name}")
                return (0, 0)
                
        except Exception as e:
            logger.error(f"Lỗi khi bulk index vào {index_name}: {e}")
            return (0, len(documents) if documents else 0)
    
    def search(self, index_name, query, size=10):
        """
        Tìm kiếm trên Elasticsearch.
        
        Args:
            index_name (str): Tên index
            query (dict): Query DSL
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách kết quả
        """
        if self._simulate_es:
            return self._simulate_search(index_name, query, size)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Thực hiện tìm kiếm
            result = self.es.search(index=full_index_name, body=query, size=size)
            
            # Trích xuất kết quả
            hits = result.get('hits', {}).get('hits', [])
            results = [hit['_source'] for hit in hits]
            
            logger.info(f"Đã tìm thấy {len(results)} kết quả từ index {full_index_name}")
            return results
            
        except Exception as e:
            logger.error(f"Lỗi khi tìm kiếm trên index {index_name}: {e}")
            return []
    
    def get_document(self, index_name, doc_id):
        """
        Lấy document từ Elasticsearch theo ID.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            
        Returns:
            dict: Document nếu tìm thấy, None nếu không tìm thấy
        """
        if self._simulate_es:
            return self._simulate_get_document(index_name, doc_id)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Lấy document
            result = self.es.get(index=full_index_name, id=doc_id)
            
            if result.get('found', False):
                return result['_source']
            else:
                logger.warning(f"Không tìm thấy document {doc_id} trong index {full_index_name}")
                return None
                
        except NotFoundError:
            logger.warning(f"Không tìm thấy document {doc_id} trong index {index_name}")
            return None
        except Exception as e:
            logger.error(f"Lỗi khi lấy document {doc_id} từ index {index_name}: {e}")
            return None
    
    def delete_document(self, index_name, doc_id):
        """
        Xóa document từ Elasticsearch theo ID.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            
        Returns:
            bool: True nếu xóa thành công, False nếu thất bại
        """
        if self._simulate_es:
            return self._simulate_delete_document(index_name, doc_id)
        
        try:
            # Xác định tên index đầy đủ
            if index_name in self.indices:
                full_index_name = self.indices[index_name]
            else:
                full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
            
            # Xóa document
            result = self.es.delete(index=full_index_name, id=doc_id)
            
            if result.get('result') == 'deleted':
                logger.info(f"Đã xóa document {doc_id} từ index {full_index_name}")
                return True
            else:
                logger.error(f"Không thể xóa document {doc_id} từ index {full_index_name}: {result}")
                return False
                
        except NotFoundError:
            logger.warning(f"Không tìm thấy document {doc_id} trong index {index_name}")
            return True  # Coi như đã xóa nếu không tìm thấy
        except Exception as e:
            logger.error(f"Lỗi khi xóa document {doc_id} từ index {index_name}: {e}")
            return False
    
    def index_from_file(self, index_name, file_path, id_field=None):
        """
        Lưu dữ liệu từ file vào Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            file_path (str): Đường dẫn đến file dữ liệu (JSON)
            id_field (str, optional): Tên trường dùng làm ID
            
        Returns:
            tuple: (Số lượng thành công, tổng số)
        """
        try:
            # Đọc dữ liệu từ file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Chuyển đổi thành list nếu là dict
            if isinstance(data, dict):
                data = [data]
            
            # Lưu vào Elasticsearch
            return self.bulk_index(index_name, data, id_field)
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu dữ liệu từ file {file_path} vào index {index_name}: {e}")
            return (0, 0)
    
    def index_from_directory(self, index_name, directory, pattern='*.json', id_field=None):
        """
        Lưu dữ liệu từ tất cả các file trong thư mục vào Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            directory (str): Đường dẫn đến thư mục chứa file dữ liệu
            pattern (str): Mẫu tên file
            id_field (str, optional): Tên trường dùng làm ID
            
        Returns:
            tuple: (Số lượng thành công, tổng số)
        """
        try:
            import glob
            
            # Tìm tất cả các file phù hợp
            file_pattern = os.path.join(directory, pattern)
            files = glob.glob(file_pattern)
            
            if not files:
                logger.warning(f"Không tìm thấy file nào phù hợp với mẫu {file_pattern}")
                return (0, 0)
            
            # Lưu dữ liệu từ từng file
            total_success, total_count = 0, 0
            for file_path in files:
                success, count = self.index_from_file(index_name, file_path, id_field)
                total_success += success
                total_count += count
            
            logger.info(f"Đã lưu {total_success}/{total_count} document từ {len(files)} file vào index {index_name}")
            return (total_success, total_count)
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu dữ liệu từ thư mục {directory} vào index {index_name}: {e}")
            return (0, 0)
    
    def setup_product_index(self):
        """
        Thiết lập index cho dữ liệu sản phẩm.
        
        Returns:
            bool: True nếu thiết lập thành công, False nếu thất bại
        """
        # Định nghĩa mappings cho index sản phẩm
        mappings = {
            "properties": {
                "name": {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword"}}},
                "price": {"type": "text"},
                "price_numeric": {"type": "double"},
                "rating": {"type": "float"},
                "sold": {"type": "text"},
                "sold_numeric": {"type": "double"},
                "location": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "url": {"type": "keyword"},
                "product_id": {"type": "keyword"},
                "popularity_score": {"type": "float"},
                "processed_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"}
            }
        }
        
        # Tạo index
        success = self.create_index('products', mappings)
        
        if success and not self._simulate_es:
            # Lưu dữ liệu từ thư mục batch_processed
            products_dir = os.path.join(self.local_data_dir, "products_batch")
            if os.path.exists(products_dir):
                self.index_from_directory('products', products_dir, 'products_batch_*.json', 'product_id')
        
        return success
    
    def setup_review_index(self):
        """
        Thiết lập index cho dữ liệu đánh giá.
        
        Returns:
            bool: True nếu thiết lập thành công, False nếu thất bại
        """
        # Định nghĩa mappings cho index đánh giá
        mappings = {
            "properties": {
                "username": {"type": "keyword"},
                "rating": {"type": "integer"},
                "comment": {"type": "text", "analyzer": "standard"},
                "comment_length": {"type": "integer"},
                "time": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "product_id": {"type": "keyword"},
                "sentiment_score": {"type": "float"},
                "review_weight": {"type": "float"},
                "processed_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"}
            }
        }
        
        # Tạo index
        success = self.create_index('reviews', mappings)
        
        if success and not self._simulate_es:
            # Lưu dữ liệu từ thư mục batch_processed
            reviews_dir = os.path.join(self.local_data_dir, "reviews_batch")
            if os.path.exists(reviews_dir):
                self.index_from_directory('reviews', reviews_dir, 'reviews_batch_*.json')
        
        return success
    
    def setup_user_behavior_index(self):
        """
        Thiết lập index cho dữ liệu hành vi người dùng.
        
        Returns:
            bool: True nếu thiết lập thành công, False nếu thất bại
        """
        # Định nghĩa mappings cho index hành vi người dùng
        mappings = {
            "properties": {
                "user_id": {"type": "keyword"},
                "product_id": {"type": "keyword"},
                "action_frequency": {"type": "integer"},
                "interaction_score": {"type": "float"},
                "processed_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"}
            }
        }
        
        # Tạo index
        success = self.create_index('user_behavior', mappings)
        
        if success and not self._simulate_es:
            # Lưu dữ liệu từ thư mục batch_processed
            behavior_dir = os.path.join(self.local_data_dir, "user_behavior_batch")
            if os.path.exists(behavior_dir):
                self.index_from_directory('user_behavior', behavior_dir, 'user_behavior_batch_*.json')
        
        return success
    
    def setup_recommendation_index(self):
        """
        Thiết lập index cho dữ liệu gợi ý sản phẩm.
        
        Returns:
            bool: True nếu thiết lập thành công, False nếu thất bại
        """
        # Định nghĩa mappings cho index gợi ý sản phẩm
        mappings = {
            "properties": {
                "user_id": {"type": "keyword"},
                "product_id": {"type": "keyword"},
                "name": {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword"}}},
                "price": {"type": "text"},
                "rating": {"type": "float"},
                "interaction_score": {"type": "float"},
                "popularity_score": {"type": "float"},
                "recommendation_score": {"type": "float"},
                "processed_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"}
            }
        }
        
        # Tạo index
        success = self.create_index('recommendations', mappings)
        
        if success and not self._simulate_es:
            # Lưu dữ liệu từ thư mục batch_processed
            recommendations_dir = os.path.join(self.local_data_dir, "product_recommendations")
            if os.path.exists(recommendations_dir):
                self.index_from_directory('recommendations', recommendations_dir, 'product_recommendations_*.json')
        
        return success
    
    def setup_all_indices(self):
        """
        Thiết lập tất cả các index.
        
        Returns:
            bool: True nếu thiết lập thành công, False nếu thất bại
        """
        products_success = self.setup_product_index()
        reviews_success = self.setup_review_index()
        behavior_success = self.setup_user_behavior_index()
        recommendations_success = self.setup_recommendation_index()
        
        overall_success = products_success and reviews_success and behavior_success and recommendations_success
        
        if overall_success:
            logger.info("Đã thiết lập tất cả các index thành công")
        else:
            logger.warning("Thiết lập index hoàn thành với một số lỗi")
        
        return overall_success
    
    def search_products(self, query_text, size=10):
        """
        Tìm kiếm sản phẩm theo text.
        
        Args:
            query_text (str): Từ khóa tìm kiếm
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách sản phẩm
        """
        # Tạo query
        query = {
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": ["name^3", "location"]
                }
            },
            "sort": [
                {"popularity_score": {"order": "desc"}},
                "_score"
            ]
        }
        
        # Thực hiện tìm kiếm
        return self.search('products', query, size)
    
    def get_product_recommendations(self, user_id, size=10):
        """
        Lấy gợi ý sản phẩm cho người dùng.
        
        Args:
            user_id (str): ID của người dùng
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách sản phẩm được gợi ý
        """
        # Tạo query
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"user_id": user_id}}
                    ]
                }
            },
            "sort": [
                {"recommendation_score": {"order": "desc"}}
            ]
        }
        
        # Thực hiện tìm kiếm
        return self.search('recommendations', query, size)
    
    def get_product_reviews(self, product_id, size=10):
        """
        Lấy đánh giá của sản phẩm.
        
        Args:
            product_id (str): ID của sản phẩm
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách đánh giá
        """
        # Tạo query
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"product_id": product_id}}
                    ]
                }
            },
            "sort": [
                {"review_weight": {"order": "desc"}},
                {"time": {"order": "desc"}}
            ]
        }
        
        # Thực hiện tìm kiếm
        return self.search('reviews', query, size)
    
    def get_user_behavior(self, user_id, size=10):
        """
        Lấy hành vi của người dùng.
        
        Args:
            user_id (str): ID của người dùng
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách hành vi
        """
        # Tạo query
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"user_id": user_id}}
                    ]
                }
            },
            "sort": [
                {"interaction_score": {"order": "desc"}}
            ]
        }
        
        # Thực hiện tìm kiếm
        return self.search('user_behavior', query, size)
    
    def _simulate_create_index(self, index_name, mappings=None):
        """
        Mô phỏng tạo Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            mappings (dict, optional): Cấu trúc mappings cho index
            
        Returns:
            bool: True
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        logger.info(f"Đã mô phỏng tạo index {full_index_name}")
        return True
    
    def _simulate_delete_index(self, index_name):
        """
        Mô phỏng xóa Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            
        Returns:
            bool: True
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        logger.info(f"Đã mô phỏng xóa index {full_index_name}")
        return True
    
    def _simulate_index_document(self, index_name, doc_id, document):
        """
        Mô phỏng lưu document vào Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            document (dict): Nội dung document
            
        Returns:
            bool: True
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        # Tạo thư mục mô phỏng nếu chưa tồn tại
        sim_dir = os.path.join(self.local_data_dir, "es_sim", full_index_name)
        if not os.path.exists(sim_dir):
            os.makedirs(sim_dir)
        
        # Lưu document vào file
        doc_file = os.path.join(sim_dir, f"{doc_id}.json")
        with open(doc_file, 'w', encoding='utf-8') as f:
            json.dump(document, f, ensure_ascii=False, indent=4)
        
        logger.debug(f"Đã mô phỏng lưu document {doc_id} vào index {full_index_name}")
        return True
    
    def _simulate_bulk_index(self, index_name, documents, id_field=None):
        """
        Mô phỏng lưu nhiều document vào Elasticsearch index.
        
        Args:
            index_name (str): Tên index
            documents (list): Danh sách các document
            id_field (str, optional): Tên trường dùng làm ID
            
        Returns:
            tuple: (Số lượng thành công, tổng số)
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        # Tạo thư mục mô phỏng nếu chưa tồn tại
        sim_dir = os.path.join(self.local_data_dir, "es_sim", full_index_name)
        if not os.path.exists(sim_dir):
            os.makedirs(sim_dir)
        
        # Lưu từng document
        success_count = 0
        for i, doc in enumerate(documents):
            try:
                # Xác định ID
                if id_field and id_field in doc:
                    doc_id = doc[id_field]
                else:
                    doc_id = i
                
                # Lưu document vào file
                doc_file = os.path.join(sim_dir, f"{doc_id}.json")
                with open(doc_file, 'w', encoding='utf-8') as f:
                    json.dump(doc, f, ensure_ascii=False, indent=4)
                
                success_count += 1
            except Exception as e:
                logger.error(f"Lỗi khi mô phỏng lưu document: {e}")
        
        logger.info(f"Đã mô phỏng lưu {success_count}/{len(documents)} document vào index {full_index_name}")
        return (success_count, len(documents))
    
    def _simulate_search(self, index_name, query, size=10):
        """
        Mô phỏng tìm kiếm trên Elasticsearch.
        
        Args:
            index_name (str): Tên index
            query (dict): Query DSL
            size (int): Số lượng kết quả tối đa
            
        Returns:
            list: Danh sách kết quả mô phỏng
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        # Thư mục mô phỏng
        sim_dir = os.path.join(self.local_data_dir, "es_sim", full_index_name)
        
        # Tạo kết quả mô phỏng
        results = []
        
        # Nếu là tìm kiếm sản phẩm
        if index_name == 'products' or full_index_name == self.indices['products']:
            # Tạo dữ liệu mẫu
            sample_products = [
                {
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "price_numeric": 15990000.0,
                    "rating": 4.8,
                    "sold": "1k2",
                    "sold_numeric": 1200.0,
                    "location": "TP. Hồ Chí Minh",
                    "url": "https://shopee.vn/product/123456789",
                    "product_id": "123456789",
                    "popularity_score": 5.76,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "price_numeric": 29990000.0,
                    "rating": 4.9,
                    "sold": "2k5",
                    "sold_numeric": 2500.0,
                    "location": "Hà Nội",
                    "url": "https://shopee.vn/product/987654321",
                    "product_id": "987654321",
                    "popularity_score": 12.25,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            results = sample_products[:size]
        
        # Nếu là tìm kiếm gợi ý sản phẩm
        elif index_name == 'recommendations' or full_index_name == self.indices['recommendations']:
            # Tạo dữ liệu mẫu
            sample_recommendations = [
                {
                    "user_id": "user123",
                    "product_id": "123456789",
                    "name": "Samsung Galaxy S21",
                    "price": "15.990.000đ",
                    "rating": 4.8,
                    "interaction_score": 9.0,
                    "popularity_score": 5.76,
                    "recommendation_score": 51.84,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                },
                {
                    "user_id": "user123",
                    "product_id": "987654321",
                    "name": "iPhone 13 Pro Max",
                    "price": "29.990.000đ",
                    "rating": 4.9,
                    "interaction_score": 1.0,
                    "popularity_score": 12.25,
                    "recommendation_score": 12.25,
                    "processed_date": datetime.now().strftime("%Y-%m-%d")
                }
            ]
            
            # Lọc theo user_id nếu có trong query
            user_id = None
            if 'query' in query and 'bool' in query['query'] and 'must' in query['query']['bool']:
                for condition in query['query']['bool']['must']:
                    if 'term' in condition and 'user_id' in condition['term']:
                        user_id = condition['term']['user_id']
            
            if user_id:
                results = [r for r in sample_recommendations if r['user_id'] == user_id][:size]
            else:
                results = sample_recommendations[:size]
        
        # Nếu là tìm kiếm đánh giá
        elif index_name == 'reviews' or full_index_name == self.indices['reviews']:
            # Tạo dữ liệu mẫu
            sample_reviews = [
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
            
            # Lọc theo product_id nếu có trong query
            product_id = None
            if 'query' in query and 'bool' in query['query'] and 'must' in query['query']['bool']:
                for condition in query['query']['bool']['must']:
                    if 'term' in condition and 'product_id' in condition['term']:
                        product_id = condition['term']['product_id']
            
            if product_id:
                results = [r for r in sample_reviews if r['product_id'] == product_id][:size]
            else:
                results = sample_reviews[:size]
        
        # Nếu là tìm kiếm hành vi người dùng
        elif index_name == 'user_behavior' or full_index_name == self.indices['user_behavior']:
            # Tạo dữ liệu mẫu
            sample_behaviors = [
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
                }
            ]
            
            # Lọc theo user_id nếu có trong query
            user_id = None
            if 'query' in query and 'bool' in query['query'] and 'must' in query['query']['bool']:
                for condition in query['query']['bool']['must']:
                    if 'term' in condition and 'user_id' in condition['term']:
                        user_id = condition['term']['user_id']
            
            if user_id:
                results = [r for r in sample_behaviors if r['user_id'] == user_id][:size]
            else:
                results = sample_behaviors[:size]
        
        logger.info(f"Đã mô phỏng tìm kiếm và trả về {len(results)} kết quả từ index {full_index_name}")
        return results
    
    def _simulate_get_document(self, index_name, doc_id):
        """
        Mô phỏng lấy document từ Elasticsearch.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            
        Returns:
            dict: Document mô phỏng
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        # Thư mục mô phỏng
        sim_dir = os.path.join(self.local_data_dir, "es_sim", full_index_name)
        doc_file = os.path.join(sim_dir, f"{doc_id}.json")
        
        # Kiểm tra file tồn tại
        if os.path.exists(doc_file):
            try:
                with open(doc_file, 'r', encoding='utf-8') as f:
                    document = json.load(f)
                return document
            except Exception as e:
                logger.error(f"Lỗi khi đọc file mô phỏng: {e}")
                return None
        
        # Tạo document mô phỏng nếu không tìm thấy
        if index_name == 'products' or full_index_name == self.indices['products']:
            return {
                "name": "Samsung Galaxy S21",
                "price": "15.990.000đ",
                "price_numeric": 15990000.0,
                "rating": 4.8,
                "sold": "1k2",
                "sold_numeric": 1200.0,
                "location": "TP. Hồ Chí Minh",
                "url": "https://shopee.vn/product/123456789",
                "product_id": doc_id,
                "popularity_score": 5.76,
                "processed_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        logger.warning(f"Không tìm thấy document {doc_id} trong index {full_index_name}")
        return None
    
    def _simulate_delete_document(self, index_name, doc_id):
        """
        Mô phỏng xóa document từ Elasticsearch.
        
        Args:
            index_name (str): Tên index
            doc_id (str): ID của document
            
        Returns:
            bool: True
        """
        # Xác định tên index đầy đủ
        if index_name in self.indices:
            full_index_name = self.indices[index_name]
        else:
            full_index_name = index_name if index_name.startswith(self.index_prefix) else f"{self.index_prefix}_{index_name}"
        
        # Thư mục mô phỏng
        sim_dir = os.path.join(self.local_data_dir, "es_sim", full_index_name)
        doc_file = os.path.join(sim_dir, f"{doc_id}.json")
        
        # Xóa file nếu tồn tại
        if os.path.exists(doc_file):
            try:
                os.remove(doc_file)
                logger.info(f"Đã mô phỏng xóa document {doc_id} từ index {full_index_name}")
            except Exception as e:
                logger.error(f"Lỗi khi xóa file mô phỏng: {e}")
        
        return True


if __name__ == "__main__":
    # Ví dụ sử dụng
    es = ShopeeElasticsearch()
    
    # Thiết lập tất cả các index
    es.setup_all_indices()
    
    # Tìm kiếm sản phẩm
    products = es.search_products("samsung")
    print(f"Tìm thấy {len(products)} sản phẩm")
    
    # Lấy gợi ý sản phẩm cho người dùng
    recommendations = es.get_product_recommendations("user123")
    print(f"Có {len(recommendations)} gợi ý sản phẩm cho người dùng user123")
