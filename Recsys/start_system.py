#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tập lệnh khởi động hệ thống gợi ý sản phẩm Shopee

Mô-đun này khởi động tất cả các thành phần của hệ thống gợi ý sản phẩm Shopee.
"""

import os
import sys
import time
import json
import argparse
import subprocess
from multiprocessing import Process

# Thêm thư mục cha vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Đường dẫn đến các thành phần
COMPONENTS = {
    'web_interface': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web_interface', 'app.py'),
    'data_collection': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_collection', 'shopee_scraper.py'),
    'serving_layer': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'query_system', 'serving_layer.py')
}

# Cấu hình mặc định
DEFAULT_CONFIG = {
    'host': '0.0.0.0',
    'port': 5000,
    'debug': False,
    'threaded': True,
    'processes': 1
}

# Khởi động web interface
def start_web_interface(config=None):
    """
    Khởi động web interface.
    
    Args:
        config (dict): Cấu hình cho web interface
    
    Returns:
        Process: Process chạy web interface
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    # Tạo file cấu hình
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web_interface', 'config.json')
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    # Khởi động web interface
    cmd = f"python3 {COMPONENTS['web_interface']} --config {config_file}"
    
    process = Process(target=os.system, args=(cmd,))
    process.start()
    
    print(f"Web interface đã khởi động tại http://{config['host']}:{config['port']}")
    
    return process

# Khởi động serving layer
def start_serving_layer():
    """
    Khởi động serving layer.
    
    Returns:
        object: Đối tượng serving layer
    """
    try:
        from query_system.serving_layer import ShopeeServingLayer
        
        serving_layer = ShopeeServingLayer()
        serving_layer.start()
        
        print("Serving layer đã khởi động")
        
        return serving_layer
    except ImportError as e:
        print(f"Không thể import serving layer: {e}")
        return None

# Khởi động data collection
def start_data_collection():
    """
    Khởi động data collection.
    
    Returns:
        Process: Process chạy data collection
    """
    # Khởi động data collection
    cmd = f"python3 {COMPONENTS['data_collection']}"
    
    process = Process(target=os.system, args=(cmd,))
    process.start()
    
    print("Data collection đã khởi động")
    
    return process

# Khởi động tất cả các thành phần
def start_all(config=None):
    """
    Khởi động tất cả các thành phần.
    
    Args:
        config (dict): Cấu hình cho các thành phần
    
    Returns:
        dict: Các process và đối tượng đã khởi động
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    processes = {}
    
    # Khởi động serving layer
    serving_layer = start_serving_layer()
    processes['serving_layer'] = serving_layer
    
    # Khởi động data collection
    data_collection_process = start_data_collection()
    processes['data_collection'] = data_collection_process
    
    # Khởi động web interface
    web_interface_process = start_web_interface(config)
    processes['web_interface'] = web_interface_process
    
    return processes

# Dừng tất cả các thành phần
def stop_all(processes):
    """
    Dừng tất cả các thành phần.
    
    Args:
        processes (dict): Các process và đối tượng đã khởi động
    """
    # Dừng web interface
    if 'web_interface' in processes and processes['web_interface']:
        processes['web_interface'].terminate()
        processes['web_interface'].join()
        print("Web interface đã dừng")
    
    # Dừng data collection
    if 'data_collection' in processes and processes['data_collection']:
        processes['data_collection'].terminate()
        processes['data_collection'].join()
        print("Data collection đã dừng")
    
    # Dừng serving layer
    if 'serving_layer' in processes and processes['serving_layer']:
        processes['serving_layer'].stop()
        print("Serving layer đã dừng")

# Hàm main
def main():
    """
    Hàm main để khởi động hệ thống.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Khởi động hệ thống gợi ý sản phẩm Shopee')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host để chạy web interface')
    parser.add_argument('--port', type=int, default=5000, help='Port để chạy web interface')
    parser.add_argument('--debug', action='store_true', help='Chạy web interface trong chế độ debug')
    parser.add_argument('--threaded', action='store_true', help='Chạy web interface trong chế độ threaded')
    parser.add_argument('--processes', type=int, default=1, help='Số lượng process cho web interface')
    args = parser.parse_args()
    
    # Tạo cấu hình
    config = {
        'host': args.host,
        'port': args.port,
        'debug': args.debug,
        'threaded': args.threaded,
        'processes': args.processes
    }
    
    print("=== Khởi động hệ thống gợi ý sản phẩm Shopee ===")
    
    # Khởi động tất cả các thành phần
    processes = start_all(config)
    
    try:
        # Chờ người dùng nhấn Ctrl+C
        print("\nNhấn Ctrl+C để dừng hệ thống...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n=== Dừng hệ thống gợi ý sản phẩm Shopee ===")
        
        # Dừng tất cả các thành phần
        stop_all(processes)

if __name__ == '__main__':
    main()
