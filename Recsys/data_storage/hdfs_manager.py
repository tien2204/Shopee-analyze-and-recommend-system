#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HDFS Manager cho dữ liệu Shopee

Mô-đun này quản lý việc lưu trữ dữ liệu vào HDFS trong kiến trúc Lambda.
"""

import os
import json
import logging
import subprocess
import pandas as pd
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('shopee_hdfs_manager')

class ShopeeHDFSManager:
    """
    Lớp ShopeeHDFSManager quản lý việc lưu trữ dữ liệu vào HDFS.
    """
    
    def __init__(self, hdfs_base_dir='/user/shopee', local_base_dir='../data'):
        """
        Khởi tạo HDFS Manager với các tùy chọn cấu hình.
        
        Args:
            hdfs_base_dir (str): Thư mục cơ sở trên HDFS
            local_base_dir (str): Thư mục cơ sở trên máy cục bộ
        """
        self.hdfs_base_dir = hdfs_base_dir
        self.local_base_dir = local_base_dir
        
        # Các thư mục con trên HDFS
        self.hdfs_dirs = {
            'raw': f"{hdfs_base_dir}/raw",
            'processed': f"{hdfs_base_dir}/processed",
            'products': f"{hdfs_base_dir}/products",
            'reviews': f"{hdfs_base_dir}/reviews",
            'user_behavior': f"{hdfs_base_dir}/user_behavior"
        }
        
        # Khởi tạo các thư mục trên HDFS
        self._initialize_hdfs_dirs()
    
    def _initialize_hdfs_dirs(self):
        """
        Khởi tạo các thư mục trên HDFS.
        """
        try:
            # Kiểm tra xem HDFS có sẵn không
            result = self._run_hdfs_command(['fs', '-ls', '/'])
            if result['returncode'] != 0:
                logger.warning("HDFS không khả dụng. Đang chuyển sang chế độ mô phỏng.")
                self._simulate_hdfs = True
                return
            
            self._simulate_hdfs = False
            
            # Tạo các thư mục trên HDFS
            for dir_name, dir_path in self.hdfs_dirs.items():
                self._run_hdfs_command(['fs', '-mkdir', '-p', dir_path])
                logger.info(f"Đã tạo thư mục HDFS: {dir_path}")
                
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo thư mục HDFS: {e}")
            logger.warning("Đang chuyển sang chế độ mô phỏng HDFS.")
            self._simulate_hdfs = True
    
    def _run_hdfs_command(self, args):
        """
        Chạy lệnh HDFS.
        
        Args:
            args (list): Danh sách các tham số lệnh
            
        Returns:
            dict: Kết quả thực thi lệnh
        """
        cmd = ['hdfs'] + args
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            stdout, stderr = process.communicate()
            
            result = {
                'returncode': process.returncode,
                'stdout': stdout.strip(),
                'stderr': stderr.strip(),
                'command': ' '.join(cmd)
            }
            
            if process.returncode != 0:
                logger.error(f"Lỗi khi chạy lệnh HDFS: {result['command']}")
                logger.error(f"Lỗi: {result['stderr']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi thực thi lệnh HDFS: {e}")
            return {
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'command': ' '.join(cmd)
            }
    
    def _simulate_hdfs_command(self, args, local_path=None):
        """
        Mô phỏng lệnh HDFS bằng cách thao tác trên hệ thống tệp cục bộ.
        
        Args:
            args (list): Danh sách các tham số lệnh
            local_path (str, optional): Đường dẫn cục bộ để mô phỏng
            
        Returns:
            dict: Kết quả mô phỏng lệnh
        """
        command = args[0] if args else ''
        
        try:
            # Chuyển đổi đường dẫn HDFS sang đường dẫn cục bộ
            if len(args) > 1:
                hdfs_path = args[-1]
                if hdfs_path.startswith('/user/'):
                    local_path = os.path.join(self.local_base_dir, 'hdfs_sim', hdfs_path[6:])
            
            if not local_path:
                local_path = os.path.join(self.local_base_dir, 'hdfs_sim')
            
            # Mô phỏng các lệnh HDFS
            if command == '-mkdir':
                os.makedirs(local_path, exist_ok=True)
                return {'returncode': 0, 'stdout': f"Created directory: {local_path}", 'stderr': ''}
                
            elif command == '-put':
                src_path = args[1]
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(src_path, 'rb') as src, open(local_path, 'wb') as dst:
                    dst.write(src.read())
                return {'returncode': 0, 'stdout': f"Put: {src_path} -> {local_path}", 'stderr': ''}
                
            elif command == '-get':
                dst_path = args[2]
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                with open(local_path, 'rb') as src, open(dst_path, 'wb') as dst:
                    dst.write(src.read())
                return {'returncode': 0, 'stdout': f"Get: {local_path} -> {dst_path}", 'stderr': ''}
                
            elif command == '-ls':
                if os.path.exists(local_path):
                    files = os.listdir(local_path)
                    file_list = '\n'.join(files)
                    return {'returncode': 0, 'stdout': f"Found {len(files)} items\n{file_list}", 'stderr': ''}
                else:
                    return {'returncode': 1, 'stdout': '', 'stderr': f"Path not found: {local_path}"}
                    
            elif command == '-rm':
                if os.path.exists(local_path):
                    os.remove(local_path)
                    return {'returncode': 0, 'stdout': f"Deleted: {local_path}", 'stderr': ''}
                else:
                    return {'returncode': 1, 'stdout': '', 'stderr': f"Path not found: {local_path}"}
                    
            else:
                return {'returncode': 1, 'stdout': '', 'stderr': f"Unsupported command: {command}"}
                
        except Exception as e:
            logger.error(f"Lỗi khi mô phỏng lệnh HDFS: {e}")
            return {'returncode': -1, 'stdout': '', 'stderr': str(e)}
    
    def upload_file(self, local_file, hdfs_dir='raw', hdfs_filename=None):
        """
        Tải file lên HDFS.
        
        Args:
            local_file (str): Đường dẫn đến file cục bộ
            hdfs_dir (str): Thư mục đích trên HDFS
            hdfs_filename (str, optional): Tên file trên HDFS
            
        Returns:
            bool: True nếu tải lên thành công, False nếu thất bại
        """
        try:
            # Kiểm tra file cục bộ
            if not os.path.exists(local_file):
                logger.error(f"File cục bộ không tồn tại: {local_file}")
                return False
            
            # Xác định thư mục đích trên HDFS
            if hdfs_dir in self.hdfs_dirs:
                hdfs_target_dir = self.hdfs_dirs[hdfs_dir]
            else:
                hdfs_target_dir = hdfs_dir if hdfs_dir.startswith('/') else f"{self.hdfs_base_dir}/{hdfs_dir}"
            
            # Xác định tên file trên HDFS
            if not hdfs_filename:
                hdfs_filename = os.path.basename(local_file)
            
            # Đường dẫn đầy đủ trên HDFS
            hdfs_path = f"{hdfs_target_dir}/{hdfs_filename}"
            
            # Tải file lên HDFS
            if self._simulate_hdfs:
                # Mô phỏng tải lên HDFS
                local_target = os.path.join(self.local_base_dir, 'hdfs_sim', hdfs_target_dir[6:], hdfs_filename)
                os.makedirs(os.path.dirname(local_target), exist_ok=True)
                with open(local_file, 'rb') as src, open(local_target, 'wb') as dst:
                    dst.write(src.read())
                logger.info(f"Đã mô phỏng tải file lên HDFS: {local_file} -> {hdfs_path}")
                return True
            else:
                # Tải lên HDFS thực tế
                result = self._run_hdfs_command(['fs', '-put', local_file, hdfs_path])
                if result['returncode'] == 0:
                    logger.info(f"Đã tải file lên HDFS: {local_file} -> {hdfs_path}")
                    return True
                else:
                    logger.error(f"Lỗi khi tải file lên HDFS: {result['stderr']}")
                    return False
                
        except Exception as e:
            logger.error(f"Lỗi khi tải file lên HDFS: {e}")
            return False
    
    def download_file(self, hdfs_file, local_dir, local_filename=None):
        """
        Tải file từ HDFS về máy cục bộ.
        
        Args:
            hdfs_file (str): Đường dẫn đến file trên HDFS
            local_dir (str): Thư mục đích trên máy cục bộ
            local_filename (str, optional): Tên file trên máy cục bộ
            
        Returns:
            bool: True nếu tải về thành công, False nếu thất bại
        """
        try:
            # Xác định đường dẫn đầy đủ trên HDFS
            if not hdfs_file.startswith('/'):
                hdfs_file = f"{self.hdfs_base_dir}/{hdfs_file}"
            
            # Xác định tên file cục bộ
            if not local_filename:
                local_filename = os.path.basename(hdfs_file)
            
            # Tạo thư mục cục bộ nếu chưa tồn tại
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            
            # Đường dẫn đầy đủ trên máy cục bộ
            local_path = os.path.join(local_dir, local_filename)
            
            # Tải file từ HDFS
            if self._simulate_hdfs:
                # Mô phỏng tải về từ HDFS
                hdfs_sim_path = os.path.join(self.local_base_dir, 'hdfs_sim', hdfs_file[6:])
                if os.path.exists(hdfs_sim_path):
                    with open(hdfs_sim_path, 'rb') as src, open(local_path, 'wb') as dst:
                        dst.write(src.read())
                    logger.info(f"Đã mô phỏng tải file từ HDFS: {hdfs_file} -> {local_path}")
                    return True
                else:
                    logger.error(f"File không tồn tại trong HDFS mô phỏng: {hdfs_file}")
                    return False
            else:
                # Tải về từ HDFS thực tế
                result = self._run_hdfs_command(['fs', '-get', hdfs_file, local_path])
                if result['returncode'] == 0:
                    logger.info(f"Đã tải file từ HDFS: {hdfs_file} -> {local_path}")
                    return True
                else:
                    logger.error(f"Lỗi khi tải file từ HDFS: {result['stderr']}")
                    return False
                
        except Exception as e:
            logger.error(f"Lỗi khi tải file từ HDFS: {e}")
            return False
    
    def list_files(self, hdfs_dir='raw'):
        """
        Liệt kê các file trong thư mục HDFS.
        
        Args:
            hdfs_dir (str): Thư mục trên HDFS
            
        Returns:
            list: Danh sách các file
        """
        try:
            # Xác định đường dẫn đầy đủ trên HDFS
            if hdfs_dir in self.hdfs_dirs:
                hdfs_path = self.hdfs_dirs[hdfs_dir]
            else:
                hdfs_path = hdfs_dir if hdfs_dir.startswith('/') else f"{self.hdfs_base_dir}/{hdfs_dir}"
            
            # Liệt kê các file
            if self._simulate_hdfs:
                # Mô phỏng liệt kê file từ HDFS
                hdfs_sim_path = os.path.join(self.local_base_dir, 'hdfs_sim', hdfs_path[6:])
                if os.path.exists(hdfs_sim_path):
                    files = os.listdir(hdfs_sim_path)
                    logger.info(f"Đã mô phỏng liệt kê {len(files)} file từ HDFS: {hdfs_path}")
                    return files
                else:
                    logger.warning(f"Thư mục không tồn tại trong HDFS mô phỏng: {hdfs_path}")
                    return []
            else:
                # Liệt kê file từ HDFS thực tế
                result = self._run_hdfs_command(['fs', '-ls', hdfs_path])
                if result['returncode'] == 0:
                    # Phân tích kết quả để lấy danh sách file
                    files = []
                    for line in result['stdout'].split('\n'):
                        if line and not line.startswith('Found'):
                            parts = line.split()
                            if len(parts) >= 8:
                                files.append(parts[-1].split('/')[-1])
                    
                    logger.info(f"Đã liệt kê {len(files)} file từ HDFS: {hdfs_path}")
                    return files
                else:
                    logger.warning(f"Lỗi khi liệt kê file từ HDFS: {result['stderr']}")
                    return []
                
        except Exception as e:
            logger.error(f"Lỗi khi liệt kê file từ HDFS: {e}")
            return []
    
    def delete_file(self, hdfs_file):
        """
        Xóa file trên HDFS.
        
        Args:
            hdfs_file (str): Đường dẫn đến file trên HDFS
            
        Returns:
            bool: True nếu xóa thành công, False nếu thất bại
        """
        try:
            # Xác định đường dẫn đầy đủ trên HDFS
            if not hdfs_file.startswith('/'):
                hdfs_file = f"{self.hdfs_base_dir}/{hdfs_file}"
            
            # Xóa file
            if self._simulate_hdfs:
                # Mô phỏng xóa file từ HDFS
                hdfs_sim_path = os.path.join(self.local_base_dir, 'hdfs_sim', hdfs_file[6:])
                if os.path.exists(hdfs_sim_path):
                    os.remove(hdfs_sim_path)
                    logger.info(f"Đã mô phỏng xóa file từ HDFS: {hdfs_file}")
                    return True
                else:
                    logger.warning(f"File không tồn tại trong HDFS mô phỏng: {hdfs_file}")
                    return False
            else:
                # Xóa file từ HDFS thực tế
                result = self._run_hdfs_command(['fs', '-rm', hdfs_file])
                if result['returncode'] == 0:
                    logger.info(f"Đã xóa file từ HDFS: {hdfs_file}")
                    return True
                else:
                    logger.error(f"Lỗi khi xóa file từ HDFS: {result['stderr']}")
                    return False
                
        except Exception as e:
            logger.error(f"Lỗi khi xóa file từ HDFS: {e}")
            return False
    
    def upload_json_data(self, data, hdfs_dir='raw', filename=None):
        """
        Tải dữ liệu JSON lên HDFS.
        
        Args:
            data (dict or list): Dữ liệu JSON
            hdfs_dir (str): Thư mục đích trên HDFS
            filename (str, optional): Tên file trên HDFS
            
        Returns:
            bool: True nếu tải lên thành công, False nếu thất bại
        """
        try:
            # Tạo tên file nếu chưa có
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data_{timestamp}.json"
            
            # Tạo file tạm thời
            temp_file = os.path.join(self.local_base_dir, filename)
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            # Tải lên HDFS
            result = self.upload_file(temp_file, hdfs_dir, filename)
            
            # Xóa file tạm thời
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu JSON lên HDFS: {e}")
            return False
    
    def upload_dataframe(self, df, hdfs_dir='raw', filename=None, format='csv'):
        """
        Tải DataFrame lên HDFS.
        
        Args:
            df (pandas.DataFrame): DataFrame cần tải lên
            hdfs_dir (str): Thư mục đích trên HDFS
            filename (str, optional): Tên file trên HDFS
            format (str): Định dạng file ('csv' hoặc 'json')
            
        Returns:
            bool: True nếu tải lên thành công, False nếu thất bại
        """
        try:
            # Tạo tên file nếu chưa có
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data_{timestamp}.{format}"
            
            # Tạo file tạm thời
            temp_file = os.path.join(self.local_base_dir, filename)
            
            # Lưu DataFrame theo định dạng
            if format.lower() == 'csv':
                df.to_csv(temp_file, index=False, encoding='utf-8-sig')
            elif format.lower() == 'json':
                df.to_json(temp_file, orient='records', force_ascii=False, indent=4)
            else:
                logger.error(f"Định dạng không được hỗ trợ: {format}")
                return False
            
            # Tải lên HDFS
            result = self.upload_file(temp_file, hdfs_dir, filename)
            
            # Xóa file tạm thời
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi tải DataFrame lên HDFS: {e}")
            return False


if __name__ == "__main__":
    # Ví dụ sử dụng
    hdfs_manager = ShopeeHDFSManager()
    
    # Tạo dữ liệu mẫu
    sample_data = [
        {
            "name": "Samsung Galaxy S21",
            "price": "15.990.000đ",
            "rating": 4.8,
            "sold": "1k2",
            "location": "TP. Hồ Chí Minh"
        },
        {
            "name": "iPhone 13 Pro Max",
            "price": "29.990.000đ",
            "rating": 4.9,
            "sold": "2k5",
            "location": "Hà Nội"
        }
    ]
    
    # Tải dữ liệu lên HDFS
    hdfs_manager.upload_json_data(sample_data, 'products', 'sample_products.json')
    
    # Tạo DataFrame và tải lên HDFS
    df = pd.DataFrame(sample_data)
    hdfs_manager.upload_dataframe(df, 'products', 'sample_products.csv')
