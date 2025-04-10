#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shopee Web Scraper

Mô-đun này thu thập dữ liệu sản phẩm từ Shopee sử dụng BeautifulSoup và Selenium.
Dữ liệu được thu thập bao gồm thông tin sản phẩm, đánh giá, và phản hồi từ người dùng.
"""

import os
import json
import time
import random
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class ShopeeScraper:
    """
    Lớp ShopeeScraper cung cấp các phương thức để thu thập dữ liệu từ Shopee.
    """
    
    def __init__(self, headless=True, output_dir='data'):
        """
        Khởi tạo scraper với các tùy chọn cấu hình.
        
        Args:
            headless (bool): Chạy trình duyệt ở chế độ headless hay không
            output_dir (str): Thư mục lưu trữ dữ liệu thu thập được
        """
        self.base_url = "https://shopee.vn"
        self.output_dir = output_dir
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Cấu hình Selenium WebDriver
        self.setup_webdriver(headless)
    
    def setup_webdriver(self, headless=True):
        """
        Cấu hình và khởi tạo Selenium WebDriver.
        
        Args:
            headless (bool): Chạy trình duyệt ở chế độ headless hay không
        """
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument(f'user-agent={self.headers["User-Agent"]}')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_window_size(1920, 1080)
            print("WebDriver đã được khởi tạo thành công.")
        except Exception as e:
            print(f"Lỗi khi khởi tạo WebDriver: {e}")
            # Thử sử dụng Firefox nếu Chrome không khả dụng
            try:
                from selenium.webdriver.firefox.options import Options as FirefoxOptions
                firefox_options = FirefoxOptions()
                if headless:
                    firefox_options.add_argument('--headless')
                self.driver = webdriver.Firefox(options=firefox_options)
                self.driver.set_window_size(1920, 1080)
                print("Đã chuyển sang sử dụng Firefox WebDriver.")
            except Exception as e2:
                print(f"Không thể khởi tạo WebDriver: {e2}")
                raise
    
    def search_products(self, keyword, num_pages=1):
        """
        Tìm kiếm sản phẩm theo từ khóa và thu thập thông tin cơ bản.
        
        Args:
            keyword (str): Từ khóa tìm kiếm
            num_pages (int): Số trang kết quả cần thu thập
            
        Returns:
            list: Danh sách thông tin sản phẩm
        """
        products = []
        search_url = f"{self.base_url}/search?keyword={keyword}"
        
        try:
            self.driver.get(search_url)
            print(f"Đang tìm kiếm sản phẩm với từ khóa: {keyword}")
            
            # Đợi trang tải xong
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".shopee-search-item-result__item"))
            )
            
            # Cuộn trang để tải thêm sản phẩm
            self._scroll_page()
            
            for page in range(num_pages):
                print(f"Đang thu thập dữ liệu từ trang {page + 1}/{num_pages}")
                
                # Lấy HTML của trang hiện tại
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Tìm tất cả các sản phẩm trên trang
                product_items = soup.select(".shopee-search-item-result__item")
                
                for item in product_items:
                    try:
                        # Trích xuất thông tin sản phẩm
                        product_data = self._extract_product_info(item)
                        if product_data:
                            products.append(product_data)
                    except Exception as e:
                        print(f"Lỗi khi trích xuất thông tin sản phẩm: {e}")
                
                # Chuyển sang trang tiếp theo nếu cần
                if page < num_pages - 1:
                    try:
                        next_button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, ".shopee-icon-button--right"))
                        )
                        next_button.click()
                        time.sleep(random.uniform(1, 3))  # Đợi trang tải
                        self._scroll_page()
                    except Exception as e:
                        print(f"Không thể chuyển sang trang tiếp theo: {e}")
                        break
            
            # Lưu dữ liệu vào file
            self._save_data(products, f"{keyword}_products.json")
            
            return products
            
        except Exception as e:
            print(f"Lỗi khi tìm kiếm sản phẩm: {e}")
            return []
    
    def get_product_details(self, product_url):
        """
        Thu thập thông tin chi tiết của một sản phẩm.
        
        Args:
            product_url (str): URL của sản phẩm
            
        Returns:
            dict: Thông tin chi tiết của sản phẩm
        """
        try:
            self.driver.get(product_url)
            print(f"Đang thu thập thông tin chi tiết sản phẩm: {product_url}")
            
            # Đợi trang tải xong
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".product-briefing"))
            )
            
            # Cuộn trang để tải thêm thông tin
            self._scroll_page()
            
            # Lấy HTML của trang
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Trích xuất thông tin chi tiết sản phẩm
            product_details = {}
            
            # Tên sản phẩm
            try:
                product_details['name'] = soup.select_one(".product-briefing .product-title").text.strip()
            except:
                product_details['name'] = "N/A"
            
            # Giá sản phẩm
            try:
                price_element = soup.select_one(".product-briefing .product-price")
                product_details['price'] = price_element.text.strip() if price_element else "N/A"
            except:
                product_details['price'] = "N/A"
            
            # Đánh giá sản phẩm
            try:
                rating_element = soup.select_one(".product-rating-overview__rating-score")
                product_details['rating'] = rating_element.text.strip() if rating_element else "N/A"
            except:
                product_details['rating'] = "N/A"
            
            # Số lượng đánh giá
            try:
                rating_count_element = soup.select_one(".product-rating-overview__rating-total")
                product_details['rating_count'] = rating_count_element.text.strip() if rating_count_element else "N/A"
            except:
                product_details['rating_count'] = "N/A"
            
            # Số lượng đã bán
            try:
                sold_element = soup.select_one(".product-briefing .product-sold")
                product_details['sold'] = sold_element.text.strip() if sold_element else "N/A"
            except:
                product_details['sold'] = "N/A"
            
            # Danh mục sản phẩm
            try:
                categories = []
                category_elements = soup.select(".product-detail-breadcrumb a")
                for element in category_elements:
                    categories.append(element.text.strip())
                product_details['categories'] = categories
            except:
                product_details['categories'] = []
            
            # Mô tả sản phẩm
            try:
                description_element = soup.select_one(".product-detail .product-detail__description")
                product_details['description'] = description_element.text.strip() if description_element else "N/A"
            except:
                product_details['description'] = "N/A"
            
            # Thu thập đánh giá của người dùng
            product_details['reviews'] = self._get_product_reviews()
            
            # Lưu dữ liệu vào file
            product_id = product_url.split("-i.")[-1].split("?")[0]
            self._save_data(product_details, f"product_{product_id}_details.json")
            
            return product_details
            
        except Exception as e:
            print(f"Lỗi khi thu thập thông tin chi tiết sản phẩm: {e}")
            return {}
    
    def _get_product_reviews(self, max_reviews=50):
        """
        Thu thập đánh giá của người dùng về sản phẩm.
        
        Args:
            max_reviews (int): Số lượng đánh giá tối đa cần thu thập
            
        Returns:
            list: Danh sách đánh giá của người dùng
        """
        reviews = []
        
        try:
            # Chuyển đến tab đánh giá
            review_tab = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".product-rating-overview__filter-wrapper"))
            )
            review_tab.click()
            time.sleep(2)
            
            # Thu thập đánh giá
            for _ in range(min(max_reviews // 6 + 1, 5)):  # Mỗi trang có khoảng 6 đánh giá, tối đa 5 trang
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                review_items = soup.select(".shopee-product-rating")
                
                for item in review_items:
                    try:
                        review = {}
                        
                        # Tên người đánh giá
                        try:
                            review['username'] = item.select_one(".shopee-product-rating__author-name").text.strip()
                        except:
                            review['username'] = "N/A"
                        
                        # Số sao đánh giá
                        try:
                            rating_element = item.select(".shopee-product-rating__rating-star--active")
                            review['rating'] = len(rating_element)
                        except:
                            review['rating'] = 0
                        
                        # Nội dung đánh giá
                        try:
                            comment_element = item.select_one(".shopee-product-rating__content")
                            review['comment'] = comment_element.text.strip() if comment_element else "N/A"
                        except:
                            review['comment'] = "N/A"
                        
                        # Thời gian đánh giá
                        try:
                            time_element = item.select_one(".shopee-product-rating__time")
                            review['time'] = time_element.text.strip() if time_element else "N/A"
                        except:
                            review['time'] = "N/A"
                        
                        reviews.append(review)
                        
                        if len(reviews) >= max_reviews:
                            break
                    except Exception as e:
                        print(f"Lỗi khi trích xuất đánh giá: {e}")
                
                if len(reviews) >= max_reviews:
                    break
                
                # Chuyển sang trang đánh giá tiếp theo nếu có
                try:
                    next_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, ".shopee-icon-button--right"))
                    )
                    next_button.click()
                    time.sleep(random.uniform(1, 2))
                except:
                    break
            
            return reviews
            
        except Exception as e:
            print(f"Lỗi khi thu thập đánh giá sản phẩm: {e}")
            return []
    
    def _extract_product_info(self, product_element):
        """
        Trích xuất thông tin cơ bản của sản phẩm từ phần tử HTML.
        
        Args:
            product_element (BeautifulSoup element): Phần tử HTML chứa thông tin sản phẩm
            
        Returns:
            dict: Thông tin cơ bản của sản phẩm
        """
        product = {}
        
        try:
            # Tên sản phẩm
            name_element = product_element.select_one(".shopee-search-result-item__name")
            product['name'] = name_element.text.strip() if name_element else "N/A"
            
            # URL sản phẩm
            try:
                link_element = product_element.select_one("a")
                if link_element and 'href' in link_element.attrs:
                    product['url'] = self.base_url + link_element['href']
                else:
                    product['url'] = "N/A"
            except:
                product['url'] = "N/A"
            
            # Giá sản phẩm
            price_element = product_element.select_one(".shopee-search-result-item__price")
            product['price'] = price_element.text.strip() if price_element else "N/A"
            
            # Đánh giá sản phẩm
            rating_element = product_element.select_one(".shopee-rating-stars__stars")
            if rating_element:
                rating_stars = rating_element.select(".shopee-rating-stars__star--active")
                product['rating'] = len(rating_stars)
            else:
                product['rating'] = 0
            
            # Số lượng đã bán
            sold_element = product_element.select_one(".shopee-search-result-item__sold")
            product['sold'] = sold_element.text.strip() if sold_element else "0"
            
            # Vị trí cửa hàng
            location_element = product_element.select_one(".shopee-search-result-item__location")
            product['location'] = location_element.text.strip() if location_element else "N/A"
            
            # Thêm timestamp
            product['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return product
            
        except Exception as e:
            print(f"Lỗi khi trích xuất thông tin sản phẩm: {e}")
            return None
    
    def _scroll_page(self, scroll_pause_time=1):
        """
        Cuộn trang để tải thêm nội dung.
        
        Args:
            scroll_pause_time (float): Thời gian đợi giữa các lần cuộn
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        for _ in range(3):  # Cuộn 3 lần
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    
    def _save_data(self, data, filename):
        """
        Lưu dữ liệu vào file JSON.
        
        Args:
            data (dict or list): Dữ liệu cần lưu
            filename (str): Tên file
        """
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Đã lưu dữ liệu vào file: {filepath}")
    
    def export_to_csv(self, json_file, csv_file=None):
        """
        Chuyển đổi dữ liệu từ file JSON sang CSV.
        
        Args:
            json_file (str): Đường dẫn đến file JSON
            csv_file (str, optional): Đường dẫn đến file CSV đầu ra
        """
        if csv_file is None:
            csv_file = json_file.replace('.json', '.csv')
        
        json_path = os.path.join(self.output_dir, json_file)
        csv_path = os.path.join(self.output_dir, csv_file)
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
            
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"Đã xuất dữ liệu sang file CSV: {csv_path}")
        except Exception as e:
            print(f"Lỗi khi chuyển đổi dữ liệu sang CSV: {e}")
    
    def close(self):
        """
        Đóng WebDriver và giải phóng tài nguyên.
        """
        if hasattr(self, 'driver'):
            self.driver.quit()
            print("WebDriver đã được đóng.")


if __name__ == "__main__":
    # Ví dụ sử dụng
    scraper = ShopeeScraper(headless=True, output_dir='../data')
    
    try:
        # Tìm kiếm sản phẩm
        products = scraper.search_products("điện thoại samsung", num_pages=1)
        
        # Lấy thông tin chi tiết của sản phẩm đầu tiên (nếu có)
        if products and len(products) > 0 and 'url' in products[0]:
            product_url = products[0]['url']
            scraper.get_product_details(product_url)
        
        # Xuất dữ liệu sang CSV
        scraper.export_to_csv("điện thoại samsung_products.json")
    
    finally:
        # Đóng scraper
        scraper.close()
