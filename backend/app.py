# Trong backend/app.py
from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch, NotFoundError # Thêm NotFoundError
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}}) # Cho phép origin của frontend

es_host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
es = Elasticsearch(es_host)

PRODUCTS_INDEX = "amazon_products_index"
RECOMMENDATIONS_INDEX = "amazon_product_recommendations"
ITEMS_PER_PAGE = 10 # Số sản phẩm mỗi trang, bạn có thể thay đổi

@app.route("/")
def home():
    return "✅ Backend API is running!"

@app.route("/products")
def get_products():
    # Lấy trang hiện tại, mặc định là trang 1
    page = request.args.get("page", 1, type=int)
    # Lấy số lượng item mỗi trang, có thể cho phép client override nếu muốn, hoặc cố định
    limit = request.args.get("limit", ITEMS_PER_PAGE, type=int) 
    
    # Tính toán vị trí bắt đầu (from) cho Elasticsearch
    from_value = (page - 1) * limit

    try:
        query = {
            "query": {
                "match_all": {}
            }
        }
        # Lấy tổng số sản phẩm để tính tổng số trang
        count_response = es.count(index=PRODUCTS_INDEX, body={"query": query["query"]})
        total_products = count_response['count']
        total_pages = (total_products + limit - 1) // limit # Công thức tính tổng số trang

        # Thực hiện tìm kiếm với from và size
        res = es.search(index=PRODUCTS_INDEX, body=query, from_=from_value, size=limit)
        
        hits = []
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            hits.append({
                "product_id": source.get("asin"),
                "name": source.get("title"),
                "price": source.get("final_price"),
                "image_url": source.get("image_url"),
                "description": source.get("description") # Đảm bảo trường này có trong ES
            })
        
        # Trả về cả danh sách sản phẩm và thông tin phân trang
        return jsonify({
            "products": hits,
            "total_products": total_products,
            "total_pages": total_pages,
            "current_page": page,
            "per_page": limit
        })
    except Exception as e:
        app.logger.error(f"Error fetching products: {e}")
        return jsonify({"error": "Failed to fetch products", "details": str(e)}), 500

@app.route("/search")
def search_products():
    keyword = request.args.get("keyword", "")
    page = request.args.get("page", 1, type=int) # Thêm page cho search
    limit = request.args.get("limit", ITEMS_PER_PAGE, type=int)
    from_value = (page - 1) * limit

    if not keyword:
        return jsonify({"error": "Keyword is required"}), 400
    
    query = {
        "query": {
            "match": {
                "title": keyword 
            }
        }
    }
    try:
        count_response = es.count(index=PRODUCTS_INDEX, body={"query": query["query"]})
        total_products = count_response['count']
        total_pages = (total_products + limit - 1) // limit

        res = es.search(index=PRODUCTS_INDEX, body=query, from_=from_value, size=limit)
        hits = []
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            hits.append({
                "product_id": source.get("asin"),
                "name": source.get("title"),
                "price": source.get("final_price"),
                "image_url": source.get("image_url"),
                "description": source.get("description")
            })
        return jsonify({
            "products": hits,
            "total_products": total_products,
            "total_pages": total_pages,
            "current_page": page,
            "per_page": limit
        })
    except Exception as e:
        app.logger.error(f"Search error: {e}")
        return jsonify({"error": "Search failed", "details": str(e)}), 500

@app.route("/recommend/<product_asin>")
def get_recommendations(product_asin):
    try:
        resp = es.get(index=RECOMMENDATIONS_INDEX, id=product_asin)
        recommendation_data = resp['_source']
        return jsonify(recommendation_data.get("recommendations", []))
    except NotFoundError:
        app.logger.info(f"No recommendations found in ES for ASIN: {product_asin}")
        return jsonify({"message": "No recommendations found for this product."}), 404
    except Exception as e:
        app.logger.error(f"Recommendation error for {product_asin}: {e}")
        return jsonify({"error": "Could not retrieve recommendations", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
