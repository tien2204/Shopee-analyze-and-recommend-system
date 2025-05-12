# Trong backend/app.py
from flask import Flask, jsonify, request # Thêm request
from elasticsearch import Elasticsearch
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app) # Cho phép CORS cho tất cả các domain (có thể giới hạn nếu cần)
es_host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
es = Elasticsearch(es_host)

PRODUCTS_INDEX = "amazon_products_index" # Index chứa thông tin sản phẩm gốc
RECOMMENDATIONS_INDEX = "amazon_product_recommendations" # Index chứa gợi ý đã tính toán trước

@app.route("/")
def home():
    return "✅ Backend API is running!"

@app.route("/search") # Sửa lại để nhận keyword từ query param
def search_products():
    keyword = request.args.get("keyword", "")
    if not keyword:
        return jsonify({"error": "Keyword is required"}), 400
    
    query = {
        "query": {
            "match": {
                "title": keyword # Tìm kiếm theo trường 'title' (hoặc 'name' nếu bạn đã alias)
            }
        }
    }
    try:
        res = es.search(index=PRODUCTS_INDEX, body=query, size=10) # Tìm trên index sản phẩm chính
        hits = []
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            # Trả về các trường cơ bản cho kết quả tìm kiếm
            hits.append({
                "product_id": source.get("asin"), # Hoặc source.get("product_id") nếu đã alias
                "name": source.get("title"),    # Hoặc source.get("name")
                "price": source.get("final_price"), # Hoặc source.get("price")
                "image_url": source.get("image_url"),
                "description": source.get("description")
            })
        return jsonify(hits)
    except Exception as e:
        app.logger.error(f"Search error: {e}")
        return jsonify({"error": "Search failed", "details": str(e)}), 500

@app.route("/recommend/<product_asin>")
def get_recommendations(product_asin):
    try:
        # Lấy document gợi ý từ index recommendations, ID của doc là ASIN của sản phẩm gốc
        resp = es.get(index=RECOMMENDATIONS_INDEX, id=product_asin)
        
        if resp and resp.get('found') and '_source' in resp:
            recommendation_data = resp['_source']
            # recommendation_data['recommendations'] là một list các dict, mỗi dict là một sản phẩm gợi ý
            # [{"recommended_asin": "ASIN2", "name": "Product Name 2", "price": 123.45, "similarity": 0.85}, ...]
            # Đã có sẵn thông tin chi tiết cơ bản, không cần query lại trừ khi muốn thêm thông tin
            return jsonify(recommendation_data.get("recommendations", []))
        else:
            return jsonify({"message": "No recommendations found for this product or product does not exist in recommendations index."}), 404
    except Exception as e:
        app.logger.error(f"Recommendation error for {product_asin}: {e}")
        return jsonify({"error": "Could not retrieve recommendations", "details": str(e)}), 500
    
@app.route("/products")
def get_products():
    limit = request.args.get("limit", 5, type=int)
    try:
        query = {
            "query": {
                "match_all": {}
            }
        }
        res = es.search(index=PRODUCTS_INDEX, body=query, size=limit) # Tìm trên index sản phẩm chính
        hits = []
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            # Trả về các trường cơ bản cho kết quả tìm kiếm
            hits.append({
                "product_id": source.get("asin"), # Hoặc source.get("product_id") nếu đã alias
                "name": source.get("title"),    # Hoặc source.get("name")
                "price": source.get("final_price"), # Hoặc source.get("price")
                "image_url": source.get("image_url"),
                "description": source.get("description")
            })
        return jsonify(hits)
    except Exception as e:
        app.logger.error(f"Search error: {e}")
        return jsonify({"error": "Search failed", "details": str(e)}), 500    

if __name__ == "__main__":
    # Cân nhắc dùng Gunicorn cho production thay vì Flask dev server
    app.run(host='0.0.0.0', port=5000, debug=True) # Thêm debug=True khi phát triển