from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys # Import sys để thoát nếu có lỗi nghiêm trọng

# === Khởi tạo Spark Session ===
# Kết nối đến Elasticsearch chạy trong Docker (tên service là 'elasticsearch')
es_nodes = "elasticsearch"
es_port = "9200"
# Đặt tên index trong Elasticsearch cho san phẩm Amazon
es_index = "amazon_products_index"
# Tên file JSON nguồn bên trong container Spark
json_file_path = "/data/Amazon_Product.json"

print("Khoi tao SparkSession...")
try:
    spark = SparkSession.builder \
        .appName("IndexLaptopsToES") \
        .config("spark.es.nodes", es_nodes) \
        .config("spark.es.port", es_port) \
        .config("spark.driver.userClassPathFirst", "true") \
        .config("spark.executor.userClassPathFirst", "true") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.index.auto.create", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate() 
    print(f"Da tao SparkSession. Ket noi ES: {es_nodes}:{es_port}")
except Exception as e:
    print(f"Loi khi khoi tao SparkSession: {e}")
    sys.exit(1)


# === Đọc dữ liệu JSON ===
print(f"Dang doc file JSON tu: {json_file_path}")
try:
    # Đọc file JSON Lines (mỗi dòng một JSON object)
    df = spark.read.json(json_file_path)
    # Cache DataFrame vì có thể dùng lại (printSchema, show, write)
    df.cache()
    print("Doc du lieu thanh cong! So luong ban ghi:", df.count())
    print("Schema duoc Spark suy luan:")
    df.printSchema()
    print("Du lieu mau (1 ban ghi):")
    # Hiển thị theo chiều dọc để xem hết các trường
    df.show(1, truncate=False, vertical=True)

    # Kiểm tra xem cột product_id có tồn tại không (cần cho es.mapping.id)
    if "asin" not in df.columns:
        print("Loi: Khong tim thay cot 'asin' trong du lieu JSON.")
        spark.stop()
        sys.exit(1)

except Exception as e:
    print(f"Loi khi doc hoac phan tich file JSON: {e}")
    spark.stop()
    sys.exit(1)

# === (Tùy chọn) Xử lý/Biến đổi Dữ liệu ===
# Ở đây chúng ta giữ nguyên DataFrame gốc để đưa vào ES
# Nếu bạn cần xử lý (chọn cột, đổi kiểu dữ liệu, làm sạch...), hãy làm ở đây
processed_df = df.select(
    col("asin"), # Đổi asin thành product_id nếu muốn
    col("title"),
    col("description"),
    col("final_price"),
    col("currency"),
    col("categories"), # Giữ nguyên là mảng string
    col("brand"),
    col("image_url"), # image_url là string, không phải mảng
    col("rating"),
    col("reviews_count"),
    col("top_review") # Có thể dùng nếu description null
    # Thêm các trường khác bạn thấy cần thiết
)

# === Ghi dữ liệu vào Elasticsearch ===
print(f"Dang ghi du lieu vao Elasticsearch index: {es_index}")
try:
    processed_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", es_index) \
        .option("es.mapping.id", "asin") \
        .mode("overwrite") \
        .save()
    print(f"Ghi du lieu vao Elasticsearch index '{es_index}' thanh cong!")
except Exception as e:
    print(f"Loi khi ghi vao Elasticsearch: {e}")

# === Dừng Spark Session ===
finally:
    # Bỏ cache để giải phóng bộ nhớ
    df.unpersist()
    spark.stop()
    print("Da dung SparkSession.")
