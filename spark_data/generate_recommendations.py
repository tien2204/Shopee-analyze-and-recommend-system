import os
os.environ['HF_HOME'] = '/tmp/huggingface_cache'
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, pandas_udf, expr, collect_list, slice, size, sort_array, struct, lit
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer # Thư viện embedding
from sklearn.metrics.pairwise import cosine_similarity # Thư viện tính cosine
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.functions import coalesce, lit # Thêm lit
from typing import Iterator

# === Khởi tạo Spark Session ===
# Cấu hình ES có thể giữ lại nếu sau này bạn muốn ghi kết quả vào ES
spark = SparkSession.builder \
    .appName("LaptopRecommendation") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.index.auto.create", "true") \
    .getOrCreate()

print("Spark Session Created")

# === Tải Model Embedding ===
# Tải model Sentence Transformer (sẽ tải về lần đầu chạy)
# Bạn có thể chọn các model khác phù hợp hơn với tiếng Việt nếu cần
# ví dụ: 'bkai-foundation-models/vietnamese-bi-encoder'
# hoặc một model đa ngôn ngữ: 'all-MiniLM-L6-v2'
print("Loading Sentence Transformer model...")
model_name = 'all-MiniLM-L6-v2'
# Sử dụng broadcast để gửi model đã load đến tất cả các worker một cách hiệu quả
# Điều này quan trọng khi chạy trên cluster, nhưng cũng tốt cho local mode
model_broadcast = spark.sparkContext.broadcast(SentenceTransformer(model_name))
print(f"Model '{model_name}' loaded and broadcasted.")

# === Định nghĩa Pandas UDF để tạo embedding ===
# Sử dụng Scalar Iterator UDF để load model một lần cho mỗi partition
@pandas_udf(ArrayType(FloatType()))
def get_embeddings_udf(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Load model từ broadcast variable (chỉ một lần cho mỗi Python worker xử lý partition)
    model = model_broadcast.value
    for series_batch in batch_iter: # series_batch là một pd.Series các chuỗi string (description)
        # model.encode() mong đợi một list of strings
        # Nó trả về một list các numpy.ndarray (mỗi ndarray là một vector embedding)
        embeddings_np_array_list = model.encode(series_batch.tolist(), show_progress_bar=False)
        
        # Chuyển đổi danh sách các numpy.ndarray thành danh sách các list của Python float
        # để tạo pd.Series tương thích với kiểu trả về ArrayType(FloatType())
        embeddings_list_of_lists = [embedding.tolist() for embedding in embeddings_np_array_list]
        
        # Yield một pd.Series, trong đó mỗi phần tử là một embedding (dưới dạng list of float)
        yield pd.Series(embeddings_list_of_lists)

# === Đọc dữ liệu JSON ===
json_path = "/data/Amazon_Product.json" # Đường dẫn trong container
print(f"Reading data from {json_path}")
try:
    products_df = spark.read.json(json_path).cache() # Cache để tái sử dụng
    print("Data loaded successfully. Schema:")
    products_df.printSchema()
    products_df.show(2, truncate=False, vertical=True)
    # Đảm bảo có ít nhất các cột cần thiết
    required_cols = ["asin", "description", "categories", "final_price"]
    if not all(c in products_df.columns for c in required_cols):
        print(f"Lỗi: Thiếu cột cần thiết. Cần có: {required_cols}")
        spark.stop()
        exit()
except Exception as e:
    print(f"Lỗi khi đọc dữ liệu: {e}")
    spark.stop()
    exit()

# === Tạo Embedding cho cột Description ===
print("Generating embeddings for descriptions...")
# Tạo cột mới chứa text để embedding, xử lý null
products_df = products_df.withColumn(
    "text_for_embedding",
    coalesce(col("description"), col("top_review"), col("title"), lit(""))
)
# Tạo embedding từ cột mới này
products_with_embeddings = products_df.withColumn("embedding", get_embeddings_udf(col("text_for_embedding")))
# Chọn các cột cần thiết và cache lại
products_embedded_selected = products_with_embeddings.select(
    col("asin"),          # <--- Dùng 'asin' thay vì 'product_id'
    col("embedding"),     # <--- Cột embedding vừa tạo
    col("final_price"),  # <--- Dùng 'final_price' thay vì 'price'
    col("categories"),   # <--- Tên cột này giống nhau
    col("title")         # <--- Dùng 'title' thay vì 'name'
).cache()
print("Embeddings generated.")

# === SỬ DỤNG LSH ĐỂ TÌM KIẾM LÂN CẬN GẦN ĐÚNG ===
# 1. Chuyển đổi cột embedding (list of float) sang dạng Vector của MLlib
#    (LSH yêu cầu đầu vào là cột kiểu VectorUDT)
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

# Giả sử products_embedded_selected có cột "asin" và "embedding" (list of float)
# Tạo cột "features" kiểu Vector
df_with_vectors = products_embedded_selected.withColumn(
    "features", list_to_vector_udf(col("embedding"))
).select("asin", "features", "final_price", "categories", "title") # Vẫn dùng tên cột gốc
# Cache lại vì sẽ dùng nhiều lần
df_with_vectors.cache()
print("Converted embeddings to MLlib Vectors.")
df_with_vectors.select("asin", "features").show(2, truncate=50)

# 2. Khởi tạo BucketedRandomProjectionLSH
#    bucketLength và numHashTables là các tham số quan trọng cần tinh chỉnh
#    bucketLength nhỏ hơn -> chính xác hơn nhưng chậm hơn
#    numHashTables lớn hơn -> chính xác hơn nhưng tốn bộ nhớ/chậm hơn
brp = BucketedRandomProjectionLSH(
    inputCol="features",
    outputCol="hashes",
    bucketLength=2.0, # Thử nghiệm giá trị này
    numHashTables=3   # Thử nghiệm giá trị này
)

# 3. Fit mô hình LSH vào dữ liệu
print("Fitting LSH model...")
lsh_model = brp.fit(df_with_vectors)
print("LSH model fitted.")

# 4. Tìm các cặp sản phẩm tương tự gần đúng
#    approxSimilarityJoin tìm các cặp có khoảng cách Cosine (1 - similarity) dưới một ngưỡng nào đó
#    Ngưỡng càng nhỏ, các cặp tìm được càng giống nhau (similarity cao)
#    Đặt ngưỡng hợp lý (ví dụ: 0.9 tương đương similarity > 0.1, 0.5 tương đương similarity > 0.5)
#    Lưu ý: distCol ở đây là Cosine Distance (0 = giống hệt, 2 = khác biệt hoàn toàn)
similarity_threshold = 0.8 # Tìm các cặp có cosine distance < 0.8 (tức cosine similarity > 0.2)
print(f"Finding approximate similar pairs with distance threshold < {similarity_threshold}...")
approx_similar_pairs = lsh_model.approxSimilarityJoin(
    df_with_vectors, df_with_vectors, threshold=similarity_threshold, distCol="cosine_distance"
)

print("Approximate similar pairs found.")
# Output của approxSimilarityJoin gồm: datasetA, datasetB, cosine_distance
# Cần xử lý tiếp để lấy Top N
approx_similar_pairs.select(
    col("datasetA.asin").alias("asin_A"),
    col("datasetB.asin").alias("asin_B"),
    col("cosine_distance")
).show(10)

# 5. Xử lý kết quả LSH để lấy Top 5 cho mỗi sản phẩm
#    a. Loại bỏ cặp trùng lặp (A, B) và (B, A), loại bỏ cặp tự so sánh (A, A)
#    b. Sắp xếp theo khoảng cách cosine tăng dần (hoặc similarity giảm dần)
#    c. Lấy top 5
print("Processing LSH results to get Top 5...")

# Tính Cosine Similarity từ Cosine Distance
similarities_from_lsh = approx_similar_pairs \
    .select(
        col("datasetA.asin").alias("p1_asin"),
        col("datasetA.title").alias("p1_title"), # Lấy thêm thông tin nếu cần
        col("datasetB.asin").alias("p2_asin"),
        col("datasetB.title").alias("p2_title"),
        col("datasetB.categories").alias("p2_categories"),
        col("datasetB.final_price").alias("p2_price"),
        (lit(1.0) - col("cosine_distance")).alias("similarity") # similarity = 1 - distance
    ).where(col("p1_asin") != col("p2_asin")) # Loại bỏ tự so sánh

# Sử dụng Window function để lấy top N (tương tự cách cũ nhưng trên dữ liệu từ LSH)
window_spec_lsh = Window.partitionBy("p1_asin").orderBy(col("similarity").desc())

top_n_similar = similarities_from_lsh \
    .withColumn("rank", expr("rank() OVER (PARTITION BY p1_asin ORDER BY similarity DESC)")) \
    .where(col("rank") <= 5).cache() # Cache lại top_n_similar để dùng cho filter và sort


print("Top 5 similar products from LSH (before filtering/sorting based on original product's category):")
top_n_similar.orderBy("p1_asin", "rank").show(10, truncate=False)

# === Lọc theo Category ===
print("Filtering recommendations by category...")

# 1. Tạo DataFrame chứa thông tin category của SẢN PHẨM GỐC (p1_asin)
#    df_with_vectors chứa: "asin", "features", "final_price", "categories", "title"
#    Chúng ta cần 'asin' và 'categories' của sản phẩm gốc để so sánh.
original_product_categories_df = df_with_vectors.select(
    col("asin").alias("orig_p1_asin"), # Alias để phân biệt khi join
    col("categories")[0].alias("orig_p1_first_category") # Lấy category string đầu tiên của sản phẩm gốc
).distinct() # distinct() nếu một asin có thể xuất hiện nhiều lần trong df_with_vectors (dù không nên)

# 2. Join top_n_similar (chứa các cặp p1_asin và p2_asin gợi ý) 
#    với original_product_categories_df để lấy category của sản phẩm gốc (p1_asin).
#    Sau đó, lọc dựa trên category của sản phẩm được gợi ý (p2_categories).
top_n_filtered = top_n_similar.alias("rec") \
    .join(
        original_product_categories_df.alias("orig"), # Join với DataFrame category của sản phẩm gốc
        col("rec.p1_asin") == col("orig.orig_p1_asin") # Điều kiện join
    ) \
    .where(
        # Điều kiện lọc: category đầu tiên của sản phẩm được gợi ý (p2_categories)
        # phải giống với category đầu tiên của sản phẩm gốc (orig_p1_first_category)
        col("rec.p2_categories")[0] == col("orig.orig_p1_first_category")
    ) \
    .select( # Chọn lại các cột cần thiết từ 'rec' (tức là top_n_similar)
        "rec.p1_asin",
        "rec.p2_asin",
        "rec.p2_title",
        "rec.p2_categories", # Giữ lại để kiểm tra nếu muốn
        "rec.p2_price",      # Đây là final_price của sản phẩm được gợi ý
        "rec.similarity",
        "rec.rank"
        # Không cần "rec.*" nếu các cột đã rõ ràng, tránh mang theo cột join không cần thiết
    )

print("Top 5 similar products after category filtering:")
top_n_filtered.orderBy("p1_asin", "rank").show(10, truncate=False)


# === Sắp xếp theo Giá (Đắt -> Rẻ) và chuẩn bị ghi vào ES ===
print("Sorting filtered recommendations by price (descending) and preparing for ES...")

# 1. Tạo DataFrame chứa chi tiết của TẤT CẢ các sản phẩm gốc (bao gồm image_url)
#    Lấy từ products_df (DataFrame gốc đã đọc từ JSON)
all_product_details_df = products_df.select(
    col("asin").alias("detail_asin"), # Đặt tên alias rõ ràng
    col("image_url").alias("detail_image_url"),
    col("title").alias("detail_title"), # Lấy thêm title để đảm bảo có thể dùng cho p2_title
    col("final_price").alias("detail_final_price") # Lấy thêm final_price để dùng cho p2_price
    # Bạn có thể lấy thêm các trường khác của sản phẩm nếu cần
).distinct() # distinct() để đảm bảo mỗi asin chỉ có một dòng chi tiết

# 2. Join top_n_filtered (kết quả sau khi lọc category, đã alias là "rec")
#    với all_product_details_df để lấy image_url (và các chi tiết khác) cho SẢN PHẨM ĐƯỢC GỢI Ý (p2_asin).
recommendations_with_full_details = top_n_filtered.alias("rec") \
    .join(
        all_product_details_df.alias("p2_info"), # Join với DataFrame chi tiết sản phẩm
        # Điều kiện join: p2_asin (ID sản phẩm được gợi ý trong danh sách) 
        # phải bằng ID sản phẩm trong bảng chi tiết
        col("rec.p2_asin") == col("p2_info.detail_asin") 
    ) \
    .select(
        col("rec.p1_asin"),  # ID sản phẩm gốc
        col("rec.p2_asin"),  # ID sản phẩm được gợi ý
        col("p2_info.detail_title").alias("p2_title"),    # Lấy title của sản phẩm gợi ý từ join
        col("p2_info.detail_final_price").alias("p2_price"),# Lấy price của sản phẩm gợi ý từ join
        col("p2_info.detail_image_url").alias("p2_image_url"),# Lấy image_url của sản phẩm gợi ý
        col("rec.similarity")
        # Các cột p2_categories, rank từ 'rec' có thể giữ lại nếu cần cho các bước sau nữa
        # Hoặc nếu đã có p2_title, p2_price từ top_n_similar rồi thì không cần join để lấy lại nữa,
        # chỉ cần join để lấy p2_image_url.
    )

print("Recommendations with full details (including image_url):")
recommendations_with_full_details.show(truncate=False)

# 3. Sắp xếp và Tạo Output Cuối cùng (dùng recommendations_with_full_details)
final_recommendations_sorted = recommendations_with_full_details \
    .orderBy(col("p1_asin"), col("p2_price").desc()) \
    .groupBy("p1_asin") \
    .agg(collect_list(struct(
        col("p2_asin").alias("recommended_asin"),
        col("p2_title").alias("name"), # Đây là title của sản phẩm được gợi ý
        col("p2_price").alias("price"),   # Đây là final_price của sản phẩm được gợi ý
        col("p2_image_url").alias("image_url"),
        col("similarity")
        )).alias("recommendations")
    )

print("Final recommendations sorted by price:")
final_recommendations_sorted.show(truncate=False)


# === (BẮT BUỘC) Ghi kết quả vào Elasticsearch ===
# (Phần này giữ nguyên như bạn đã có, chỉ cần đảm bảo input DataFrame là final_recommendations_sorted)
print("Saving final recommendations to Elasticsearch...")
output_df_for_es = final_recommendations_sorted.select(
      col("p1_asin").alias("source_asin"), # Dùng p1_asin làm ID nguồn
      col("recommendations")
)
try:
    output_df_for_es.write \
      .format("org.elasticsearch.spark.sql") \
      .option("es.resource", "amazon_product_recommendations") \
      .option("es.mapping.id", "source_asin") \
      .mode("overwrite") \
      .save()
    print("Lưu kết quả gợi ý vào Elasticsearch thành công!")
except Exception as e:
    print(f"Lỗi khi ghi kết quả gợi ý vào Elasticsearch: {e}")


# === Dừng Spark Session ===
# Nhớ unpersist tất cả các DataFrame đã cache
if 'products_df' in locals() and products_df.is_cached:
    products_df.unpersist()
if 'products_embedded_selected' in locals() and products_embedded_selected.is_cached:
    products_embedded_selected.unpersist()
if 'df_with_vectors' in locals() and df_with_vectors.is_cached:
    df_with_vectors.unpersist()
if 'top_n_similar' in locals() and top_n_similar.is_cached: # top_n_similar có thể không cần cache nữa nếu bạn xử lý ngay
    top_n_similar.unpersist()

spark.stop()
print("Da dung Spark Session.")
