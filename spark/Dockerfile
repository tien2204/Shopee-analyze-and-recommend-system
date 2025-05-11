# Sử dụng image Bitnami Spark làm base image
FROM bitnami/spark:3.5.1

# Chuyển sang user root để có quyền cài đặt packages và copy file
USER root

# Cập nhật danh sách gói, cài đặt curl, sau đó dọn dẹp apt lists
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Sao chép file JAR connector từ build context (thư mục ./spark-jars trên host, so với vị trí Dockerfile)
# vào thư mục /opt/bitnami/spark/jars bên trong image.
# Giả sử file JAR của bạn nằm ở ../spark-jars/elasticsearch-spark-30_2.13-8.13.4.jar
# so với vị trí của Dockerfile (là ~/docker-hadoop/spark/Dockerfile)

COPY jars_to_copy/elasticsearch-spark-30_2.12-8.11.3.jar /opt/bitnami/spark/jars/
# Chạy lệnh pip install để cài các thư viện Python cần thiết
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir sentence-transformers torch pandas pyarrow

# Chuyển về user mặc định của image Bitnami
USER 1001