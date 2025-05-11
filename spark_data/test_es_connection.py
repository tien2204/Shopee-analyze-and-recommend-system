# test_es_connection.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestESConnection") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.driver.userClassPathFirst", "true") \
    .config("spark.executor.userClassPathFirst", "true") \
    .getOrCreate()

print("SparkSession created for ES test.")

try:
    test_data = [("1", "test_data")]
    test_df = spark.createDataFrame(test_data, ["id", "data"])
    test_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "test_spark_index") \
        .option("es.mapping.id", "id") \
        .mode("overwrite") \
        .save()
    print("Successfully wrote to test_spark_index!")

    read_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "test_spark_index") \
        .load()
    print("Successfully read from test_spark_index:")
    read_df.show()

except Exception as e:
    print(f"Error during ES operation: {e}")

spark.stop()
