from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, udf
from pyspark.sql.types import StringType
import json

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStream") \
    .config("spark.jars", "path_to_spark-sql-kafka.jar") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-n3603.us-central1.gcp.confluent.cloud:9092") \
    .option("subscribe", "real_time_data") \
    .option("startingOffsets", "latest") \
    .load()

# Chuyển đổi dữ liệu Kafka (chuyển giá trị từ byte thành string)
kafka_data = kafka_stream.selectExpr("CAST(value AS STRING)")

# Hàm làm sạch dữ liệu (ví dụ: loại bỏ ký tự đặc biệt, chuẩn hóa văn bản)
def clean_text(text):
    text = text.lower()  # Chuyển thành chữ thường
    text = regexp_replace(text, '[^a-zA-Z0-9\\s]', '')  # Loại bỏ ký tự không phải là chữ cái và số
    return text

# Đăng ký UDF để áp dụng làm sạch dữ liệu
clean_text_udf = udf(clean_text, StringType())

# Áp dụng làm sạch dữ liệu
cleaned_data = kafka_data.withColumn("cleaned_value", clean_text_udf(col("value")))

# Lọc hoặc thêm các bước xử lý khác (ví dụ: loại bỏ bản ghi null)
processed_data = cleaned_data.filter(col("cleaned_value").isNotNull())

# Ghi kết quả vào Elasticsearch
processed_data.writeStream \
    .outputMode("append") \
    .format("es") \
    .option("es.resource", "your_index_name/_doc") \
    .option("es.nodes", "localhost:9200") \
    .start() \
    .awaitTermination()
