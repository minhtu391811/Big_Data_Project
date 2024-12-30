from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType
import json

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStream") \
    .getOrCreate()

# Cấu hình Kafka Consumer (với các thông tin bạn đã có)
topic = 'real_time_data'
consumer_config = {
    'bootstrap.servers': 'pkc-n3603.us-central1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'CLWBGYF4SIM553SP',
    'sasl.password': 'P2LUtgMSQSfy1R7w74eu8cq1EkVVFZggY+JLqFIKIqi31KCzfS9SObQOoGF96qeQ',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False  # Commit thủ công các offset
}

# Tạo consumer Kafka
from confluent_kafka import Consumer
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Khởi tạo DataFrame stream từ Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-n3603.us-central1.gcp.confluent.cloud:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Chuyển đổi dữ liệu Kafka thành string (nếu dữ liệu là chuỗi JSON hoặc văn bản)
kafka_data = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Xử lý dữ liệu (ví dụ: phân tích văn bản, làm sạch dữ liệu, v.v.)
processed_data = kafka_data.select("value")

# Ví dụ: Giả sử bạn muốn lưu vào Elasticsearch
processed_data.writeStream \
    .outputMode("append") \
    .format("es") \
    .option("es.resource", "your_index_name/_doc") \
    .option("es.nodes", "localhost:9200") \
    .start() \
    .awaitTermination()
