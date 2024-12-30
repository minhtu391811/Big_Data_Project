from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, length, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from elasticsearch import Elasticsearch, helpers
import json
import logging
import signal
import sys
import hashlib
import os
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaSparkElasticsearchIntegration")

# Elasticsearch index
es_index = "web-crawl_1"

# Graceful shutdown handler
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    es_query.stop()
    console_query.stop()
    spark.stop()
    sys.exit(0)

# Get embedding function with error handling
def get_embedding_safe(text):
    url = "http://127.0.0.1:8000/get_docs_embedding"
    payload = {"text": text}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            return response.json().get("embedding", [0] * 768)  # Default embedding
        else:
            logger.warning(f"API failed with status {response.status_code}: {response.text}")
            return [0] * 768
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return [0] * 768

# Check if HDFS path exists
def hdfs_path_exists(path):
    try:
        return os.system(f"hadoop fs -test -e {path}") == 0
    except Exception as e:
        logger.error(f"Error checking HDFS path: {e}")
        return False

# Generate new HDFS path if file exists
def generate_new_hdfs_path(path):
    base, ext = os.path.splitext(path)
    counter = 1
    new_path = f"{base}_{counter}{ext}"
    while hdfs_path_exists(new_path):
        counter += 1
        new_path = f"{base}_{counter}{ext}"
    return new_path

# Signal handling for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Spark session with necessary connectors
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.17.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Schema for incoming JSON data
schema = StructType([
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", FloatType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-crawl") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Process data
processed_df = json_df.withColumn("word_count", 
    (length(col("content")) - length(regexp_replace(col("content"), "\\w+", ""))) / 1
)

# Write processed data to console
console_query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write batch data to HDFS and Elasticsearch
def process_batch(batch_df, batch_id):
    try:
        logger.info(f"Processing batch {batch_id}...")

        # HDFS path
        file_name = f"web_crawl_data_batch_{batch_id}.parquet"
        hdfs_path = f"hdfs://localhost:9000/user/spark/web_crawl_data/{file_name}"
        if hdfs_path_exists(hdfs_path):
            hdfs_path = generate_new_hdfs_path(hdfs_path)

        # Add embeddings
        records = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
        for record in records:
            record["embedding"] = get_embedding_safe(record.get("content", ""))

        # Save to HDFS
        new_batch_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
        new_batch_df = spark.read.json(new_batch_rdd)
        new_batch_df.coalesce(1).write.mode("append").parquet(hdfs_path)
        logger.info(f"Batch {batch_id} saved to HDFS: {hdfs_path}")

        # Elasticsearch
        es = Elasticsearch(hosts=["http://localhost:9200"])
        if not es.ping():
            logger.error("Elasticsearch cluster is down!")
            return
        actions = [
            {
                "_index": es_index,
                "_source": record,
                "_id": hashlib.sha256(record["url"].encode()).hexdigest()
            }
            for record in records
        ]
        helpers.bulk(es, actions)
        logger.info(f"Batch {batch_id} indexed to Elasticsearch.")

    except Exception as e:
        logger.error(f"Error in process_batch: {e}")

# Stream query for Elasticsearch and HDFS
es_query = (
    processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/web-crawl-es-hdfs") \
        .start()
)

# Await termination
spark.streams.awaitAnyTermination()
