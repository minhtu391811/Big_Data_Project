from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# 1. Cấu Hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HDFStoElasticsearch")

# 2. Khởi Tạo SparkSession với Elasticsearch Connector
spark = SparkSession.builder \
    .appName("HDFStoElasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.17.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# 3. Đọc Dữ Liệu Từ HDFS
def read_data_from_hdfs(hdfs_path: str):
    try:
        logger.info(f"Reading data from HDFS path: {hdfs_path}")
        df = spark.read.parquet(hdfs_path)

        if df.head(1):  # Kiểm tra xem DataFrame có dữ liệu không
            content = df.select("content").limit(1).collect()[0]["content"]
            logger.info("Successfully fetched content from the first record.")
            return content
        else:
            logger.warning("No data found in the specified path.")
            return None
    except Exception as e:
        logger.error(f"Error while reading from HDFS: {str(e)}")
        return None

# 4. Main Execution
if __name__ == "__main__":
    hdfs_path = "hdfs://localhost:9000/user/spark/web_crawl_data/"
    content = read_data_from_hdfs(hdfs_path)

    if content:
        print("Content from the first record:", content)
    else:
        print("No content retrieved.")
