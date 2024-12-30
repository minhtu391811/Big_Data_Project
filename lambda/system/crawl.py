import logging
import sys
import time
import json
import requests
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CrawlAndProduce")

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

server_url = "http://127.0.0.1:8000/crawl_data"

headers = {"Content-Type": "application/json"}

def crawl_url(url):
    try:
        payload = {"text": url}
        response = requests.post(server_url, headers=headers, data=json.dumps(payload), timeout=10)

        if response.status_code == 200:
            data = json.loads(response.text)['content']
            # Send data to Kafka
            producer.send('web-crawl', {'url': url, 'content': data, 'timestamp': time.time()})
            logger.info(f"Data from {url} sent to Kafka.")
        else:
            logger.error(f"Error crawling {url}: {response.status_code}")
    except Exception as e:
        logger.error(f"Exception when crawling {url}: {e}")

if __name__ == "__main__":
    target_url = "https://vnexpress.net/du-kien-can-130-000-ty-dong-giai-quyet-che-do-khi-tinh-gon-bo-may-4833492.html"
    while True:
        crawl_url(target_url)
        time.sleep(10)  # Crawl every 10 seconds
