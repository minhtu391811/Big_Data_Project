from kafka import KafkaProducer
import json
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducerApp")

def create_producer(bootstrap_servers):
    """Tạo Kafka producer với cấu hình cụ thể."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka producer đã được khởi tạo thành công.")
        return producer
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Kafka producer: {str(e)}")
        return None

def send_message(producer, topic, message):
    """Gửi tin nhắn đến một topic Kafka."""
    if producer is None:
        logger.error("Kafka producer không khả dụng. Không thể gửi tin nhắn.")
        return

    try:
        producer.send(topic, message)
        producer.flush()  # Đảm bảo gửi tin nhắn ngay lập tức
        logger.info(f"Đã gửi tin nhắn: {message} đến topic: {topic}")
    except Exception as e:
        logger.error(f"Lỗi khi gửi tin nhắn: {str(e)}")

if __name__ == "__main__":
    # Cấu hình Kafka
    bootstrap_servers = ['localhost:9092']
    topic = 'test-topic'
    message = {'message': 'Hello Kafka!'}

    # Khởi tạo producer
    producer = create_producer(bootstrap_servers)

    # Gửi tin nhắn
    send_message(producer, topic, message)

    # Đóng producer sau khi sử dụng
    if producer:
        producer.close()
