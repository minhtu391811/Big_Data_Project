import sys
sys.path.append('F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda')

import os
import time
import threading
from Lambda.producer import send_message
from Lambda.Stream_layer.consumer import consume
from Stream_data.stream_data import generate_real_time_data

stop_flag = threading.Event()

def producer_thread():
    csv_folder_path = 'F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda/Stream_data'
    while not stop_flag.is_set():
        try:
            csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
            if not csv_files:
                print("No CSV files found. Retrying in 10 seconds...")
                time.sleep(10)
                continue

            for file_name in csv_files:
                file_path = os.path.join(csv_folder_path, file_name)
                message = generate_real_time_data(file_path).encode('utf-8')
                send_message(message)
                print(f"Message sent to Kafka topic from {file_name}")
                time.sleep(5)  # Wait before sending next data
        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")
            time.sleep(5)  # Avoid infinite loop spam

def consumer_thread():
    while not stop_flag.is_set():
        try:
            consume()
            time.sleep(3)  # Wait before consuming next message
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")
            time.sleep(5)  # Avoid infinite loop spam

try:
    producer = threading.Thread(target=producer_thread, daemon=True)
    consumer = threading.Thread(target=consumer_thread, daemon=True)
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
except KeyboardInterrupt:
    print("Shutting down threads...")
    stop_flag.set()
    producer.join()
    consumer.join()
