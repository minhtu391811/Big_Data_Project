import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from put_data_hdfs import store_data_in_hdfs  # Assuming the store_data_in_hdfs function is defined here

def consume_hdfs():
    # Kafka broker configuration
    topic = 'historical_data'  # Change the topic name if needed

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'pkc-n3603.us-central1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'CLWBGYF4SIM553SP',
        'sasl.password': 'P2LUtgMSQSfy1R7w74eu8cq1EkVVFZggY+JLqFIKIqi31KCzfS9SObQOoGF96qeQ',
        'group.id': 'my_group_21',
        'auto.offset.reset': 'earliest'
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)  # Wait up to 1 second for a message

            if msg is None:
                # No message available within the timeout
                print('Waiting for messages...')
            elif msg.error():
                # Handle error
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached.')
                else:
                    print(f"ERROR: {msg.error()}")
            else:
                # Decode the message (assuming the message is JSON-encoded)
                try:
                    data = msg.value().decode('utf-8')  # Decode byte message to string
                    print(f"Raw message received: {data}")

                    # Parse the JSON message
                    json_data = json.loads(data)

                    # Validate required fields
                    required_keys = ['url', 'text', 'title', 'description', 'keywords']
                    if not all(key in json_data for key in required_keys):
                        print(f"Invalid JSON structure: {data}")
                        continue

                    # Extract values
                    url = json_data['url']
                    text = json_data['text']
                    title = json_data['title']
                    description = json_data['description']
                    keywords = json_data['keywords']

                    # Store the data in HDFS
                    try:
                        store_data_in_hdfs(url, text, title, description, keywords)
                        print(f"Successfully processed and stored data for URL: {url}")
                        consumer.commit()  # Commit offset after successful processing
                    except Exception as e:
                        print(f"Error storing data for URL {url}: {e}")

                except json.JSONDecodeError as jde:
                    # Handle JSON decoding errors
                    print(f"JSON decoding error: {jde}")
                except Exception as e:
                    # Handle any other errors
                    print(f"Unexpected error processing message: {e}")
    except KeyboardInterrupt:
        # Gracefully handle shutdown
        print("Shutting down consumer...")
    finally:
        # Close the consumer
        consumer.close()

# Run the consumer
consume_hdfs()
