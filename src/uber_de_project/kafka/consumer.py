import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
from uber_de_project.aws.s3_client import upload_to_s3, get_s3_client
load_dotenv()

def kafka_consumer():
    INSTANCE_IP = os.getenv("KAFKA_INSTANCE_IP")
    # Configuration
    conf = {
        'bootstrap.servers': f"{INSTANCE_IP}:9092,{INSTANCE_IP}:9093",
        'group.id': 'events-group',
        'auto.offset.reset': 'earliest',  # 'earliest' reads from the beginning, 'latest' reads new messages only
        'enable.auto.commit': True,
        'client.id': 'python-consumer'
    }

    s3_client = get_s3_client()
    print("AWS s3 connection initialized successfully")

    consumer = Consumer(conf)
    # topic = 'orders'
    topic = os.getenv("KAFKA_TOPIC")

    consumer.subscribe([topic])

    print(f"Subscribed to '{topic}'. Waiting for messages... Press Ctrl+C to stop.")

    try:
        while True:
            # Timeout set to 1.0 second
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            # 3. Real errors (network dropped, broker failure, auth issue)
            if msg.error():
                print(f"Consumer encountered fatal error: {msg.error()}")
                break

            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8'))

            upload_to_s3(s3_client=s3_client, event=value, parent_folder="test")
            print(f"Received -> Key: {key} Partition: {msg.partition()} | Offset: {msg.offset()}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        # Close down consumer to commit final offsets and clean up consumer group membership
        consumer.close()
        s3_client.close()
        print("Kafka consumer and S3 client closed!")


if __name__ == "__main__":

    kafka_consumer()