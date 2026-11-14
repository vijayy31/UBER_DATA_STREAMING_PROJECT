from confluent_kafka import Producer
import json
from dotenv import load_dotenv
load_dotenv()
import os

INSTANCE_IP = os.getenv("KAFKA_INSTANCE_IP")
conf = {
    # Provide all 3 broker addresses separated by commas
    'bootstrap.servers': f"{INSTANCE_IP}:9092,{INSTANCE_IP}:9093",
    'client.id': 'python-producer',
    'acks': 'all'
}

producer = Producer(conf)

def delivery_report(err, msg): 
    if err:
         print(f"Message delivery failed: {err}")

    else:
        key = msg.key().decode('utf-8') if msg.key() else None
        print(
            f"Message delivered to {msg.topic()} "
            f"[Key: {key}]"
            f"[Partition: {msg.partition()}] "
            f"[Offset: {msg.offset()}]"
        )


if __name__ == "__main__":

    topic = "orders"
    for i in range(5):
        payload = {'id': i*100, 'message': f'Hello from Confluent Producer {i}'}
        data = json.dumps(payload).encode('utf-8')
        key = str(i*100).encode('utf-8')

        # Produce is asynchronous; it buffers messages in memory
        producer.produce(topic=topic, key=key, value=data, callback=delivery_report)
        
        # Serve delivery report callbacks from previous produce calls
        producer.poll(0)



    # Block until all outstanding messages are delivered
    print("Flushing outstanding messages...")
    remaining = producer.flush(timeout=20)
    print("Remaining:", remaining)