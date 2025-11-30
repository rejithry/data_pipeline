import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_BROKER = "kafkabroker:9092"
TOPIC = "weather"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})
faker = Faker()


# -------------------------------
# Delivery Callback
# -------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✔ Sent to {msg.topic()} [{msg.partition()}]: {msg.value().decode('utf-8')}")

# -------------------------------
# Generate Random Stock Data
# -------------------------------
def generate_stock_data():
    return {'city': faker.city(), 'temperature': random.uniform(10.0, 110.0)}


# -------------------------------
# Main Loop
# -------------------------------
if __name__ == "__main__":
    print(f"🚀 Sending stock data to Kafka topic '{TOPIC}' every 5 seconds...\n")

    while True:
        messages = []

        # Generate 10 stock events every 5 seconds
        for _ in range(10):
            msg = generate_stock_data()
            producer.produce(
                TOPIC,
                json.dumps(msg).encode("utf-8"),
                callback=delivery_report
            )
            messages.append(msg)

        producer.flush()

        print(f"📤 Sent batch of 10 messages @ {datetime.utcnow().isoformat()}")
        time.sleep(5)
        break