import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import uuid


# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_BROKER = "kafkabroker:9092"
TOPIC = "stock_prices"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

# -------------------------------
# Stock list (10 symbols)
# -------------------------------
STOCKS = [
    "AAPL", "AMZN", "MSFT", "GOOGL", "META",
    "TSLA", "NFLX", "NVDA", "INTC", "AMD"
]

# Initialize base prices
base_prices = {s: random.uniform(100, 500) for s in STOCKS}

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
    stock = random.choice(STOCKS)

    # Random price movement
    change_pct = random.uniform(-1, 1)  # -1% to +1%
    base_prices[stock] *= (1 + change_pct / 100)

    data = {
        "symbol": stock,
        "price": round(base_prices[stock], 2),
        "volume": random.randint(100, 20000),
        "timestamp": datetime.utcnow().isoformat(),
        "id": str(uuid.uuid4()),
    }

    return data

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