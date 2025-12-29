import json
import os
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topics_env = os.getenv("KAFKA_TOPICS", "declaration_topic,elspot_topic")
KAFKA_TOPICS = [t.strip() for t in topics_env.split(",")]

# Database config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "energi_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


# ----------------------------
# Validation Function
# ----------------------------
def validate_record(data):
    if not isinstance(data, dict):
        return False

    if "PriceArea" not in data or not data["PriceArea"]:
        return False

    if "HourUTC" in data:
        try:
            datetime.fromisoformat(data["HourUTC"])
        except:
            return False

    if "SpotPriceEUR" in data:
        try:
            if float(data["SpotPriceEUR"]) < 0:
                return False
        except:
            return False

    return True


# ----------------------------
# Database Connection
# ----------------------------
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# ----------------------------
# Kafka Consumer
# ----------------------------
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Connected to PostgreSQL at {DB_HOST}")
print("Listening for messages... (Press Ctrl+C to stop)\n")

try:
    for message in consumer:
        topic = message.topic
        data = message.value

        if validate_record(data):
            price_area = data.get("PriceArea", "N/A")

            cur.execute("""
                INSERT INTO energi_records (topic, price_area, payload)
                VALUES (%s, %s, %s);
            """, (topic, price_area, json.dumps(data)))

            conn.commit()
            print(f"Inserted record from topic: {topic}, area: {price_area}")
        else:
            print("Skipped invalid message")

except KeyboardInterrupt:
    print("\nStopped by user.")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer and database connections closed cleanly.")
