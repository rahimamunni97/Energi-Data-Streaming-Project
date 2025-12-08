from kafka import KafkaConsumer
import psycopg2
import json
import os

# Read config from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "declaration_topic","elspot_topic")

POSTGRES_HOST = os.getenv("POSTGRES_HOST","localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB","energi_data")
POSTGRES_USER = os.getenv("POSTGRES_USER","postgres")
POSTGRES_PASSWORD= os.getenv("POSTGRES_PASSWORD","postgres")

#connect to PostgreSQL
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

# Connect to Kafka
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    valiue_deserializer=lambda x: json.loads(x.decode('utf=8'))
)

print(f" Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
print(f" Connected to PostgreSQL at {POSTGRES_HOST}")
print(" Listening for messages... (Press Ctrl+C to stop)\n")


try:
    for message in consumer:
        topic = message.topic
        data = message.value
        
        # Sample processing - example
        price_area = data.get("PriceArea", "N/A")

        # Insert into PostgreSQL
        cur.execute("""
            INSERT INTO energi_records (topic, price_area, playload)
            VALUES (%s, %s, %s);
        """, (topic, price_area, json.dumps(data)))
        
        conn.commit()
        print(f" Inserted record from topic: {topic}, area: {price_area}")

except KeyboardInterrupt:
    print("\nStopped by user.")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer and database connections closed cleanly.")
