from kafka import KafkaConsumer
import psycopg2
import json

# --- PostgreSQL connection ---
pg_conn = psycopg2.connect(
    host="localhost",
    port=55432,
    database="energi_data",
    user="postgres",
    password="postgres"
)
pg_cursor = pg_conn.cursor()

# --- Kafka consumer setup ---
consumer = KafkaConsumer(
    "declaration_topic", "elspot_topic",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

print("Listening for messages... (Press Ctrl+C to stop)\n")

try:
    for message in consumer:
        topic = message.topic
        data = message.value
        fields = data.get("fields", data)

        price_area = fields.get("PriceArea", "N/A")
        production_type = fields.get("ProductionType", "N/A")
        co2_per_kwh = fields.get("CO2PerkWh", 0.0)
        production_mwh = fields.get("Production_MWh", 0.0)

        # Print to console for tracking
        print(f"Topic: {topic} | PriceArea: {price_area} | ProductionType: {production_type}")

        # Insert into PostgreSQL
        pg_cursor.execute("""
            INSERT INTO energi_records (source, price_area, production_type, co2_per_kwh, production_mwh)
            VALUES (%s, %s, %s, %s, %s);
        """, (topic, price_area, production_type, co2_per_kwh, production_mwh))
        pg_conn.commit()

except KeyboardInterrupt:
    print("\nStopped by user.")

finally:
    consumer.close()
    pg_cursor.close()
    pg_conn.close()
    print("Consumer and database connections closed cleanly.")
