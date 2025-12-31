import json
import os
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv
import uuid

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

# --------------------------------------------------
# Kafka configuration
# --------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = ["declaration_topic", "elspot_topic"]
CONSUMER_GROUP = "energi-consumers"

# --------------------------------------------------
# Database configuration
# --------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "energi_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# --------------------------------------------------
# Connect to PostgreSQL
# --------------------------------------------------
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("==========================================")
print(" Energi Kafka Consumer Started")
print(f" Topics       : {KAFKA_TOPICS}")
print(f" Kafka Broker : {KAFKA_BOOTSTRAP_SERVERS}")
print(f" Database     : {DB_NAME}")
print("==========================================")

try:
    for message in consumer:
        topic = message.topic
        payload = message.value

        # -----------------------------
        # Generate event_id
        # -----------------------------
        event_id = str(uuid.uuid4())
        price_area = payload.get("PriceArea")

        production_type = None
        production_mwh = None
        co2_per_kwh = None
        spot_price_eur = None

        # -----------------------------
        # Topic-specific extraction
        # -----------------------------
        if topic == "declaration_topic":
            production_type = payload.get("ProductionType")
            production_mwh = payload.get("Production_MWh")
            co2_per_kwh = payload.get("CO2PerkWh")

        elif topic == "elspot_topic":
            spot_price_eur = payload.get("SpotPriceEUR")

        # -----------------------------
        # DEBUG PRINT (NOW SAFE)
        # -----------------------------
        print(
            "INSERTING:",
            topic,
            price_area,
            production_mwh,
            spot_price_eur
        )

        # -----------------------------
        # Insert into database
        # -----------------------------
        cur.execute(
            """
            INSERT INTO energi_records (
                event_id,
                source,
                price_area,
                production_type,
                production_mwh,
                co2_per_kwh,
                spot_price_eur,
                payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                event_id,
                topic,
                price_area,
                production_type,
                production_mwh,
                co2_per_kwh,
                spot_price_eur,
                json.dumps(payload)
            )
        )

        conn.commit()

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

except Exception as e:
    print("\n[ERROR]", e)

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer and database connections closed.")
