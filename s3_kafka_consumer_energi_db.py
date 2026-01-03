import json
import os
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv
import uuid


# Load environment variables from .env file.
# Environment variables are used to avoid hardcoding
# connection details and to simplify deployment changes.
load_dotenv()


# Kafka configuration parameters
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# The consumer subscribes to both production and price topics
# to handle them in a unified pipeline.
KAFKA_TOPICS = ["declaration_topic", "elspot_topic"]

# Consumer group allows Kafka to manage offsets and supports
# scaling if additional consumers are added later.
CONSUMER_GROUP = "energi-consumers"


# Database configuration parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "energi_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Connect to PostgreSQL Database
# A single persistent database connection is used to
# store validated streaming records.
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# Kafka Consumer Setup
# The consumer reads JSON-encoded messages from the specified topics.
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    # Messages are deserialized from JSON before processing
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("==========================================")
print(" Energi Kafka Consumer Started")
print(f" Topics       : {KAFKA_TOPICS}")
print(f" Kafka Broker : {KAFKA_BOOTSTRAP_SERVERS}")
print(f" Database     : {DB_NAME}")
print("==========================================")

try:
    # Continuously consume messages from Kafka topics
    for message in consumer:
        topic = message.topic
        payload = message.value

    
        # Generate unique event identifier for each record
        # A UUID is assigned to each event to support traceability
        # and avoid duplicate primary keys in the database.
        event_id = str(uuid.uuid4())

        price_area = payload.get("PriceArea")

        # Initialize optional fields
        production_type = None
        production_mwh = None
        co2_per_kwh = None
        spot_price_eur = None


        # Topic-specific extraction logic
        # Different topics contain different fields, so extraction
        # is handled conditionally based on the source topic.
        if topic == "declaration_topic":
            production_type = payload.get("ProductionType")
            production_mwh = payload.get("Production_MWh")
            co2_per_kwh = payload.get("CO2PerkWh")

        elif topic == "elspot_topic":
            spot_price_eur = payload.get("SpotPriceEUR")


        # Debug output 
        # This print statement was useful during development to
        # verify correct message routing and extracted values.
        print(
            "INSERTING:",
            topic,
            price_area,
            production_mwh,
            spot_price_eur
        )


        # Insert record into database 
        # All records are stored in a single table with a flexible
        # schema, while the full original payload is preserved as JSON.
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

        # Commit after each insert to ensure durability
        conn.commit()

except KeyboardInterrupt:
    # Allow graceful shutdown when stopping the consumer manually
    print("\nConsumer stopped by user.")

except Exception as e:
    # Catch unexpected runtime errors to avoid silent failures
    print("\n[ERROR]", e)

finally:
    # Ensure all resources are released properly
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer and database connections closed.")
