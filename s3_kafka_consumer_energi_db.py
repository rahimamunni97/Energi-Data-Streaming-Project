import json
import os
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from dotenv import load_dotenv
from validation import validate_and_normalize

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

# --------------------------------------------------
# Kafka configuration
# --------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topics_env = os.getenv("KAFKA_TOPICS", "declaration_topic,elspot_topic")
KAFKA_TOPICS = [t.strip() for t in topics_env.split(",")]

CONSUMER_GROUP = "energi-consumers"
DLQ_TOPIC = "energi_invalid"

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
    enable_auto_commit=False,          # manual commit (safe)
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# --------------------------------------------------
# Kafka Producer (Dead Letter Queue)
# --------------------------------------------------
dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --------------------------------------------------
# Counters (monitoring)
# --------------------------------------------------
processed = 0
valid_count = 0
invalid_count = 0
duplicate_count = 0

print("==========================================")
print(" Energi Kafka Consumer Started")
print(f" Topics       : {KAFKA_TOPICS}")
print(f" Kafka Broker : {KAFKA_BOOTSTRAP_SERVERS}")
print(f" Database     : {DB_NAME}")
print("==========================================")

try:
    for message in consumer:
        processed += 1
        topic = message.topic
        payload = message.value

        # --------------------------------------------------
        # 1) Validate + normalize
        # --------------------------------------------------
        clean, reason = validate_and_normalize(payload, topic)

        # --------------------------------------------------
        # 2) INVALID DATA → store + DLQ
        # --------------------------------------------------
        if reason:
            invalid_count += 1

            cur.execute(
                """
                INSERT INTO energi_rejected (topic, reason, raw_payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                (topic, reason, json.dumps(payload))
            )
            conn.commit()

            dlq_producer.send(
                DLQ_TOPIC,
                {
                    "topic": topic,
                    "reason": reason,
                    "payload": payload
                }
            )

            print(f"[INVALID] {reason}")
            consumer.commit()
            continue

        # --------------------------------------------------
        # 3) VALID DATA → idempotent insert
        # --------------------------------------------------
        event_id = clean["event_id"]
        price_area = clean.get("PriceArea")
        price = clean.get("SpotPriceEUR")

        cur.execute(
            """
            INSERT INTO energi_records (event_id, topic, price_area, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, topic, price_area, json.dumps(clean))
        )

        if cur.rowcount == 1:
            valid_count += 1
            print(f"[INSERTED] {topic} | {price_area}")
        else:
            duplicate_count += 1
            print(f"[DUPLICATE] {event_id}")

        # --------------------------------------------------
        # 4) DATA QUALITY DECISION (NEW FEATURE)
        # --------------------------------------------------
        if price is None:
            quality_status = "DENIED"
            quality_reason = "Missing price"
        elif price < 0:
            quality_status = "DENIED"
            quality_reason = "Negative price"
        elif price > 1000:
            quality_status = "RISKY"
            quality_reason = "Unusually high price"
        else:
            quality_status = "USABLE"
            quality_reason = "Passed all quality checks"

        cur.execute(
            """
            INSERT INTO energi_quality (event_id, quality_status, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, quality_status, quality_reason)
        )

        conn.commit()
        consumer.commit()

        # --------------------------------------------------
        # 5) Periodic ingestion metrics (every 50 records)
        # --------------------------------------------------
        if processed % 50 == 0:
            cur.execute(
                """
                INSERT INTO energi_ingest_metrics
                (topic, valid_count, invalid_count, duplicate_count)
                VALUES (%s, %s, %s, %s)
                """,
                ("ALL", valid_count, invalid_count, duplicate_count)
            )
            conn.commit()

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

except Exception as e:
    print(f"\n[ERROR] {e}")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer and database connections closed.")
