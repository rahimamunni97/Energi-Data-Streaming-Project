import json
import os
import requests
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# --------------------------------------------------
# Kafka config
# --------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# --------------------------------------------------
# API endpoints
# --------------------------------------------------
DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"

# --------------------------------------------------
# Data Cleaning
# --------------------------------------------------
def clean_record(record):
    if not record:
        return None

    # Normalize PriceArea
    if "PriceArea" in record and record["PriceArea"]:
        record["PriceArea"] = record["PriceArea"].strip().upper()
    else:
        return None

    # Fix timestamp format
    if "HourUTC" in record:
        record["HourUTC"] = record["HourUTC"].replace(" ", "T")

    # Remove invalid prices
    if "SpotPriceEUR" in record:
        try:
            if float(record["SpotPriceEUR"]) < 0:
                return None
        except ValueError:
            return None

    return record

# --------------------------------------------------
# Fetch data ONCE (rate-safe)
# --------------------------------------------------
def fetch_data(url, limit=5):
    try:
        resp = requests.get(
            url,
            params={"limit": limit},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", [])
    except Exception as e:
        print(f"[ERROR] API request failed: {e}")
        return []

# --------------------------------------------------
# Send cleaned data to Kafka (slow & safe)
# --------------------------------------------------
def send_to_kafka(topic, records):
    sent = 0
    for record in records:
        fields = record.get("fields", record)
        cleaned = clean_record(fields)

        if cleaned:
            producer.send(topic, cleaned)
            sent += 1
            print(f"Sent to {topic}: {cleaned.get('PriceArea')}")
            time.sleep(2)  # <-- VERY IMPORTANT (rate-safe)
        else:
            print("Skipped invalid record")

    return sent

# --------------------------------------------------
# MAIN — ONE-SHOT INGESTION
# --------------------------------------------------
if __name__ == "__main__":
    print("=== One-shot Energi Data Producer started ===")

    decl_data = fetch_data(DECLARATION_URL)
    elspot_data = fetch_data(ELSPOT_URL)

    print(f"Declaration records fetched: {len(decl_data)}")
    print(f"Elspot records fetched: {len(elspot_data)}")

    sent_decl = send_to_kafka("declaration_topic", decl_data)
    sent_elspot = send_to_kafka("elspot_topic", elspot_data)

    producer.flush()
    producer.close()

    print("==========================================")
    print(f"Sent {sent_decl} declaration records")
    print(f"Sent {sent_elspot} elspot records")
    print("One-shot ingestion completed successfully")
    print("==========================================")
