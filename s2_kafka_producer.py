import json
import os
import requests
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# Kafka config
producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# APIs
DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"


# Data Cleaning Function
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
        try:
            record["HourUTC"] = record["HourUTC"].replace(" ", "T")
        except:
            return None

    # Remove invalid prices
    if "SpotPriceEUR" in record:
        try:
            if float(record["SpotPriceEUR"]) < 0:
                return None
        except:
            return None

    return record


# Fetch Data
def fetch_data(url, limit=10):
    resp = requests.get(url, params={"limit": limit})
    data = resp.json()
    return data.get("records", [])


# Send to Kafka (CLEANED)
def send_to_kafka(topic, records):
    for record in records:
        fields = record.get("fields", record)

        cleaned = clean_record(fields)

        if cleaned:
            producer.send(topic, cleaned)
            print(f"Sent record to {topic}: {cleaned.get('PriceArea')}")
            time.sleep(1)
        else:
            print("Skipped invalid record")


# Main Execution
if __name__ == "__main__":
    print("Fetching DeclarationProduction sample...")
    decl_data = fetch_data(DECLARATION_URL)
    print(f"Got {len(decl_data)} records")

    print("Fetching Elspotprices sample...")
    elspot_data = fetch_data(ELSPOT_URL)
    print(f"Got {len(elspot_data)} records")

    print("Sending cleaned data to Kafka...")
    send_to_kafka("declaration_topic", decl_data)
    send_to_kafka("elspot_topic", elspot_data)

    producer.flush()
    producer.close()
    print("All records sent successfully!")