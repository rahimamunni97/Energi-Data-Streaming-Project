import json
import os
import requests
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from .env file.
# This is mainly used to keep Kafka configuration separate from code.
load_dotenv()

# Kafka producer configuration.
# The producer sends JSON-encoded messages to Kafka topics.
producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Energinet Data Service API endpoints used in the project
DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"



# Data Cleaning Function
def clean_record(record):
    """
    Perform basic validation and normalization on a single record.

    This function was introduced after observing that raw API responses
    sometimes contain inconsistent formatting or unexpected values.
    Only records that pass these checks are forwarded to Kafka.
    """
    if not record:
        return None

    # Normalize PriceArea values to a consistent format (e.g. DK1, DK2)
    if "PriceArea" in record and record["PriceArea"]:
        record["PriceArea"] = record["PriceArea"].strip().upper()
    else:
        # Skip records without a valid price area
        return None

    # Normalize timestamp format to ISO-style (used later in storage)
    if "HourUTC" in record:
        try:
            record["HourUTC"] = record["HourUTC"].replace(" ", "T")
        except Exception:
            # Skip record if timestamp format cannot be fixed
            return None

    # Filter out invalid or negative price values
    if "SpotPriceEUR" in record:
        try:
            if float(record["SpotPriceEUR"]) < 0:
                return None
        except Exception:
            # Skip records with non-numeric price values
            return None

    return record



# Fetch data from Energinet API
def fetch_data(url, limit=10):
    """
    Fetch a small batch of records from the given Energinet API endpoint.

    This function is used both for testing and for controlled ingestion,
    avoiding large API responses during development.
    """
    resp = requests.get(url, params={"limit": limit})
    data = resp.json()
    return data.get("records", [])



# Send cleaned records to Kafka
def send_to_kafka(topic, records):
    """
    Clean records and publish valid entries to the specified Kafka topic.

    Records that fail validation are skipped to prevent malformed data
    from entering the streaming pipeline.
    """
    for record in records:
        # Some API responses wrap actual fields inside a "fields" object
        fields = record.get("fields", record)

        cleaned = clean_record(fields)

        if cleaned:
            producer.send(topic, cleaned)
            print(f"Sent record to {topic}: {cleaned.get('PriceArea')}")
            # Small delay added to avoid overwhelming the broker during testing
            time.sleep(1)
        else:
            print("Skipped invalid record")



# Main execution block
if __name__ == "__main__":
    # Fetch and inspect sample production data
    print("Fetching DeclarationProduction sample...")
    decl_data = fetch_data(DECLARATION_URL)
    print(f"Got {len(decl_data)} records")

    # Fetch and inspect sample price data
    print("Fetching Elspotprices sample...")
    elspot_data = fetch_data(ELSPOT_URL)
    print(f"Got {len(elspot_data)} records")

    # Publish cleaned records to Kafka topics
    print("Sending cleaned data to Kafka...")
    send_to_kafka("declaration_topic", decl_data)
    send_to_kafka("elspot_topic", elspot_data)

    # Ensure all messages are delivered before shutting down
    producer.flush()
    producer.close()

    print("All records sent successfully!")
