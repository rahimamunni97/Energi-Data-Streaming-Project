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
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# APIs
DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"

def fetch_data(url, limit=5):
    resp = requests.get(url, params={'limit': limit})
    data = resp.json()
    if 'records' in data:
        return data['records']
    else:
        print("No 'records' field found in API response")
        return []

def send_to_kafka(topic, records):
    for record in records:
        fields = record.get('fields', record)
        producer.send(topic, fields)
        print(f" Sent record to {topic}: {fields.get('PriceArea', 'N/A')}")
        time.sleep(1)

if __name__ == "__main__":
    print(" Fetching DeclarationProduction sample...")
    decl_data = fetch_data(DECLARATION_URL)
    print(f"Got {len(decl_data)} records")

    print("\n Fetching Elspotprices sample...")
    elspot_data = fetch_data(ELSPOT_URL)
    print(f"Got {len(elspot_data)} records")

    print("\n Sending to Kafka...")
    send_to_kafka("declaration_topic", decl_data)
    send_to_kafka("elspot_topic", elspot_data)

    producer.flush()
    producer.close()
    print("\nAll records sent successfully!")
