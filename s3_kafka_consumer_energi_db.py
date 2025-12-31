import os, json, time
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPICS = ["declaration_topic", "elspot_topic"]

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def connect_db():
    while True:
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
        except:
            time.sleep(2)

conn = connect_db()
cur = conn.cursor()

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset="earliest",
)

for msg in consumer:
    p = msg.value
    topic = msg.topic

    hour = p.get("HourDK") or p.get("HourUTC")
    price_area = p.get("PriceArea")

    if not hour or not price_area:
        continue

    hour = datetime.fromisoformat(hour.replace("Z", "+00:00"))

    production_type = None
    production_mwh = None
    price_eur = None

    if topic == "declaration_topic":
        production_type = p.get("ProductionType")
        production_mwh = p.get("Production_MWh")

    if topic == "elspot_topic":
        price_eur = p.get("SpotPriceEUR")

    cur.execute("""
        INSERT INTO energi_hourly
        (hour, price_area, production_type, production_mwh, price_eur, source_topic)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        hour, price_area,
        production_type,
        production_mwh,
        price_eur,
        topic
    ))
    conn.commit()
