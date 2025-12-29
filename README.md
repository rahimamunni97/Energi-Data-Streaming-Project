# ⚡ Energi Data Streaming Project

This project shows how to collect and process **real-time Danish energy data** using:
- **Kafka** for live streaming  
- **PostgreSQL** for storage  
- **Streamlit** for visualization  

You will stream data from [EnergiDataService.dk](https://www.energidataservice.dk/), store it in a database, and display it on a live dashboard.

---

## 🧩 What You’ll Build

EnergiDataService API → Kafka → PostgreSQL → Streamlit Dashboard


### 💡 Example Flow:
1. **Producer (Python)** gets data from the Energi API  
2. **Kafka** streams that data in real-time  
3. **Consumer (Python)** receives the data and saves it in PostgreSQL  
4. **Streamlit Dashboard** displays the latest data visually  

---

## ⚙️ Tools Used

| Component | Purpose |
|------------|----------|
| **Python** | Main language for producer, consumer, and dashboard |
| **Kafka** | Handles real-time data streaming |
| **Zookeeper** | Required for Kafka to run |
| **PostgreSQL** | Stores the energy data |
| **Docker** | Runs Kafka, Zookeeper, and PostgreSQL easily |
| **Streamlit** | Creates a simple live dashboard |

---

## 🏗️ Folder Structure

Energi_project/
│
├── docker-compose.yml # Starts Kafka, Zookeeper, PostgreSQL
├── s1_test_apis.py # Tests EnergiDataService APIs
├── s2_kafka_producer_energi.py # Sends API data to Kafka topics
├── s3_kafka_consumer_energi.py # Reads data from Kafka and saves to PostgreSQL
├── s4_streamlit_dashboard.py # Displays data on Streamlit dashboard
├── create_table_energi.sql # SQL to create table in PostgreSQL
└── README.md # Project guide


---

## 🚀 Step-by-Step Setup

### 1️⃣ Copy env and Install Python deps
In `Energi_project`:
```
cp env.sample .env   # adjust if needed
pip install -r requirements.txt
```

### 2️⃣ Start Docker Services
Make sure **Docker Desktop** is running. Then run:

```bash
docker-compose up -d

You should see 3 running containers:
energi_zookeeper
energi_kafka
energi_postgres

3️⃣ Create PostgreSQL Table
docker exec -it energi_postgres psql -U postgres -d energi_data

Then inside PostgreSQL, run:
CREATE TABLE energi_records (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    price_area VARCHAR(10),
    production_type VARCHAR(50),
    co2_per_kwh FLOAT,
    production_mwh FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
Exit with:
\q
4️⃣ Run Kafka Producer
This script gets data from the Energi API and sends it to Kafka.
python s2_kafka_producer.py
5️⃣ Run Kafka Consumer
This script reads from Kafka and saves data to PostgreSQL.
python s3_kafka_consumer.py
6️⃣ Check Saved Data

Run this command to view what’s in the database:
docker exec -it energi_postgres psql -U postgres -d energi_data -c "SELECT * FROM energi_records;"
7️⃣ Launch Streamlit Dashboard
Finally, launch the Streamlit app:
python -m streamlit run s4_streamlit_dashboard.py
Then open in your browser:
👉 http://localhost:8501

You’ll see:

A table of latest energy data

A bar chart of CO₂ emissions by Price Area

A “Refresh” button to update the view


Docker Images Used:
https://drive.google.com/file/d/1Rh22rGfutJMBvlq2GgeQBXjOyPHuqAdd/view?usp=drive_link

---

## 🔧 Environment variables (via `.env`)
Copy `env.sample` to `.env` in the project root and adjust if needed. Defaults:
- Kafka: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `KAFKA_TOPICS=declaration_topic,elspot_topic`
- Postgres: `DB_HOST=localhost`, `DB_PORT=55432`, `DB_NAME=energi_data`, `DB_USER=postgres`, `DB_PASSWORD=postgres`


## Data Cleaning and Validation

To ensure data quality, the project applies several cleaning steps:

### At Producer Level
- Removal of empty or malformed records.
- Conversion of timestamps to ISO format.
- Validation of numeric fields (e.g., negative prices removed).
- Normalisation of categorical fields such as PriceArea.

### At Consumer Level
- Schema validation before inserting messages into PostgreSQL.
- Automatic skipping of invalid or corrupted messages.

### At Dashboard Level
- Dropping missing records.
- Removing statistical outliers.
- Aggregating data for clearer visual interpretation.
