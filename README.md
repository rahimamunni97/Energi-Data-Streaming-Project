1. Project Overview

This project demonstrates an end-to-end data streaming pipeline using real-time Danish electricity market data from EnergiDataService. The system collects external API data, streams it through Kafka, stores it in a PostgreSQL database, and visualizes it using a Streamlit dashboard.

The implementation showcases a modern data engineering workflow, including message streaming, database integration, containerization with Docker, and interactive data presentation.

2. System Architecture

The project follows the pipeline below:

EnergiDataService API → Kafka Producer → Kafka Broker → Kafka Consumer → PostgreSQL Database → Streamlit Dashboard


Components

Producer: Downloads data from EnergiDataService and sends it to Kafka topics.

Kafka: Receives streaming messages.

Consumer: Listens to Kafka topics and inserts messages into a PostgreSQL table.

PostgreSQL: Stores structured data.

Streamlit: Displays tables and visualizations based on stored records.

### 3. Repository Structure

```text
Energi-Data-Streaming-Project/
│
├── docker-compose.yml              # Kafka, Zookeeper, PostgreSQL containers
├── create_table.sql                # SQL script for database table
│
├── s1_test_apis.py                 # Test EnergiDataService API connectivity
├── s2_kafka_producer.py            # Kafka Producer
├── s3_kafka_consumer_energi_db.py  # Kafka Consumer → PostgreSQL
├── s4_streamlit_dashboard.py       # Streamlit dashboard
│
├── requirements.txt                # Python dependencies
├── Dockerfile.consumer             # Container for Kafka consumer (optional)
└── k8s/                            # Kubernetes deployment files (optional)
```

4. Installation and Setup
4.1 Clone the Repository
git clone https://github.com/rahimamunni97/Energi-Data-Streaming-Project.git
cd Energi-Data-Streaming-Project

4.2 Optional: Create a Python Virtual Environment
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate # macOS/Linux

4.3 Install Dependencies
pip install -r requirements.txt

4.4 Create the .env Configuration File

In the project root, create a file named .env:

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPICS=declaration_topic,elspot_topic

DB_HOST=localhost
DB_PORT=55432
DB_NAME=energi_data
DB_USER=postgres
DB_PASSWORD=postgres

5. Running the Pipeline
5.1 Start Kafka, Zookeeper, and PostgreSQL
docker-compose up -d

5.2 Create PostgreSQL Table

Enter the database container:

docker exec -it energi_postgres psql -U postgres -d energi_data


Run the SQL commands inside create_table.sql, then exit:

\q

5.3 Start the Kafka Consumer

(Consumes records and writes to PostgreSQL)

python s3_kafka_consumer_energi_db.py

5.4 In a New Terminal, Run the Kafka Producer

(Fetches API data and sends to Kafka topics)

python s2_kafka_producer.py

5.5 Verify Data Insertion
docker exec -it energi_postgres \
 psql -U postgres -d energi_data -c "SELECT * FROM energi_records LIMIT 5;"

6. Running the Streamlit Dashboard

To launch the visualization dashboard:

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

PostgreSQL
docker exec -it energi_postgres psql -U postgres
\c energi_data
SELECT COUNT(*) FROM energi_records;

SELECT COUNT(*) FROM energi_rejected;

SELECT topic, reason, created_at
FROM energi_rejected
ORDER BY created_at DESC
LIMIT 5;

SELECT *
FROM energi_ingest_metrics
ORDER BY metric_time DESC
LIMIT 5;

SELECT COUNT(DISTINCT event_id), COUNT(*)
FROM energi_records;


python s3_kafka_consumer_energi_db.py
python s2_kafka_producer.py
python -m streamlit run s4_streamlit_dashboard.py
