# ⚡ EnergiFlow: Price–Production Analytics Platform

**EnergiFlow** is a real-time data streaming and analytics platform for electricity **price–production analysis**, built using modern **Big Data technologies**.  
The system ingests live energy market data from public APIs, processes it via **Kafka**, stores it in **PostgreSQL**, and visualizes insights through an interactive **Streamlit dashboard**.

This project demonstrates an **end-to-end Big Data pipeline**, including streaming ingestion, data validation, aggregation, visualization, and decision support.

---

## 📌 Project Objectives

- Stream electricity **spot prices** and **production data** in real time  
- Handle **multiple asynchronous Kafka data streams**  
- Store validated records in a relational database  
- Perform **daily aggregation and analytical joins**  
- Visualize trends and support **data-driven decisions**  
- Demonstrate real-world **Big Data system design**

---

## 🏗️ System Architecture

```EnergiDataService APIs
│
▼
Kafka Producer (Python)
│
▼
Apache Kafka
│
▼
Kafka Consumer (Python)
│
▼
PostgreSQL (energi_records)
│
▼
Streamlit Analytics Dashboard```


---

## 🔄 Data Sources

The platform consumes data from the **Energi Data Service (Denmark)**.

### 1️⃣ DeclarationProduction
- Electricity production by source (wind, gas, biomass, etc.)
- Metric: `Production (MWh)`
- Kafka topic: `declaration_topic`

### 2️⃣ ElspotPrices
- Electricity spot market prices
- Metric: `Spot Price (EUR)`
- Kafka topic: `elspot_topic`

⚠️ **Design Note**  
Price and production arrive in **separate Kafka streams** and do **not exist in the same event**.  
They are combined later using **daily aggregation and logical joins**.

---

## 🛠️ Technologies Used

```| Layer | Technology |
|-----|-----------|
| Programming | Python 3 |
| Streaming | Apache Kafka |
| Messaging | kafka-python |
| Database | PostgreSQL |
| Containerization | Docker |
| Visualization | Streamlit, Altair |
| API Access | Requests |
| Data Processing | Pandas |```

---

## 🗂️ Project Structure

```Energi_project/
│
├── s1_test_apis.py # API connectivity test
├── s2_kafka_producer.py # Kafka producer (price + production)
├── s3_kafka_consumer_energi_db.py # Kafka consumer → PostgreSQL
├── s4_streamlit_dashboard.py # Streamlit analytics dashboard
│
├── docker-compose.yml # Kafka, Zookeeper, PostgreSQL
├── requirements.txt # Python dependencies
├── .env # Environment variables
└── README.md # Project documentation```


---

## 🧪 Data Storage Model

All streaming data is stored in a single table:

### `energi_records`

| Column | Description |
|------|------------|
| `timestamp` | Event timestamp |
| `price_area` | Market area (DK1, DK2, DE, etc.) |
| `production_mwh` | Production amount (nullable) |
| `spot_price_eur` | Spot price (nullable) |
| `production_type` | Energy source |
| `source` | Kafka topic origin |
| `payload` | Raw JSON payload |

📌 Nullable fields are **expected** due to asynchronous streams.

---

## 📊 Streamlit Dashboard Features

### 1️⃣ Raw Data Tables
- Latest **Elspot price records**
- Latest **Declaration production records**

### 2️⃣ Daily Aggregated Table
- Daily **Total Production (MWh)**
- Daily **Average Spot Price (EUR)**
- Aggregated by **Date and Area**

### 3️⃣ Total Production Summary
- Cumulative production per area

### 4️⃣ Decision Support Table

| Decision | Logic |
|--------|------|
| BUY | Low price & high production |
| AVOID | High price & low production |
| MONITOR | Mixed market signals |

### 5️⃣ Price vs Production Comparison Graph
- Two **line charts** (price vs production)
- Daily aggregated comparison
- Interactive tooltips

---

## 🧠 Big Data Design Considerations

- Proper handling of **asynchronous streams**
- No incorrect row-level joins
- Aggregation before analytics
- Robust API error handling
- Modular and scalable architecture

---

## ▶️ How to Run the Project

### 1️⃣ Start Infrastructure
```bash
docker-compose up -d
2️⃣ Start Kafka Producer
python s2_kafka_producer.py

3️⃣ Start Kafka Consumer
python s3_kafka_consumer_energi_db.py

4️⃣ Launch Streamlit Dashboard
streamlit run s4_streamlit_dashboard.py

🎓 Academic Relevance

This project demonstrates core Big Data concepts:

Streaming data ingestion

Kafka producers and consumers

Data validation and quality

Real-time processing

Aggregation and analytics

Visualization and decision support

🧾 Key Learning Outcomes

Designing pipelines with non-overlapping streams

Handling real-world API instability

Separating ingestion from analytics

Building explainable dashboards

Translating raw streams into insights