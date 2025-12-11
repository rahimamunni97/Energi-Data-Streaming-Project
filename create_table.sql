CREATE TABLE energi_records (
    id SERIAL PRIMARY KEY,
    topic TEXT,
    price_area VARCHAR(10),
    payload JSONB,
    source VARCHAR(50),
    production_type VARCHAR(50),
    co2_per_kwh FLOAT,
    production_mwh FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
