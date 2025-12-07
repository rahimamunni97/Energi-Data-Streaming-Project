CREATE TABLE energi_records (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    price_area VARCHAR(10),
    production_type VARCHAR(50),
    co2_per_kwh FLOAT,
    production_mwh FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
