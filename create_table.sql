CREATE TABLE IF NOT EXISTS energi_records (
    id SERIAL PRIMARY KEY,
    topic TEXT,
    price_area VARCHAR(10),
    payload JSONB,
    source VARCHAR(50),
    production_type VARCHAR(50),
    co2_per_kwh FLOAT,
    production_mwh FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_id TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_energi_records_event_id
ON energi_records(event_id);

-- 1) Store rejected / invalid messages (audit)
CREATE TABLE IF NOT EXISTS energi_rejected (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 2) Ingestion metrics
CREATE TABLE IF NOT EXISTS energi_ingest_metrics (
  id BIGSERIAL PRIMARY KEY,
  metric_time TIMESTAMPTZ DEFAULT now(),
  topic TEXT NOT NULL,
  valid_count INT NOT NULL,
  invalid_count INT NOT NULL,
  duplicate_count INT NOT NULL
);
