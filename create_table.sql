CREATE TABLE IF NOT EXISTS energi_hourly (
    id BIGSERIAL PRIMARY KEY,
    hour TIMESTAMPTZ NOT NULL,
    price_area TEXT NOT NULL,
    production_type TEXT,
    production_mwh DOUBLE PRECISION,
    price_eur DOUBLE PRECISION,
    source_topic TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hour_area
ON energi_hourly (hour DESC, price_area);
