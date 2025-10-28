-- Warehouse schema for Uganda Health Sector Indicators

-- Create schema
CREATE SCHEMA IF NOT EXISTS health;

-- dim_date
CREATE TABLE IF NOT EXISTS health.dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT,
    period_label TEXT UNIQUE, -- e.g., "2016/17"
    date_value DATE
);

-- dim_indicator
CREATE TABLE IF NOT EXISTS health.dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    indicator_key TEXT UNIQUE,
    indicator_name TEXT UNIQUE,
    category TEXT
);

-- dim_location (if sheet contains facility/region)
CREATE TABLE IF NOT EXISTS health.dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name TEXT,
    district TEXT,
    region TEXT
);

-- fact_indicator_values
CREATE TABLE IF NOT EXISTS health.fact_indicator_values (
    fact_id SERIAL PRIMARY KEY,
    indicator_id INT REFERENCES health.dim_indicator(indicator_id),
    date_id INT REFERENCES health.dim_date(date_id),
    location_id INT REFERENCES health.dim_location(location_id),
    value NUMERIC,
    units TEXT,
    notes TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_indicator_date ON health.fact_indicator_values(indicator_id, date_id);
