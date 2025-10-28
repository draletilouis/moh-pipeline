-- Additional database initialization for Uganda Health Pipeline
-- This runs after the schema is created

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_indicator_date_composite
ON health.fact_indicator_values(indicator_id, date_id);

CREATE INDEX IF NOT EXISTS idx_fact_value
ON health.fact_indicator_values(value);

CREATE INDEX IF NOT EXISTS idx_fact_period
ON health.dim_date(period_label);

-- Grant permissions (if needed for other services)
-- GRANT SELECT ON ALL TABLES IN SCHEMA health TO metabase_user;

-- Add any additional constraints or validations
ALTER TABLE health.fact_indicator_values
ADD CONSTRAINT check_positive_value
CHECK (value >= 0);

-- Create a view for easy querying
CREATE OR REPLACE VIEW health.indicator_summary AS
SELECT
    i.indicator_name,
    i.category,
    COUNT(f.fact_id) as total_measurements,
    ROUND(AVG(f.value), 2) as avg_value,
    ROUND(STDDEV(f.value), 2) as std_dev,
    MIN(f.value) as min_value,
    MAX(f.value) as max_value,
    COUNT(DISTINCT d.period_label) as periods_covered
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
JOIN health.dim_date d ON f.date_id = d.date_id
GROUP BY i.indicator_name, i.category, i.indicator_id;

-- Create a materialized view for performance (optional)
-- CREATE MATERIALIZED VIEW health.monthly_summary AS
-- SELECT
--     DATE_TRUNC('month', d.date_value) as month,
--     i.indicator_name,
--     AVG(f.value) as monthly_avg,
--     COUNT(*) as measurements
-- FROM health.fact_indicator_values f
-- JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
-- JOIN health.dim_date d ON f.date_id = d.date_id
-- WHERE d.date_value IS NOT NULL
-- GROUP BY DATE_TRUNC('month', d.date_value), i.indicator_name
-- ORDER BY month, indicator_name;

