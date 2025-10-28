-- ============================================================================
-- UGANDA HEALTH DATA ANALYSIS QUERIES
-- PostgreSQL queries to explore the loaded health sector data
-- Database: uganda_health
-- Schema: health
-- ============================================================================

-- ============================================================================
-- 1. BASIC EXPLORATION QUERIES
-- ============================================================================

-- Check all tables and their row counts
SELECT
    schemaname,
    tablename,
    tableowner,
    tablespace,
    hasindexes,
    hasrules,
    hastriggers
FROM pg_tables
WHERE schemaname = 'health'
ORDER BY tablename;

-- Quick overview of data volumes
SELECT
    'dim_indicator' AS table_name,
    COUNT(*) AS record_count
FROM health.dim_indicator
UNION ALL
SELECT
    'dim_date',
    COUNT(*)
FROM health.dim_date
UNION ALL
SELECT
    'fact_indicator_values',
    COUNT(*)
FROM health.fact_indicator_values;

-- Sample of indicators and their types
SELECT
    indicator_name,
    indicator_key,
    category
FROM health.dim_indicator
ORDER BY indicator_name
LIMIT 20;

-- Time periods available
SELECT
    period_label,
    year,
    date_value
FROM health.dim_date
ORDER BY year, period_label;

-- ============================================================================
-- 2. DATA QUALITY AND VALIDATION QUERIES
-- ============================================================================

-- Check for data completeness
SELECT
    COUNT(*) as total_records,
    COUNT(CASE WHEN value IS NOT NULL THEN 1 END) as non_null_values,
    COUNT(CASE WHEN value = 0 THEN 1 END) as zero_values,
    COUNT(CASE WHEN value < 0 THEN 1 END) as negative_values,
    ROUND(AVG(value), 2) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value
FROM health.fact_indicator_values;

-- Check for duplicate records
SELECT
    indicator_id,
    date_id,
    value,
    COUNT(*) as duplicate_count
FROM health.fact_indicator_values
GROUP BY indicator_id, date_id, value
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Validate foreign key relationships
SELECT
    'Orphaned facts (no indicator)' as issue,
    COUNT(*) as count
FROM health.fact_indicator_values f
LEFT JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.indicator_id IS NULL
UNION ALL
SELECT
    'Orphaned facts (no date)',
    COUNT(*)
FROM health.fact_indicator_values f
LEFT JOIN health.dim_date d ON f.date_id = d.date_id
WHERE d.date_id IS NULL;

-- ============================================================================
-- 3. BASIC ANALYTICS QUERIES
-- ============================================================================

-- Top 10 indicators by total value
SELECT
    i.indicator_name,
    COUNT(*) as measurement_count,
    ROUND(SUM(f.value), 2) as total_value,
    ROUND(AVG(f.value), 2) as avg_value,
    MIN(f.value) as min_value,
    MAX(f.value) as max_value
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_name, i.indicator_id
ORDER BY total_value DESC
LIMIT 10;

-- Indicators with highest average values
SELECT
    i.indicator_name,
    COUNT(*) as measurements,
    ROUND(AVG(f.value), 2) as avg_value,
    ROUND(STDDEV(f.value), 2) as std_dev
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_name, i.indicator_id
HAVING COUNT(*) > 5  -- At least 5 measurements
ORDER BY avg_value DESC
LIMIT 15;

-- ============================================================================
-- 4. TIME SERIES ANALYSIS QUERIES
-- ============================================================================

-- Overall trends by time period
SELECT
    d.period_label,
    d.year,
    COUNT(*) as total_measurements,
    ROUND(AVG(f.value), 2) as avg_value,
    ROUND(SUM(f.value), 2) as total_value,
    COUNT(DISTINCT f.indicator_id) as unique_indicators
FROM health.fact_indicator_values f
JOIN health.dim_date d ON f.date_id = d.date_id
GROUP BY d.period_label, d.year, d.date_id
ORDER BY d.year, d.period_label;

-- Year-over-year growth for each indicator
WITH yearly_totals AS (
    SELECT
        i.indicator_name,
        d.year,
        SUM(f.value) as total_value,
        COUNT(*) as measurement_count
    FROM health.fact_indicator_values f
    JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
    JOIN health.dim_date d ON f.date_id = d.date_id
    GROUP BY i.indicator_name, d.year
)
SELECT
    indicator_name,
    year,
    total_value,
    measurement_count,
    LAG(total_value) OVER (PARTITION BY indicator_name ORDER BY year) as prev_year_value,
    CASE
        WHEN LAG(total_value) OVER (PARTITION BY indicator_name ORDER BY year) > 0
        THEN ROUND(
            (total_value - LAG(total_value) OVER (PARTITION BY indicator_name ORDER BY year))
            / LAG(total_value) OVER (PARTITION BY indicator_name ORDER BY year) * 100, 2
        )
        ELSE NULL
    END as yoy_growth_percent
FROM yearly_totals
ORDER BY indicator_name, year;

-- ============================================================================
-- 5. DISTRIBUTION AND STATISTICAL ANALYSIS
-- ============================================================================

-- Value distribution analysis
SELECT
    CASE
        WHEN value = 0 THEN 'Zero'
        WHEN value > 0 AND value <= 10 THEN '1-10'
        WHEN value > 10 AND value <= 50 THEN '11-50'
        WHEN value > 50 AND value <= 100 THEN '51-100'
        WHEN value > 100 AND value <= 500 THEN '101-500'
        WHEN value > 500 AND value <= 1000 THEN '501-1000'
        WHEN value > 1000 AND value <= 5000 THEN '1001-5000'
        WHEN value > 5000 AND value <= 10000 THEN '5001-10000'
        ELSE '10000+'
    END as value_range,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM health.fact_indicator_values
GROUP BY
    CASE
        WHEN value = 0 THEN 'Zero'
        WHEN value > 0 AND value <= 10 THEN '1-10'
        WHEN value > 10 AND value <= 50 THEN '11-50'
        WHEN value > 50 AND value <= 100 THEN '51-100'
        WHEN value > 100 AND value <= 500 THEN '101-500'
        WHEN value > 500 AND value <= 1000 THEN '501-1000'
        WHEN value > 1000 AND value <= 5000 THEN '1001-5000'
        WHEN value > 5000 AND value <= 10000 THEN '5001-10000'
        ELSE '10000+'
    END
ORDER BY MIN(value);

-- Outlier detection (values more than 3 standard deviations from mean)
WITH stats AS (
    SELECT
        AVG(value) as mean_value,
        STDDEV(value) as stddev_value
    FROM health.fact_indicator_values
    WHERE value > 0  -- Exclude zeros for outlier detection
)
SELECT
    i.indicator_name,
    d.period_label,
    f.value,
    ROUND((f.value - s.mean_value) / s.stddev_value, 2) as z_score
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
JOIN health.dim_date d ON f.date_id = d.date_id
CROSS JOIN stats s
WHERE f.value > 0
  AND ABS(f.value - s.mean_value) / s.stddev_value > 3  -- More than 3 std devs
ORDER BY ABS(f.value - s.mean_value) / s.stddev_value DESC;

-- ============================================================================
-- 6. CORRELATION AND PATTERN ANALYSIS
-- ============================================================================

-- Indicators with most consistent values (lowest coefficient of variation)
SELECT
    i.indicator_name,
    COUNT(*) as measurements,
    ROUND(AVG(f.value), 2) as avg_value,
    ROUND(STDDEV(f.value), 2) as std_dev,
    ROUND(STDDEV(f.value) / NULLIF(AVG(f.value), 0) * 100, 2) as cv_percent
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_name, i.indicator_id
HAVING COUNT(*) >= 3  -- At least 3 measurements for meaningful stats
ORDER BY STDDEV(f.value) / NULLIF(AVG(f.value), 0) ASC
LIMIT 10;

-- Indicators with highest variability
SELECT
    i.indicator_name,
    COUNT(*) as measurements,
    ROUND(AVG(f.value), 2) as avg_value,
    ROUND(STDDEV(f.value), 2) as std_dev,
    ROUND(STDDEV(f.value) / NULLIF(AVG(f.value), 0) * 100, 2) as cv_percent
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_name, i.indicator_id
HAVING COUNT(*) >= 3
ORDER BY STDDEV(f.value) / NULLIF(AVG(f.value), 0) DESC
LIMIT 10;

-- ============================================================================
-- 7. ADVANCED QUERIES - TIME SERIES PATTERNS
-- ============================================================================

-- Monthly/quarterly patterns (if data allows)
SELECT
    d.period_label,
    EXTRACT(YEAR FROM d.date_value) as year,
    CASE
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 7 AND 9 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 10 AND 12 THEN 'Q4'
        ELSE 'Unknown'
    END as quarter,
    COUNT(*) as measurements,
    ROUND(AVG(f.value), 2) as avg_value
FROM health.fact_indicator_values f
JOIN health.dim_date d ON f.date_id = d.date_id
WHERE d.date_value IS NOT NULL
GROUP BY d.period_label, EXTRACT(YEAR FROM d.date_value),
    CASE
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 7 AND 9 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM d.date_value) BETWEEN 10 AND 12 THEN 'Q4'
        ELSE 'Unknown'
    END
ORDER BY year, quarter;

-- ============================================================================
-- 8. EXPORT AND REPORTING QUERIES
-- ============================================================================

-- Create a summary report for all indicators
SELECT
    i.indicator_name,
    d.period_label,
    f.value,
    d.year,
    CASE
        WHEN f.value = 0 THEN 'Zero'
        WHEN f.value > 0 AND f.value <= 100 THEN 'Low'
        WHEN f.value > 100 AND f.value <= 1000 THEN 'Medium'
        WHEN f.value > 1000 THEN 'High'
        ELSE 'Unknown'
    END as value_category
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
JOIN health.dim_date d ON f.date_id = d.date_id
ORDER BY i.indicator_name, d.year, d.period_label;

-- ============================================================================
-- 9. PERFORMANCE AND INDEX ANALYSIS
-- ============================================================================

-- Check index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'health'
ORDER BY idx_scan DESC;

-- Query performance analysis
EXPLAIN ANALYZE
SELECT
    i.indicator_name,
    COUNT(*) as measurement_count,
    ROUND(AVG(f.value), 2) as avg_value
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_name, i.indicator_id
ORDER BY avg_value DESC
LIMIT 10;

-- ============================================================================
-- 10. USEFUL ONE-LINERS FOR EXPLORATION
-- ============================================================================

-- Random sample of 10 records
SELECT * FROM health.fact_indicator_values
ORDER BY RANDOM()
LIMIT 10;

-- Count distinct values for each column
SELECT
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT date_id) as unique_dates,
    COUNT(DISTINCT value) as unique_values
FROM health.fact_indicator_values;

-- Find indicators with most recent data
SELECT
    i.indicator_name,
    MAX(d.period_label) as latest_period,
    MAX(d.year) as latest_year,
    COUNT(*) as total_measurements
FROM health.fact_indicator_values f
JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
JOIN health.dim_date d ON f.date_id = d.date_id
GROUP BY i.indicator_name, i.indicator_id
ORDER BY latest_year DESC, latest_period DESC
LIMIT 20;

