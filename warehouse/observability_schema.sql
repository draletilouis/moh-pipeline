-- Observability & Data Lineage Schema
-- Tracks pipeline execution, data quality, and lineage

-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS metadata;

-- =====================================================
-- 1. PIPELINE RUN TRACKING
-- =====================================================
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name TEXT NOT NULL,
    pipeline_stage TEXT, -- 'ingestion', 'transform', 'load'
    source_file TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    status TEXT CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    records_input INT,
    records_processed INT,
    records_loaded INT,
    records_rejected INT,
    execution_duration_seconds NUMERIC,
    error_message TEXT,
    error_details JSONB,
    metadata JSONB -- Store any additional context
);

-- Index for querying recent runs
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
ON metadata.pipeline_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
ON metadata.pipeline_runs(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline
ON metadata.pipeline_runs(pipeline_name, started_at DESC);

-- =====================================================
-- 2. DATA QUALITY METRICS
-- =====================================================
CREATE TABLE IF NOT EXISTS metadata.data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    check_category TEXT, -- 'completeness', 'validity', 'consistency', 'timeliness'
    table_name TEXT,
    column_name TEXT,
    passed BOOLEAN NOT NULL,
    metric_value NUMERIC,
    threshold_value NUMERIC,
    row_count INT,
    failure_count INT,
    details JSONB,
    checked_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_run
ON metadata.data_quality_metrics(run_id);

CREATE INDEX IF NOT EXISTS idx_quality_check
ON metadata.data_quality_metrics(check_name, checked_at DESC);

-- =====================================================
-- 3. FIELD-LEVEL LINEAGE
-- =====================================================
CREATE TABLE IF NOT EXISTS metadata.field_lineage (
    lineage_id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    target_schema TEXT DEFAULT 'health',
    target_table TEXT NOT NULL,
    target_column TEXT NOT NULL,
    source_file TEXT,
    source_sheet TEXT,
    source_column TEXT,
    transformation_logic TEXT,
    transformation_type TEXT, -- 'direct_copy', 'unpivot', 'aggregate', 'derived'
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_target
ON metadata.field_lineage(target_table, target_column);

CREATE INDEX IF NOT EXISTS idx_lineage_source
ON metadata.field_lineage(source_file);

-- =====================================================
-- 4. SOURCE FILE TRACKING
-- =====================================================
CREATE TABLE IF NOT EXISTS metadata.source_files (
    file_id SERIAL PRIMARY KEY,
    file_path TEXT UNIQUE NOT NULL,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL, -- MD5/SHA256 for change detection
    file_size_bytes BIGINT,
    sheet_count INT,
    row_count INT,
    column_count INT,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_processed TIMESTAMP,
    processing_count INT DEFAULT 1,
    schema_fingerprint JSONB,
    status TEXT CHECK (status IN ('new', 'processed', 'failed', 'archived'))
);

CREATE INDEX IF NOT EXISTS idx_source_files_hash
ON metadata.source_files(file_hash);

CREATE INDEX IF NOT EXISTS idx_source_files_status
ON metadata.source_files(status);

-- =====================================================
-- 5. DATA QUALITY RULES CONFIGURATION
-- =====================================================
CREATE TABLE IF NOT EXISTS metadata.quality_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name TEXT UNIQUE NOT NULL,
    rule_category TEXT,
    table_name TEXT,
    column_name TEXT,
    rule_type TEXT, -- 'threshold', 'range', 'pattern', 'custom_sql'
    rule_config JSONB, -- Store rule parameters
    is_active BOOLEAN DEFAULT TRUE,
    severity TEXT CHECK (severity IN ('critical', 'warning', 'info')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Example quality rules
INSERT INTO metadata.quality_rules (rule_name, rule_category, table_name, rule_type, rule_config, severity)
VALUES
    ('completeness_indicator', 'completeness', 'fact_indicator_values', 'threshold',
     '{"column": "indicator_id", "threshold": 0.99, "check": "not_null"}', 'critical'),
    ('completeness_value', 'completeness', 'fact_indicator_values', 'threshold',
     '{"column": "value", "threshold": 0.95, "check": "not_null"}', 'critical'),
    ('validity_value_range', 'validity', 'fact_indicator_values', 'range',
     '{"column": "value", "min": 0, "max": 1000000000}', 'warning'),
    ('consistency_foreign_keys', 'consistency', 'fact_indicator_values', 'custom_sql',
     '{"check": "foreign_key_integrity"}', 'critical')
ON CONFLICT (rule_name) DO NOTHING;

-- =====================================================
-- 6. VIEWS FOR MONITORING
-- =====================================================

-- Recent pipeline runs summary
CREATE OR REPLACE VIEW metadata.v_recent_pipeline_runs AS
SELECT
    run_id,
    pipeline_name,
    pipeline_stage,
    source_file,
    started_at,
    completed_at,
    status,
    records_processed,
    records_loaded,
    records_rejected,
    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds,
    error_message
FROM metadata.pipeline_runs
ORDER BY started_at DESC
LIMIT 100;

-- Pipeline success rate by pipeline
CREATE OR REPLACE VIEW metadata.v_pipeline_health AS
SELECT
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_seconds,
    MAX(started_at) as last_run_at
FROM metadata.pipeline_runs
WHERE started_at > NOW() - INTERVAL '30 days'
GROUP BY pipeline_name;

-- Data quality trend
CREATE OR REPLACE VIEW metadata.v_quality_trend AS
SELECT
    DATE_TRUNC('day', checked_at) as check_date,
    check_category,
    COUNT(*) as total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM metadata.data_quality_metrics
WHERE checked_at > NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', checked_at), check_category
ORDER BY check_date DESC, check_category;

-- Failed quality checks summary
CREATE OR REPLACE VIEW metadata.v_failed_quality_checks AS
SELECT
    dqm.check_name,
    dqm.check_category,
    dqm.table_name,
    dqm.column_name,
    pr.source_file,
    dqm.metric_value,
    dqm.threshold_value,
    dqm.details,
    dqm.checked_at,
    pr.pipeline_name
FROM metadata.data_quality_metrics dqm
JOIN metadata.pipeline_runs pr ON dqm.run_id = pr.run_id
WHERE dqm.passed = FALSE
ORDER BY dqm.checked_at DESC
LIMIT 100;

-- =====================================================
-- 7. HELPER FUNCTIONS
-- =====================================================

-- Function to get lineage for a specific field
CREATE OR REPLACE FUNCTION metadata.get_field_lineage(
    p_table TEXT,
    p_column TEXT
)
RETURNS TABLE (
    source_file TEXT,
    source_column TEXT,
    transformation_logic TEXT,
    last_updated TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        fl.source_file,
        fl.source_column,
        fl.transformation_logic,
        fl.recorded_at
    FROM metadata.field_lineage fl
    WHERE fl.target_table = p_table
      AND fl.target_column = p_column
    ORDER BY fl.recorded_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate data quality score
CREATE OR REPLACE FUNCTION metadata.get_quality_score(p_run_id UUID)
RETURNS NUMERIC AS $$
DECLARE
    v_score NUMERIC;
BEGIN
    SELECT
        ROUND(100.0 *
            SUM(CASE WHEN passed THEN 1 ELSE 0 END)::NUMERIC /
            NULLIF(COUNT(*), 0), 2)
    INTO v_score
    FROM metadata.data_quality_metrics
    WHERE run_id = p_run_id;

    RETURN COALESCE(v_score, 0);
END;
$$ LANGUAGE plpgsql;

COMMENT ON SCHEMA metadata IS 'Schema for pipeline observability, data lineage, and quality tracking';
COMMENT ON TABLE metadata.pipeline_runs IS 'Tracks every pipeline execution with metrics and status';
COMMENT ON TABLE metadata.data_quality_metrics IS 'Stores results of automated data quality checks';
COMMENT ON TABLE metadata.field_lineage IS 'Tracks where each field in the warehouse comes from';
COMMENT ON TABLE metadata.source_files IS 'Catalog of all source files processed';
COMMENT ON TABLE metadata.quality_rules IS 'Configuration for automated quality checks';
