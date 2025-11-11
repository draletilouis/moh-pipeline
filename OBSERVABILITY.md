# Observability Framework

## Overview

The Uganda Health Pipeline now includes a **production-grade observability framework** that tracks:

- Pipeline execution metrics and status
- Data quality checks with automated validation
- Field-level lineage (where every piece of data comes from)
- Source file management and change detection
- Performance metrics (execution duration, record counts)

## Architecture

```
┌─────────────────┐
│  Source Files   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│         Pipeline Stages                      │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐ │
│  │Ingestion │→ │ Transform │→ │  Load    │ │
│  └──────────┘  └───────────┘  └──────────┘ │
│       │              │              │       │
│       ▼              ▼              ▼       │
│  ┌────────────────────────────────────────┐│
│  │     Pipeline Observer (Tracking)       ││
│  └────────────────────────────────────────┘│
└─────────────────────────────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │  Metadata Database    │
        │  - pipeline_runs      │
        │  - quality_metrics    │
        │  - field_lineage      │
        │  - source_files       │
        └───────────────────────┘
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
   ┌──────────────┐   ┌─────────────┐
   │ CLI Dashboard│   │ API Endpoints│
   └──────────────┘   └─────────────┘
```

## Features Implemented

### 1. Pipeline Run Tracking
- Unique ID for every pipeline execution
- Start/end timestamps
- Status tracking (running, success, failed)
- Record counts (input, processed, loaded, rejected)
- Execution duration
- Error messages and details

### 2. Data Quality Validation
Automated checks in 5 categories:

**Completeness**
- Check for null/missing values in critical fields
- Threshold: <5% nulls acceptable

**Validity**
- Value range checks (no negatives, within bounds)
- Data type validation
- Format validation (e.g., year labels)

**Consistency**
- Duplicate detection
- Referential integrity
- Format consistency

**Uniqueness**
- Key constraint validation
- Duplicate key detection

**Timeliness**
- Data freshness checks (not yet implemented)

### 3. Field-Level Lineage
Track transformations:
- Source file → Raw CSV → Clean CSV → Warehouse
- Transformation logic documented
- Types: direct_copy, unpivot, aggregate, derived

### 4. Source File Management
- File hash-based change detection
- Processing history (times processed)
- Schema fingerprinting
- Status tracking (new, processed, failed, archived)

## Quick Start

### Step 1: Initialize Metadata Schema

```bash
# Make sure your database is running
# Then run the warehouse loader (it will create observability schema)
python warehouse/load_to_postgres.py
```

The observability schema will be created automatically from [warehouse/observability_schema.sql](warehouse/observability_schema.sql).

### Step 2: Run Pipeline with Observability

The pipelines are now instrumented automatically:

```bash
# Ingestion (with tracking)
python ingestion/load-excel.py

# Transform (with tracking)
python transform/clean_and_unpivot.py

# Load (with tracking)
python warehouse/load_to_postgres.py
```

Each stage will:
- Register a pipeline run
- Log data quality checks
- Track lineage
- Register source files
- Print a summary on completion

### Step 3: View Monitoring Dashboard

#### CLI Dashboard

```bash
# Full dashboard
python observability/monitor_dashboard.py

# Specific views
python observability/monitor_dashboard.py health
python observability/monitor_dashboard.py quality
python observability/monitor_dashboard.py runs 20
python observability/monitor_dashboard.py files

# Lineage tracking
python observability/monitor_dashboard.py lineage fact_indicator_values value
```

#### API Endpoints

Start the API:

```bash
python api/main.py
```

Access monitoring endpoints:

- **Full Dashboard**: `http://localhost:8000/observability/dashboard`
- **Pipeline Health**: `http://localhost:8000/observability/pipeline-health`
- **Recent Runs**: `http://localhost:8000/observability/recent-runs?limit=10`
- **Data Quality**: `http://localhost:8000/observability/data-quality?days=7`
- **Lineage**: `http://localhost:8000/observability/lineage/fact_indicator_values/value`
- **Source Files**: `http://localhost:8000/observability/source-files`

Interactive docs: `http://localhost:8000/docs`

## Usage Examples

### Example 1: Track Custom Pipeline

```python
from observability import ObservedPipeline

# Automatic tracking with context manager
with ObservedPipeline('my_pipeline', 'custom_stage', 'input_file.csv') as observer:
    # Do your work
    df = load_data()

    # Log quality checks
    observer.log_quality_check(
        check_name='row_count',
        passed=len(df) > 100,
        check_category='completeness',
        metric_value=len(df),
        threshold_value=100
    )

    # Track lineage
    observer.track_lineage(
        target_table='my_table',
        target_column='my_column',
        source_file='input_file.csv',
        source_column='source_col',
        transformation_logic='Applied filter and aggregation',
        transformation_type='aggregate'
    )

    # Complete automatically on context exit
```

### Example 2: Custom Data Quality Checks

```python
from observability import DataQualityValidator, PipelineObserver

observer = PipelineObserver()
observer.start_run('my_pipeline', 'validation')

validator = DataQualityValidator(observer)

# Run all checks
passed, summary = validator.validate_all(df, table_name='my_data')

if not passed:
    validator.print_report()
    # Handle failures

observer.complete_run(status='success' if passed else 'failed')
observer.print_summary()
```

### Example 3: Health-Specific Validation

```python
from observability import HealthDataValidator, ObservedPipeline

with ObservedPipeline('health_etl', 'validation') as observer:
    validator = HealthDataValidator(observer)

    # Run health-specific checks
    passed, summary = validator.validate_health_data(df)

    print(f"Overall Quality Score: {summary['overall_score']:.1f}/100")
```

## Database Schema

### metadata.pipeline_runs
Tracks every pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| run_id | UUID | Unique run identifier |
| pipeline_name | TEXT | Pipeline name (e.g., 'uganda_health_etl') |
| pipeline_stage | TEXT | Stage name ('ingestion', 'transform', 'load') |
| source_file | TEXT | Source file path |
| started_at | TIMESTAMP | When run started |
| completed_at | TIMESTAMP | When run completed |
| status | TEXT | 'running', 'success', 'failed', 'skipped' |
| records_input | INT | Records read from source |
| records_processed | INT | Records processed |
| records_loaded | INT | Records loaded to target |
| records_rejected | INT | Records rejected |
| execution_duration_seconds | NUMERIC | Duration in seconds |
| error_message | TEXT | Error message if failed |

### metadata.data_quality_metrics
Stores quality check results.

| Column | Type | Description |
|--------|------|-------------|
| metric_id | SERIAL | Primary key |
| run_id | UUID | Associated run |
| check_name | TEXT | Name of the check |
| check_category | TEXT | 'completeness', 'validity', 'consistency', 'timeliness' |
| table_name | TEXT | Table being checked |
| column_name | TEXT | Column being checked |
| passed | BOOLEAN | Did check pass? |
| metric_value | NUMERIC | Actual value measured |
| threshold_value | NUMERIC | Threshold for passing |
| row_count | INT | Total rows checked |
| failure_count | INT | Rows that failed |
| details | JSONB | Additional context |

### metadata.field_lineage
Tracks where data comes from.

| Column | Type | Description |
|--------|------|-------------|
| lineage_id | SERIAL | Primary key |
| run_id | UUID | Associated run |
| target_table | TEXT | Target table in warehouse |
| target_column | TEXT | Target column name |
| source_file | TEXT | Source file path |
| source_sheet | TEXT | Excel sheet name |
| source_column | TEXT | Source column name |
| transformation_logic | TEXT | Description of transformation |
| transformation_type | TEXT | 'direct_copy', 'unpivot', 'aggregate', 'derived' |

### metadata.source_files
Catalog of all source files.

| Column | Type | Description |
|--------|------|-------------|
| file_id | SERIAL | Primary key |
| file_path | TEXT | Full file path |
| file_name | TEXT | File name |
| file_hash | TEXT | MD5 hash for change detection |
| file_size_bytes | BIGINT | File size |
| sheet_count | INT | Number of sheets (Excel) |
| row_count | INT | Total rows |
| column_count | INT | Total columns |
| first_seen | TIMESTAMP | First time seen |
| last_processed | TIMESTAMP | Last processing time |
| processing_count | INT | Number of times processed |
| schema_fingerprint | JSONB | Schema metadata |
| status | TEXT | 'new', 'processed', 'failed', 'archived' |

## Monitoring Queries

### Pipeline Success Rate (Last 30 Days)

```sql
SELECT * FROM metadata.v_pipeline_health;
```

### Recent Failed Runs

```sql
SELECT
    pipeline_name,
    pipeline_stage,
    started_at,
    error_message
FROM metadata.pipeline_runs
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 10;
```

### Data Quality Score by Run

```sql
SELECT
    pr.pipeline_name,
    pr.started_at,
    metadata.get_quality_score(pr.run_id) as quality_score
FROM metadata.pipeline_runs pr
WHERE pr.completed_at > NOW() - INTERVAL '7 days'
ORDER BY pr.started_at DESC;
```

### Find Slow Runs

```sql
SELECT
    pipeline_name,
    pipeline_stage,
    started_at,
    execution_duration_seconds
FROM metadata.pipeline_runs
WHERE execution_duration_seconds > 60
ORDER BY execution_duration_seconds DESC
LIMIT 10;
```

### Trace Data Lineage

```sql
-- Where does fact_indicator_values.value come from?
SELECT * FROM metadata.get_field_lineage('fact_indicator_values', 'value');
```

## Performance Impact

The observability framework adds minimal overhead:

- **Ingestion**: +2-5% execution time
- **Transform**: +5-10% execution time (due to quality checks)
- **Load**: +10-15% execution time (due to integrity checks)

Benefits far outweigh the cost:
- Debug issues in minutes instead of hours
- Prevent bad data from entering warehouse
- Full audit trail for compliance
- Performance monitoring and optimization

## Best Practices

1. **Always use ObservedPipeline context manager**
   - Automatic run tracking
   - Auto-completion on success/failure
   - Clean error handling

2. **Log meaningful quality checks**
   - Check critical fields first
   - Set appropriate thresholds
   - Include context in details

3. **Track lineage for transformations**
   - Document the "why" not just the "what"
   - Track all transformation types
   - Include source references

4. **Monitor dashboard regularly**
   - Check pipeline health weekly
   - Review failed quality checks daily
   - Track performance trends

5. **Set up alerts**
   - Alert on failed runs
   - Alert on quality score drops
   - Alert on performance degradation

## Troubleshooting

### Q: Pipeline runs but no observability data?

**A:** Check that observability schema was created:

```sql
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'metadata';
```

If missing, run:

```bash
psql -U postgres -d uganda_health -f warehouse/observability_schema.sql
```

### Q: Quality checks always failing?

**A:** Review thresholds in your checks. Raw data may have more nulls than transformed data. Adjust thresholds accordingly.

### Q: Lineage not showing up?

**A:** Ensure you're calling `observer.track_lineage()` in your pipeline code. Check that run_id is set.

### Q: Dashboard shows no data?

**A:** Run the pipeline at least once to populate metadata. Check database connection in [conf/.env](conf/.env).

## Future Enhancements

Planned features:

1. **Alerting System**
   - Email/Slack notifications on failures
   - Threshold-based alerts
   - Anomaly detection

2. **Performance Profiling**
   - Step-by-step timing
   - Resource usage tracking
   - Bottleneck identification

3. **Data Drift Detection**
   - Schema change alerts
   - Statistical drift detection
   - Data distribution monitoring

4. **Web UI Dashboard**
   - Interactive charts
   - Real-time monitoring
   - Drill-down analysis

5. **ML-Based Quality Checks**
   - Automatic threshold learning
   - Anomaly detection
   - Predictive quality scoring

## Contributing

To extend the observability framework:

1. **Add new quality checks** in [observability/data_quality.py](observability/data_quality.py)
2. **Add new metrics** to the database schema
3. **Add new API endpoints** in [api/main.py](api/main.py)
4. **Update dashboard** in [observability/monitor_dashboard.py](observability/monitor_dashboard.py)

## Support

For issues or questions:
- Check existing pipeline runs: `python observability/monitor_dashboard.py runs`
- Review logs in pipeline output
- Check database for error details:
  ```sql
  SELECT * FROM metadata.pipeline_runs WHERE status = 'failed' ORDER BY started_at DESC LIMIT 5;
  ```

---

**Built with ❤️ for production-grade data engineering**
