# Uganda Health Sector Data Pipeline

Transform health sector performance data from Excel to queryable analytics with PostgreSQL and FastAPI.

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Overview

A production-ready data engineering platform that transforms Uganda's health sector performance data from raw Excel files into a structured, queryable analytics system. This pipeline processes **72,402 health performance measurements** across **152 indicators**, spanning from **2016/17 to 2019/20**, and serves them through a modern REST API.

### Key Features

- **Complete ETL Pipeline**: Extract from Excel → Transform & Clean → Load to Data Warehouse
- **Star Schema Design**: Optimized dimensional modeling for fast analytical queries
- **Production Observability**: 151+ automated data quality checks across 5 categories (completeness, validity, consistency, uniqueness, timeliness)
- **Field-Level Lineage**: Complete audit trail from Excel source cells to warehouse columns
- **Pipeline Monitoring**: Real-time tracking of execution metrics, success rates, and performance statistics
- **RESTful API**: FastAPI-powered endpoints with automatic OpenAPI documentation (15+ endpoints)
- **Data Quality Framework**: Automated validation with configurable thresholds and quality scoring
- **CLI Dashboard**: Command-line monitoring tools for pipeline health and data quality inspection
- **Hash-Based Change Detection**: Idempotent file processing with MD5 fingerprinting
- **Time Series Analysis**: Track health indicators over 4 fiscal years
- **Flexible Queries**: Filter, sort, paginate, and aggregate health metrics
- **Self-Documenting**: Interactive API docs at `/docs`

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   Source Data       │     │   ETL Pipeline      │     │   Observability     │
│   Excel Files       │────▶│   Python Scripts    │────▶│   Metadata Layer    │
│                     │     │                     │     │                     │
│ • 5 sheets         │     │ • load-excel.py     │     │ • Run Tracking      │
│ • 152 indicators   │     │ • clean_transform   │     │ • Quality Checks    │
│ • 72K+ records     │     │ • load_to_postgres  │     │ • Field Lineage     │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
                                      │                           │
                                      ▼                           ▼
                          ┌─────────────────────┐     ┌─────────────────────┐
                          │   Data Warehouse    │     │  Metadata Schema    │
                          │   PostgreSQL        │     │  PostgreSQL         │
                          │                     │     │                     │
                          │ • Star Schema       │     │ • pipeline_runs     │
                          │ • 72K+ records      │     │ • quality_metrics   │
                          │ • 4 dimensions      │     │ • field_lineage     │
                          │ • 1 fact table      │     │ • source_files      │
                          └─────────────────────┘     └─────────────────────┘
                                      │                           │
                                      ▼                           ▼
                          ┌─────────────────────────────────────────────────┐
                          │              REST API - FastAPI                 │
                          │                                                 │
                          │  Business Endpoints  │  Observability Endpoints │
                          │  • /health/*         │  • /observability/*      │
                          │  • 9 endpoints       │  • 6 endpoints           │
                          └─────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                     ▼
          ┌─────────────────┐                  ┌─────────────────┐
          │   BI Tools      │                  │   Monitoring    │
          │                 │                  │                 │
          │ • Metabase      │                  │ • CLI Dashboard │
          │ • Power BI      │                  │ • Quality Score │
          │ • Tableau       │                  │ • Lineage Query │
          └─────────────────┘                  └─────────────────┘
```

### Data Flow

1. **Ingestion**: Excel files parsed into structured CSV format with quality validation
2. **Transformation**: Data cleaned, normalized, and unpivoted for time series (with lineage tracking)
3. **Loading**: Processed data loaded into star schema warehouse with integrity checks
4. **Observability**: All pipeline stages tracked with execution metrics, quality scores, and lineage
5. **API Serving**: REST endpoints provide programmatic access to both business data and metadata
6. **Analytics**: BI tools and monitoring dashboards consume standardized data

## Technology Stack

- **Python 3.9+** - Core programming language used
- **pandas** - Data manipulation and analysis
- **SQLAlchemy** - ORM and database toolkit
- **FastAPI** - Modern, fast web framework for APIs
- **PostgreSQL 15** - Relational database with dimensional modeling
- **psycopg2** - PostgreSQL adapter for Python
- **uvicorn** - ASGI server for FastAPI

## Project Structure

```
uganda-health-pipeline/
├── 📂 data/
│   ├── source/                    # Input Excel files
│   ├── raw/                       # Extracted CSV files (5 sheets)
│   └── clean/                     # Processed and unpivoted data
├── 📂 ingestion/
│   └── load-excel.py              # Excel parsing and CSV extraction (with observability)
├── 📂 transform/
│   └── clean_and_unpivot.py       # Data cleaning and normalization (with quality checks)
├── 📂 warehouse/
│   ├── schema.sql                 # Dimensional warehouse schema
│   ├── observability_schema.sql   # Metadata schema for observability
│   └── load_to_postgres.py        # Database loading and optimization (with validation)
├── 📂 observability/
│   ├── __init__.py                # Module initialization
│   ├── pipeline_observer.py       # Core tracking engine for pipeline execution
│   ├── data_quality.py            # Data quality validation framework (151+ checks)
│   ├── monitor_dashboard.py       # CLI monitoring dashboard
│   └── init_observability.py      # Schema initialization script
├── 📂 api/
│   └── main.py                    # FastAPI application with 15+ endpoints
├── 📂 tests/
│   └── smoke_test.py              # Data quality and integration tests
├── 📂 scripts/
│   └── load_data.sh               # Pipeline orchestration script
├── 📂 conf/
│   └── .env                       # Environment configuration (database credentials)
├── 📄 requirements.txt            # Python dependencies
├── 📄 README.md                   # This guide
├── 📄 OBSERVABILITY.md            # Comprehensive observability documentation
└── 📄 ROADMAP.md                  # Future enhancements
```

## Quick Start

### Prerequisites

- Python 3.9+ installed
- PostgreSQL 15 running locally
- Source Excel file in `data/source/`

### 1. Setup Environment

   ```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/Mac)
source .venv/bin/activate

   # Install dependencies
   pip install -r requirements.txt
   ```

### 2. Initialize Database

   ```bash
# Create PostgreSQL database (Windows)
psql -U postgres -c "CREATE DATABASE uganda_health;"

# Or use pgAdmin to create the database

# Initialize observability metadata schema
python observability/init_observability.py
```

### 3. Run Pipeline

   ```bash
   # Step 1: Extract Excel to CSV (with tracking)
   python ingestion/load-excel.py

# Step 2: Transform and clean data (with quality checks)
   python transform/clean_and_unpivot.py

# Step 3: Load to PostgreSQL (with validation)
   python warehouse/load_to_postgres.py
```

### 3a. Monitor Pipeline (Optional)

   ```bash
# View full monitoring dashboard
python observability/monitor_dashboard.py

# Check pipeline health metrics
python observability/monitor_dashboard.py health

# View data quality summary
python observability/monitor_dashboard.py quality

# See recent pipeline runs
python observability/monitor_dashboard.py runs 20

# Query field lineage
python observability/monitor_dashboard.py lineage fact_indicator_values value
```

### 4. Start API Server

```bash
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

### 5. Access API

- **Interactive Docs**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc
- **API Root**: http://127.0.0.1:8000/

## Data Model

### Dimensional Schema

The warehouse implements a **star schema** optimized for analytical queries:

**Dimension Tables:**
- `dim_indicator` - 152 health performance metrics
- `dim_date` - Time periods (2016/17 through 2019/20)
- `dim_location` - Geographic entities (districts, regions)

**Fact Table:**
- `fact_indicator_values` - 72,402 measurements with foreign keys to dimensions

### Sample Indicators

- **Hospital Performance**: Mulago National Referral Hospital, Butabika Hospital
- **District Health**: Health Officer Offices, District performance metrics
- **Regional Statistics**: Regional health system indicators
- **Specialized Care**: Maternal health, child health, infectious diseases

## API Endpoints

### Business Data Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information and status |
| `GET` | `/test` | Readiness check (no DB required) |
| `GET` | `/health` | Health check with DB connectivity status |
| `GET` | `/health/indicators` | Query indicators with filtering, sorting, pagination |
| `GET` | `/health/indicators/metadata` | Indicator coverage and value ranges |
| `GET` | `/health/indicators/{name}/timeseries` | Complete time series for specific indicator |
| `GET` | `/health/rankings/top-performers` | Top indicators by various metrics |
| `GET` | `/health/stats` | Overall dataset statistics |
| `GET` | `/health/quality/dashboard` | Data quality metrics |

### Observability Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/observability/pipeline-health` | Pipeline health metrics (last 30 days) |
| `GET` | `/observability/recent-runs` | Recent pipeline runs with execution details |
| `GET` | `/observability/data-quality` | Data quality metrics summary (configurable days) |
| `GET` | `/observability/lineage/{table}/{column}` | Field-level lineage for specific column |
| `GET` | `/observability/source-files` | Registered source files with processing stats |
| `GET` | `/observability/dashboard` | Comprehensive monitoring dashboard data |

### Example API Calls

**Business Data Queries:**
```bash
# Get dataset statistics
curl http://127.0.0.1:8000/health/stats

# Query indicators (top 5 by value)
curl "http://127.0.0.1:8000/health/indicators?limit=5&sort_by=value&sort_order=desc"

# Get time series for specific indicator
curl "http://127.0.0.1:8000/health/indicators/Mulago%20National%20Referral%20Hospital/timeseries"

# Filter by period
curl "http://127.0.0.1:8000/health/indicators?period=2019/20&limit=10"
```

**Observability Queries:**
```bash
# Get pipeline health metrics
curl http://127.0.0.1:8000/observability/pipeline-health

# View recent pipeline runs (last 10)
curl "http://127.0.0.1:8000/observability/recent-runs?limit=10"

# Check data quality metrics (last 7 days)
curl "http://127.0.0.1:8000/observability/data-quality?days=7"

# Query field lineage
curl http://127.0.0.1:8000/observability/lineage/fact_indicator_values/value

# Get full monitoring dashboard
curl http://127.0.0.1:8000/observability/dashboard
```

📖 **Full API Documentation**: http://127.0.0.1:8000/docs

## Data Quality & Observability

### Production-Grade Observability Framework

The pipeline includes a comprehensive observability layer that provides enterprise-level monitoring, validation, and audit capabilities:

**Pipeline Execution Tracking:**
- Automatic tracking of every pipeline run with unique UUIDs
- Execution duration, record counts, and success/failure status
- Error logging with detailed exception information
- Run history and performance trends over time

**Data Quality Validation (151+ Automated Checks):**
- **Completeness**: Null value detection in critical fields (<5% threshold)
- **Validity**: Range checks, data type validation, format consistency
- **Consistency**: Duplicate detection, referential integrity checks
- **Uniqueness**: Primary key validation, unique constraint enforcement
- **Timeliness**: Processing time monitoring and SLA tracking

**Field-Level Lineage:**
- Complete audit trail from Excel source cells to warehouse columns
- Transformation logic documentation for every field
- Source file tracking with MD5 hash-based change detection
- Sheet-to-table and column-to-column mapping

**Quality Scoring:**
- Overall quality score (0-100) for each pipeline run
- Category-specific scores (completeness, validity, consistency)
- Configurable thresholds and alert rules
- Historical quality trend analysis

### CLI Monitoring Dashboard

```bash
# View full monitoring dashboard
python observability/monitor_dashboard.py

# Check pipeline health (success rates, avg duration)
python observability/monitor_dashboard.py health

# View data quality summary (last 7 days)
python observability/monitor_dashboard.py quality

# See recent pipeline runs with details
python observability/monitor_dashboard.py runs 20

# Query field lineage for specific column
python observability/monitor_dashboard.py lineage fact_indicator_values value

# List registered source files
python observability/monitor_dashboard.py files
```

### Sample Dashboard Output

```
================================================================================
PIPELINE HEALTH (Last 30 Days)
================================================================================
Pipeline                       Runs     Success    Failed     Success Rate    Avg Duration
----------------------------------------------------------------------------------------------------
uganda_health_etl/ingestion    7        7          0          [OK] 100.0%     0.4s
uganda_health_etl/transform    7        7          0          [OK] 100.0%     0.3s
uganda_health_etl/load         7        7          0          [OK] 100.0%     1.5s

================================================================================
DATA QUALITY SUMMARY (Last 7 Days)
================================================================================
Category             Total Checks      Passed          Pass Rate
----------------------------------------------------------------------------------------------------
completeness         45                45              [OK] 100.0%
consistency          38                38              [OK] 100.0%
validity             42                40              [WARN] 95.2%
uniqueness           26                26              [OK] 100.0%
```

### Metadata Schema

The observability framework uses a dedicated `metadata` schema with 4 core tables:

- **pipeline_runs**: Tracks every pipeline execution with metrics
- **data_quality_metrics**: Stores results of 151+ quality checks
- **field_lineage**: Documents complete data lineage
- **source_files**: Catalogs source files with hash-based change detection

See [OBSERVABILITY.md](OBSERVABILITY.md) for complete documentation.

### Run Tests

```bash
# Execute smoke tests
python tests/smoke_test.py

# Expected output: All tests passing
```

## Analytics & Integration

### Business Intelligence

Connect your favorite BI tool to the PostgreSQL database:

- **Metabase** - Self-service analytics
- **Power BI** - Microsoft data visualization
- **Tableau** - Advanced analytics platform
- **Custom Dashboards** - Use the REST API

### Use Cases

- **Health Ministry Dashboards** - Real-time performance monitoring
- **Hospital Management** - Facility-specific analytics
- **Research & Epidemiology** - Data export for studies
- **Public Health Policy** - Evidence-based decision making
- **Mobile Applications** - Field worker data access

## Troubleshooting

### Common Issues

**Database Connection Errors:**
```bash
# Verify PostgreSQL is running
pg_isready -h localhost -p 5432

# Check credentials in conf/.env match your local setup
# Ensure both 'health' and 'metadata' schemas exist
```

**Observability Schema Not Found:**
```bash
# Initialize the observability schema
python observability/init_observability.py

# Verify schema creation
psql -U postgres -d uganda_health -c "\d metadata.*"
```

**API Not Starting:**
```bash
# Check port availability
netstat -ano | findstr :8000

# Start with verbose logging
uvicorn api.main:app --host 127.0.0.1 --port 8000 --log-level debug
```

**Pipeline Errors:**
```bash
# Verify source data exists
ls data/source/

# Check database schemas
psql -U postgres -d uganda_health -c "\d health.*"
psql -U postgres -d uganda_health -c "\d metadata.*"

# View recent pipeline failures
python observability/monitor_dashboard.py runs 10
```

**Quality Check Failures:**
```bash
# View detailed quality check results
python observability/monitor_dashboard.py quality

# Query failed checks via API
curl "http://127.0.0.1:8000/observability/data-quality?days=7"
```


## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Standards

- **Python**: PEP 8 compliance, type hints
- **API Design**: RESTful principles, OpenAPI spec
- **Database**: Optimized queries, proper indexing
- **Testing**: 80%+ code coverage, automated quality checks
- **Documentation**: Comprehensive docstrings
- **Observability**: All new pipeline stages must use ObservedPipeline context manager
- **Data Quality**: Minimum 95% quality score for production releases

## License

This project is developed for **educational and research purposes**. The health sector performance data is sourced from official Ugandan government publications and is intended to support public health research, policy development, and healthcare system improvement initiatives.

### Data Sources

- Uganda Ministry of Health publications
- District Health Information System (DHIS2)
- National Health Management Information System

## Support

- **Documentation**: See this README, [OBSERVABILITY.md](OBSERVABILITY.md), and `/docs` endpoint
- **Issues**: Report bugs and request features on GitHub
- **API Reference**: Interactive docs at http://127.0.0.1:8000/docs
- **Monitoring**: Use CLI dashboard or observability API endpoints for pipeline health

## Project Statistics

- **Data Volume**: 72,402 health measurements processed
- **Indicators Tracked**: 152 unique health performance metrics
- **Time Periods**: 4 fiscal years (2016/17 - 2019/20)
- **API Endpoints**: 15+ REST endpoints (9 business + 6 observability)
- **Quality Checks**: 151+ automated validations across 5 categories
- **Pipeline Stages**: 3 (ingestion, transform, load) - all instrumented
- **Database Schemas**: 2 (health data warehouse + metadata observability)
- **Tables**: 8 total (4 dimensional model + 4 metadata tracking)

---

**Built with modern data engineering practices for reliable, scalable, production-grade health analytics.**
