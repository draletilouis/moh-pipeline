# Uganda Health Sector Data Pipeline

> **Enterprise-grade ETL pipeline transforming Uganda's health sector performance data into actionable analytics**

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green.svg)](https://fastapi.tiangolo.com/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [API Documentation](#api-documentation)
- [Authentication & Security](#authentication--security)
- [Observability & Monitoring](#observability--monitoring)
- [Data Model](#data-model)
- [Deployment](#deployment)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

A **production-ready data engineering platform** that transforms Uganda's health sector performance data from raw Excel files into a structured, queryable analytics system. This pipeline processes **72,402 health performance measurements** across **152 indicators**, spanning **2016/17 to 2019/20**, and serves them through a modern, secure REST API.

**Scalable Data Ingestion**: Supports unlimited Excel files - simply drop multiple `.xlsx` or `.xls` files into the source directory, and the pipeline automatically discovers and processes them all.

### What Makes This Pipeline Enterprise-Grade?

✅ **Production Observability** - 151+ automated data quality checks, field-level lineage, and execution tracking
✅ **Security-First** - API key authentication, CORS configuration, input validation
✅ **Performance Optimized** - Star schema design, indexed queries, connection pooling
✅ **Multi-Source Support** - Automatic discovery and processing of multiple Excel files
✅ **Self-Documenting** - Comprehensive API docs, data dictionaries, inline documentation
✅ **Developer-Friendly** - CLI tools, interactive dashboards, detailed error messages
✅ **Battle-Tested** - Idempotent pipeline execution, error recovery, data validation

---

## Key Features

### 🔄 Complete ETL Pipeline
- **Extract** from multiple Excel files with automatic discovery and sheet detection
- **Transform** with intelligent header detection and data unpivoting
- **Load** to dimensional warehouse with UPSERT logic and integrity checks
- **Scalable** - Supports unlimited Excel files, automatically processes all `.xlsx` and `.xls` files

### 🏗️ Star Schema Design
- Optimized dimensional modeling for fast analytical queries
- 4 dimension tables, 1 fact table
- Proper foreign key relationships and indexing

### 📊 Production Observability
- **151+ Automated Data Quality Checks** across 5 categories:
  - Completeness (null detection, missing values)
  - Validity (range checks, data types)
  - Consistency (duplicate detection, cross-field validation)
  - Uniqueness (primary key validation)
  - Timeliness (processing time monitoring)
- **Field-Level Lineage** - Complete audit trail from Excel to warehouse
- **Pipeline Monitoring** - Real-time execution metrics and success rates
- **Quality Scoring** - 0-100 score for each pipeline run

### 🔐 API Security
- **API Key Authentication** - Header-based authentication for all protected endpoints
- **CORS Configuration** - Configurable allowed origins for production
- **Input Validation** - Pydantic models with automatic validation
- **Rate Limiting Ready** - Infrastructure supports future rate limiting

### 🚀 RESTful API
- **15+ Endpoints** - 9 business data + 6 observability endpoints
- **Interactive Documentation** - Auto-generated Swagger UI and ReDoc
- **Flexible Querying** - Filter, sort, paginate, and aggregate
- **Time Series Support** - Track indicators over fiscal years

### 📈 Advanced Analytics
- Hash-based change detection for idempotent processing
- Time series analysis with year-over-year comparisons
- Statistical aggregations and rankings
- Data export capabilities for BI tools

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          UGANDA HEALTH PIPELINE                         │
│                   Production Data Engineering Platform                  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  SOURCE DATA    │         │  ETL PIPELINE   │         │  OBSERVABILITY  │
│  Excel Files    │────────▶│  Python         │────────▶│  Metadata       │
│                 │         │                 │         │                 │
│ • Multi-file    │         │ • Ingestion     │         │ • Run Tracking  │
│ • Auto-discover │         │ • Transform     │         │ • Quality Checks│
│ • 72K+ records  │         │ • Load          │         │ • Lineage       │
│ • MD5 Hashing   │         │ • Validation    │         │ • File Registry │
└─────────────────┘         └─────────────────┘         └─────────────────┘
                                     │                           │
                                     ▼                           ▼
                         ┌─────────────────┐         ┌─────────────────┐
                         │ DATA WAREHOUSE  │         │ METADATA SCHEMA │
                         │ PostgreSQL      │         │ PostgreSQL      │
                         │                 │         │                 │
                         │ Schema: health  │         │ Schema: metadata│
                         │ • dim_indicator │         │ • pipeline_runs │
                         │ • dim_date      │         │ • quality_metrics│
                         │ • dim_location  │         │ • field_lineage │
                         │ • fact_values   │         │ • source_files  │
                         └─────────────────┘         └─────────────────┘
                                     │                           │
                                     └───────────┬───────────────┘
                                                 ▼
                              ┌──────────────────────────────┐
                              │     REST API (FastAPI)       │
                              │    🔐 API Key Protected      │
                              │                              │
                              │  /health/*  | /observability/*│
                              │  9 endpoints| 6 endpoints     │
                              └──────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
          ┌──────────────────┐              ┌──────────────────┐
          │   CONSUMERS      │              │   MONITORING     │
          │                  │              │                  │
          │ • BI Tools       │              │ • CLI Dashboard  │
          │ • Dashboards     │              │ • Quality Score  │
          │ • Mobile Apps    │              │ • Lineage Query  │
          │ • Analysts       │              │ • Health Metrics │
          └──────────────────┘              └──────────────────┘
```

### Data Flow

```
1. INGEST     → Excel files parsed and validated → CSV extraction
                ↓
2. TRANSFORM  → Data cleaned, normalized, unpivoted → Quality checks
                ↓
3. LOAD       → Dimensional warehouse load → Integrity validation
                ↓
4. OBSERVE    → Metadata tracked → Lineage recorded → Quality scored
                ↓
5. SERVE      → REST API → Authenticated access → Real-time queries
```

---

## Technology Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Language** | Python | 3.9+ | Pipeline development |
| **Database** | PostgreSQL | 15 | Data warehouse & metadata |
| **API Framework** | FastAPI | 0.104 | REST API server |
| **ASGI Server** | uvicorn | 0.24 | Production web server |
| **Data Processing** | pandas | 2.1.4 | ETL transformations |
| **ORM** | SQLAlchemy | 2.0.25 | Database abstraction |
| **DB Driver** | psycopg2 | 2.9.9 | PostgreSQL connectivity |
| **Validation** | pydantic | 2.5.0 | Data validation |
| **Excel** | openpyxl | 3.1.2 | Excel file parsing |
| **Config** | python-dotenv | 1.0.0 | Environment management |

### Why These Technologies?

- **FastAPI**: Automatic OpenAPI docs, async support, type safety
- **PostgreSQL**: ACID compliance, analytical query optimization, JSON support
- **pandas**: Industry-standard data manipulation, rich ecosystem
- **SQLAlchemy**: Database portability, ORM flexibility
- **pydantic**: Runtime type checking, automatic validation

---

## Quick Start

### Prerequisites

```bash
# Required
✓ Python 3.9 or higher
✓ PostgreSQL 15 or higher
✓ Git

# Optional
○ Docker (for containerized deployment)
○ pgAdmin (for database management)
```

### Installation

#### 1. Clone Repository

```bash
git clone https://github.com/your-org/uganda-health-pipeline.git
cd uganda-health-pipeline
```

#### 2. Create Virtual Environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
```

#### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 4. Configure Environment

```bash
# Copy example configuration
cp conf/.env.example conf/.env

# Edit conf/.env with your database credentials
# Required variables:
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_USER=postgres
#   DB_PASSWORD=your_password
#   DB_NAME=uganda_health
#   API_KEY=your-secure-api-key-here
```

#### 5. Initialize Database

```bash
# Create database
createdb -U postgres uganda_health

# Or using psql
psql -U postgres -c "CREATE DATABASE uganda_health;"

# Initialize schemas
python observability/init_observability.py
```

#### 6. Add Source Data

```bash
# Place your Excel file(s) in data/source/
# The pipeline automatically discovers and processes ALL Excel files

# Single file (works)
data/source/health_indicators.xlsx

# Multiple files (also works!)
data/source/
├── health_2016_2020.xlsx
├── health_2020_2022.xlsx
└── districts.xlsx

# All .xlsx and .xls files will be processed automatically
```

#### 7. Run Pipeline

```bash
# Option A: Run complete pipeline
python run_pipeline.py

# Option B: Run stages individually
python ingestion/load-excel.py      # Stage 1: Extract
python transform/clean_and_unpivot.py  # Stage 2: Transform
python warehouse/load_to_postgres.py   # Stage 3: Load
```

#### 8. Start API Server

```bash
# Development mode (auto-reload)
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload

# Production mode
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

#### 9. Access API

Open your browser:
- **Swagger UI**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc
- **API Root**: http://127.0.0.1:8000/

### First API Request

```bash
# Get API status (no auth required)
curl http://127.0.0.1:8000/

# Get health stats (requires API key)
curl -H "X-API-Key: your-api-key-here" \
  http://127.0.0.1:8000/health/stats
```

---

## Project Structure

```
uganda-health-pipeline/
│
├── 📂 api/                          # REST API Layer
│   ├── main.py                      # FastAPI application (15+ endpoints)
│   ├── auth.py                      # Authentication module
│   └── README_AUTH.md               # Authentication documentation
│
├── 📂 conf/                         # Configuration
│   ├── .env                         # Environment variables (git-ignored)
│   └── .env.example                 # Configuration template
│
├── 📂 data/                         # Data Storage
│   ├── source/                      # Raw Excel files (supports multiple files)
│   ├── raw/                         # Extracted CSV files (all sheets from all files)
│   └── clean/                       # Processed and unpivoted data
│
├── 📂 ingestion/                    # Data Extraction
│   └── load-excel.py                # Multi-file Excel to CSV converter
│
├── 📂 transform/                    # Data Transformation
│   └── clean_and_unpivot.py         # Cleaning and normalization
│
├── 📂 warehouse/                    # Data Loading
│   ├── schema.sql                   # Dimensional warehouse DDL
│   ├── observability_schema.sql     # Metadata schema DDL
│   ├── load_to_postgres.py          # Warehouse loader
│   └── analyze_data.py              # Data analysis utilities
│
├── 📂 observability/                # Monitoring & Quality
│   ├── __init__.py                  # Module exports
│   ├── pipeline_observer.py         # Execution tracking engine
│   ├── data_quality.py              # Quality validation framework
│   ├── monitor_dashboard.py         # CLI monitoring dashboard
│   └── init_observability.py        # Schema initialization
│
├── 📂 tests/                        # Testing
│   └── smoke_test.py                # Integration tests
│
├── 📂 scripts/                      # Utilities
│   └── init_data.sql                # Database initialization
│
├── 📄 run_pipeline.py               # Pipeline orchestrator
├── 📄 requirements.txt              # Python dependencies
├── 📄 README.md                     # This file
├── 📄 DATA_SOURCES.md               # Multi-file ingestion guide
├── 📄 OBSERVABILITY.md              # Observability documentation
├── 📄 API_DESIGN.md                 # API design documentation
├── 📄 postgres_queries.sql          # Sample analytical queries
└── 📄 .gitignore                    # Git ignore rules
```

---

## Data Pipeline

### Pipeline Orchestration

The pipeline consists of three main stages executed sequentially:

```bash
# Automated orchestration
python run_pipeline.py
```

**Expected Output:**
```
======================================================================
UGANDA HEALTH PIPELINE - STARTING
======================================================================

======================================================================
STAGE 1: INGESTION - Loading Excel data to CSV
======================================================================
✅ Ingestion completed in 5.23 seconds

======================================================================
STAGE 2: TRANSFORM - Cleaning and unpivoting data
======================================================================
✅ Transform completed in 3.45 seconds

======================================================================
STAGE 3: WAREHOUSE LOAD - Loading to PostgreSQL
======================================================================
✅ Warehouse load completed in 2.10 seconds

======================================================================
PIPELINE COMPLETED SUCCESSFULLY
======================================================================
Stage 1 (Ingestion):  5.23s
Stage 2 (Transform):  3.45s
Stage 3 (Load):       2.10s
Total Duration:       10.78s
======================================================================
```

### Stage 1: Ingestion

**Purpose**: Extract data from multiple Excel files to structured CSV format

**Features**:
- **Multi-file support** - Automatically discovers ALL `.xlsx` and `.xls` files
- **Automatic sheet detection** - Extracts all sheets from each file
- **Unique naming** - CSVs named as `{filename}_{sheetname}.csv` to prevent conflicts
- **MD5 hash tracking** - Detects file changes for idempotent processing
- **Resilient processing** - File-level error handling (one failure doesn't stop others)
- **File metadata tracking** - Complete audit trail for each source file
- **Quality validation** - Completeness checks on raw data

**Input**: `data/source/*.xlsx` and `data/source/*.xls` (all files)
**Output**: `data/raw/{filename}_{sheetname}.csv` (one per sheet)

**Example:**
```bash
# Input: 3 Excel files
data/source/
├── health_2016_2020.xlsx (5 sheets)
├── health_2020_2022.xlsx (3 sheets)
└── districts.xlsx (2 sheets)

# Output: 10 CSV files
data/raw/
├── health_2016_2020_Sheet1.csv
├── health_2016_2020_Sheet2.csv
├── health_2016_2020_Sheet3.csv
├── health_2016_2020_Sheet4.csv
├── health_2016_2020_Sheet5.csv
├── health_2020_2022_Summary.csv
├── health_2020_2022_Details.csv
├── health_2020_2022_Trends.csv
├── districts_Regional.csv
└── districts_National.csv
```

**Run:**
```bash
python ingestion/load-excel.py
```

### Stage 2: Transformation

**Purpose**: Clean, normalize, and unpivot data for analytics

**Features**:
- Intelligent header detection
- Data type inference and conversion
- Year column unpivoting for time series
- Missing value handling
- Data quality validation
- Field-level lineage tracking

**Input**: `data/raw/*.csv`
**Output**: `data/clean/health_data.csv`

```bash
python transform/clean_and_unpivot.py
```

### Stage 3: Warehouse Load

**Purpose**: Load transformed data into PostgreSQL data warehouse

**Features**:
- UPSERT logic (no duplicates on reruns)
- Dimensional model population
- Referential integrity checks
- Index creation for performance
- Transaction management
- Load statistics tracking

**Input**: `data/clean/health_data.csv`
**Output**: PostgreSQL `health` schema tables

```bash
python warehouse/load_to_postgres.py
```

---

## API Documentation

### Authentication

All protected endpoints require an API key in the request header:

```bash
curl -H "X-API-Key: your-api-key-here" \
  http://127.0.0.1:8000/health/indicators
```

**Public Endpoints** (no auth):
- `GET /` - API information
- `GET /test` - Health check
- `GET /health` - Database connectivity check

**Protected Endpoints** (requires API key):
- All `/health/*` endpoints
- All `/observability/*` endpoints

See [api/README_AUTH.md](api/README_AUTH.md) for complete authentication documentation.

### Business Data Endpoints

#### GET /health/indicators

Query health indicators with filtering and pagination.

**Query Parameters:**
- `limit` (int, 1-1000): Maximum records to return (default: 100)
- `offset` (int): Records to skip for pagination (default: 0)
- `indicator` (string): Filter by indicator name (partial match)
- `period` (string): Filter by fiscal period (e.g., "2019/20")
- `year` (int): Filter by year
- `sort_by` (enum): Sort field - `indicator_name`, `period_label`, `year`, `value`
- `sort_order` (enum): `asc` or `desc`

**Example:**
```bash
curl -H "X-API-Key: your-key" \
  "http://127.0.0.1:8000/health/indicators?limit=10&sort_by=value&sort_order=desc"
```

#### GET /health/indicators/metadata

Get metadata about all indicators.

**Response:**
```json
{
  "total_indicators": 152,
  "total_records": 72402,
  "indicators": [
    {
      "indicator_name": "Mulago National Referral Hospital",
      "category": "Hospital Performance",
      "data_points": 476,
      "first_period": "2016/17",
      "last_period": "2019/20",
      "value_range": [0.0, 15000.0]
    }
  ]
}
```

#### GET /health/indicators/{name}/timeseries

Get complete time series for a specific indicator.

**Example:**
```bash
curl -H "X-API-Key: your-key" \
  "http://127.0.0.1:8000/health/indicators/Mulago%20National%20Referral%20Hospital/timeseries"
```

### Observability Endpoints

#### GET /observability/pipeline-health

Get pipeline health metrics for last 30 days.

**Response:**
```json
{
  "pipeline_health": [
    {
      "pipeline_name": "uganda_health_etl/ingestion",
      "total_runs": 7,
      "successful_runs": 7,
      "failed_runs": 0,
      "success_rate": 100.0,
      "avg_duration_seconds": 0.4
    }
  ]
}
```

#### GET /observability/data-quality

Get data quality metrics summary.

**Query Parameters:**
- `days` (int, 1-90): Days to look back (default: 7)

**Example:**
```bash
curl -H "X-API-Key: your-key" \
  "http://127.0.0.1:8000/observability/data-quality?days=30"
```

### Complete Endpoint Reference

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/` | GET | No | API information |
| `/test` | GET | No | Health check |
| `/health` | GET | No | Database connectivity |
| `/health/indicators` | GET | Yes | Query indicators |
| `/health/indicators/metadata` | GET | Yes | Indicator metadata |
| `/health/indicators/{name}/timeseries` | GET | Yes | Time series data |
| `/health/rankings/top-performers` | GET | Yes | Top performers |
| `/health/stats` | GET | Yes | Dataset statistics |
| `/health/quality/dashboard` | GET | Yes | Quality metrics |
| `/observability/pipeline-health` | GET | Yes | Pipeline health |
| `/observability/recent-runs` | GET | Yes | Recent runs |
| `/observability/data-quality` | GET | Yes | Quality summary |
| `/observability/lineage/{table}/{column}` | GET | Yes | Field lineage |
| `/observability/source-files` | GET | Yes | Source file registry |
| `/observability/dashboard` | GET | Yes | Full dashboard |

**Interactive Documentation**: http://127.0.0.1:8000/docs

---

## Authentication & Security

### API Key Authentication

This pipeline uses API key-based authentication for secure access.

#### Configuration

1. **Set API Key** in `conf/.env`:
```bash
API_KEY=gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto
```

2. **Generate New Key** (recommended for production):
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

#### Usage

**Python:**
```python
import requests

headers = {"X-API-Key": "your-api-key-here"}
response = requests.get(
    "http://127.0.0.1:8000/health/indicators",
    headers=headers
)
print(response.json())
```

**JavaScript:**
```javascript
fetch('http://127.0.0.1:8000/health/indicators', {
  headers: {
    'X-API-Key': 'your-api-key-here'
  }
})
.then(res => res.json())
.then(data => console.log(data));
```

**curl:**
```bash
curl -H "X-API-Key: your-api-key-here" \
  http://127.0.0.1:8000/health/indicators
```

### CORS Configuration

Configure allowed origins in `conf/.env`:

```bash
# Development (allow all)
ALLOWED_ORIGINS=*

# Production (specific domains)
ALLOWED_ORIGINS=https://dashboard.health.go.ug,https://analytics.health.go.ug
```

### Security Best Practices

✅ **Never commit** `.env` file to version control
✅ **Use HTTPS** in production
✅ **Rotate keys** regularly
✅ **Different keys** per environment (dev, staging, prod)
✅ **Monitor usage** via observability endpoints

See [api/README_AUTH.md](api/README_AUTH.md) for complete documentation.

---

## Observability & Monitoring

### Production-Grade Observability

The pipeline includes comprehensive monitoring and data quality validation.

### CLI Monitoring Dashboard

```bash
# Full dashboard
python observability/monitor_dashboard.py

# Pipeline health metrics
python observability/monitor_dashboard.py health

# Data quality summary
python observability/monitor_dashboard.py quality

# Recent pipeline runs
python observability/monitor_dashboard.py runs 20

# Field lineage query
python observability/monitor_dashboard.py lineage fact_indicator_values value

# Source file registry
python observability/monitor_dashboard.py files
```

### Data Quality Framework

**151+ Automated Checks** across 5 categories:

1. **Completeness** (45 checks)
   - Null value detection
   - Missing value thresholds
   - Required field validation

2. **Validity** (42 checks)
   - Data type validation
   - Range checks
   - Format consistency

3. **Consistency** (38 checks)
   - Duplicate detection
   - Cross-field validation
   - Referential integrity

4. **Uniqueness** (26 checks)
   - Primary key validation
   - Unique constraint enforcement

5. **Timeliness** (ongoing)
   - Processing time monitoring
   - SLA tracking

### Quality Scoring

Each pipeline run receives a quality score (0-100):

- **95-100**: Excellent - Production ready
- **90-94**: Good - Minor issues
- **85-89**: Fair - Review required
- **<85**: Poor - Investigation needed

### Metadata Tracking

All pipeline executions tracked in `metadata` schema:

- **pipeline_runs**: Execution history, duration, status
- **data_quality_metrics**: Quality check results
- **field_lineage**: Complete data lineage
- **source_files**: File registry with MD5 hashes

See [OBSERVABILITY.md](OBSERVABILITY.md) for complete documentation.

---

## Data Model

### Star Schema Design

```
┌─────────────────┐
│  dim_indicator  │
│─────────────────│
│ indicator_id PK │◀────┐
│ indicator_name  │     │
│ indicator_key   │     │
│ category        │     │
└─────────────────┘     │
                        │
┌─────────────────┐     │     ┌──────────────────────┐
│   dim_date      │     │     │ fact_indicator_values│
│─────────────────│     │     │──────────────────────│
│ date_id      PK │◀────┼─────│ fact_id           PK │
│ period_label    │     │     │ indicator_id      FK │─────┐
│ year            │     │     │ date_id           FK │     │
│ date_value      │     └─────│ value                │     │
└─────────────────┘           └──────────────────────┘     │
                                                            │
┌─────────────────┐                                         │
│  dim_location   │                                         │
│─────────────────│                                         │
│ location_id  PK │◀────────────────────────────────────────┘
│ location_name   │
│ location_type   │
└─────────────────┘
```

### Table Descriptions

**dim_indicator** (152 rows)
- Health performance metrics and indicators
- Examples: Hospital names, district health offices, service types

**dim_date** (4-5 rows)
- Fiscal year periods
- Range: 2016/17 to 2019/20

**dim_location** (variable)
- Geographic entities
- Districts, regions, facilities

**fact_indicator_values** (72,402 rows)
- Measurement facts
- Foreign keys to all dimensions
- Optimized with indexes on FK columns

---

## Deployment

### Local Development

```bash
# Activate environment
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Run pipeline
python run_pipeline.py

# Start API
uvicorn api.main:app --reload
```

### Production Deployment

#### Using systemd (Linux)

```ini
# /etc/systemd/system/uganda-health-api.service
[Unit]
Description=Uganda Health API
After=network.target postgresql.service

[Service]
Type=notify
User=www-data
WorkingDirectory=/opt/uganda-health-pipeline
Environment="PATH=/opt/uganda-health-pipeline/.venv/bin"
ExecStart=/opt/uganda-health-pipeline/.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable uganda-health-api
sudo systemctl start uganda-health-api
```

#### Using Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
docker build -t uganda-health-api .
docker run -p 8000:8000 --env-file conf/.env uganda-health-api
```

### Scheduling Pipeline Execution

#### Using cron (Linux/Mac)

```bash
# Edit crontab
crontab -e

# Add daily execution at 2 AM
0 2 * * * cd /path/to/uganda-health-pipeline && /path/to/.venv/bin/python run_pipeline.py >> logs/pipeline.log 2>&1
```

#### Using Task Scheduler (Windows)

1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily at 2:00 AM
4. Action: Start a program
   - Program: `C:\path\to\.venv\Scripts\python.exe`
   - Arguments: `run_pipeline.py`
   - Start in: `C:\path\to\uganda-health-pipeline`

---

## Testing

### Run Tests

```bash
# Execute smoke tests
python tests/smoke_test.py
```

### Test Coverage

Current tests validate:
- ✅ Source file existence
- ✅ Data extraction completeness
- ✅ Database connectivity
- ✅ Schema structure
- ✅ Data loading success
- ✅ API endpoint availability

**Planned:**
- Unit tests for transformation logic
- Integration tests for API endpoints
- Performance tests for large datasets
- Quality threshold enforcement

---

## Troubleshooting

### Common Issues

#### Database Connection Failed

**Symptoms:**
```
psycopg2.OperationalError: could not connect to server
```

**Solutions:**
```bash
# Verify PostgreSQL is running
pg_isready -h localhost -p 5432

# Check credentials in conf/.env
# Ensure database exists
psql -U postgres -l | grep uganda_health
```

#### API Authentication Failed

**Symptoms:**
```json
{"detail": "Invalid API Key"}
```

**Solutions:**
- Verify API key in `conf/.env` matches request header
- Check header name is `X-API-Key` (case-sensitive)
- Ensure `.env` file is loaded correctly

#### Pipeline Stage Failed

**Symptoms:**
```
❌ Pipeline failed: [error message]
```

**Solutions:**
```bash
# Check source data exists
ls data/source/

# View recent pipeline runs
python observability/monitor_dashboard.py runs 10

# Check data quality
python observability/monitor_dashboard.py quality
```

#### Quality Checks Failing

**Symptoms:**
```
[WARN] Quality score: 87.5%
```

**Solutions:**
```bash
# View detailed quality metrics
curl -H "X-API-Key: your-key" \
  "http://127.0.0.1:8000/observability/data-quality?days=7"

# Check specific failed checks
python observability/monitor_dashboard.py quality
```

### Getting Help

📖 **Documentation**:
- API docs at `/docs`
- [DATA_SOURCES.md](DATA_SOURCES.md) - Multi-file ingestion guide
- [OBSERVABILITY.md](OBSERVABILITY.md) - Monitoring & quality
- [API_DESIGN.md](API_DESIGN.md) - API architecture

🐛 **Issues**: Report bugs on GitHub
💬 **Questions**: Check documentation files listed above

---

## Contributing

We welcome contributions! Please follow these guidelines:

### Development Workflow

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** changes (`git commit -m 'Add amazing feature'`)
4. **Push** to branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Code Standards

- **Python**: PEP 8 compliance, type hints where applicable
- **Documentation**: Docstrings for all functions and classes
- **Testing**: Write tests for new features
- **Quality**: Maintain 95%+ data quality score
- **Observability**: Use `ObservedPipeline` for new pipeline stages

### Commit Messages

```
feat: Add incremental loading support
fix: Resolve null handling in transform stage
docs: Update API authentication guide
test: Add unit tests for data quality validators
```

---

## License

This project is developed for **educational and research purposes** to support public health analytics and policy development in Uganda.

### Data Sources

- Uganda Ministry of Health
- District Health Information System (DHIS2)
- National Health Management Information System

---

## Project Statistics

| Metric | Value |
|--------|-------|
| **Data Volume** | 72,402+ measurements (scalable) |
| **Indicators** | 152+ unique metrics (grows with data) |
| **Time Range** | 4+ fiscal years (expandable) |
| **Source Files** | Unlimited Excel files supported |
| **API Endpoints** | 15+ (9 business + 6 observability) |
| **Quality Checks** | 151+ automated validations |
| **Pipeline Stages** | 3 (extract, transform, load) |
| **Database Schemas** | 2 (health + metadata) |
| **Tables** | 8 (4 dimensional + 4 metadata) |
| **Code Lines** | ~3,500+ lines of Python |

---

## Acknowledgments

Built with modern data engineering practices for **reliable, scalable, production-grade health analytics**.

**Key Technologies**: Python • PostgreSQL • FastAPI • pandas • SQLAlchemy

---

<div align="center">

**[⬆ Back to Top](#uganda-health-sector-data-pipeline)**

Made with ❤️ for better health data analytics

</div>
