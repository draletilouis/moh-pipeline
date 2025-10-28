# Uganda Health Sector Data Pipeline

Transform health sector performance data from Excel to queryable analytics with PostgreSQL and FastAPI.

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Overview

A production-ready data engineering platform that transforms Uganda's health sector performance data from raw Excel files into a structured, queryable analytics system. This pipeline processes **72,402 health performance measurements** across **152 indicators**, spanning from **2016/17 to 2019/20**, and serves them through a modern REST API.

### Key Features

- Complete ETL Pipeline: Extract from Excel → Transform & Clean → Load to Data Warehouse
- Star Schema Design: Optimized dimensional modeling for fast analytical queries
- RESTful API: FastAPI-powered endpoints with automatic OpenAPI documentation
- Data Quality: Comprehensive validation, quality checks, and smoke tests
- Time Series Analysis: Track health indicators over 4 fiscal years
- Flexible Queries: Filter, sort, paginate, and aggregate health metrics
- Self-Documenting: Interactive API docs at `/docs`

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│   Source Data       │     │   ETL Pipeline      │
│   Excel Files       │────▶│   Python Scripts    │
│                     │     │                     │
│ • 5 sheets         │     │ • load-excel.py     │
│ • 152 indicators   │     │ • clean_transform   │
└─────────────────────┘     └─────────────────────┘
                                      │
                                      ▼
                          ┌─────────────────────┐
                          │   Data Warehouse    │
                          │   PostgreSQL        │
                          │                     │
                          │ • Star Schema       │
                          │ • 72K+ records      │
                          └─────────────────────┘
                                      │
                                      ▼
                          ┌─────────────────────┐
                          │   REST API          │
                          │   FastAPI           │
                          │                     │
                          │ • 9+ endpoints      │
                          │ • Auto-docs         │
                          └─────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                     ▼
          ┌─────────────────┐                  ┌─────────────────┐
          │   BI Tools      │                  │   Applications  │
          │                 │                  │                 │
          │ • Metabase      │                  │ • Dashboards    │
          │ • Power BI      │                  │ • Reports       │
          └─────────────────┘                  └─────────────────┘
```

### Data Flow

1. Ingestion: Excel files parsed into structured CSV format
2. Transformation: Data cleaned, normalized, and unpivoted for time series
3. Loading: Processed data loaded into star schema warehouse
4. API Serving: REST endpoints provide programmatic data access
5. Analytics: BI tools and custom apps consume standardized data

## Technology Stack

- **Python 3.9+** - Core programming language
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
│   └── load-excel.py              # Excel parsing and CSV extraction
├── 📂 transform/
│   └── clean_and_unpivot.py       # Data cleaning and normalization
├── 📂 warehouse/
│   ├── schema.sql                 # Dimensional warehouse schema
│   └── load_to_postgres.py        # Database loading and optimization
├── 📂 api/
│   └── main.py                    # FastAPI application with 9+ endpoints
├── 📂 tests/
│   └── smoke_test.py              # Data quality and integration tests
├── 📂 scripts/
│   └── load_data.sh               # Pipeline orchestration script
├── 📂 conf/
│   └── db_env.example             # Environment configuration template
├── 📄 requirements.txt            # Python dependencies
├── 📄 README.md                   # This guide
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
```

### 3. Run Pipeline

   ```bash
   # Step 1: Extract Excel to CSV
   python ingestion/load-excel.py

# Step 2: Transform and clean data
   python transform/clean_and_unpivot.py

# Step 3: Load to PostgreSQL
   python warehouse/load_to_postgres.py

# Step 4: Run analysis
python analyze_data.py
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

### Core Endpoints

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

### Example API Calls

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

📖 **Full API Documentation**: http://127.0.0.1:8000/docs

## Data Quality

The pipeline includes comprehensive quality assurance:

- **100% Data Completeness** - Zero null values in critical fields
- **Automated Validation** - Foreign key integrity maintained
- **Quality Metrics** - Built-in dashboard for monitoring
- **Smoke Tests** - End-to-end validation of entire pipeline
- **Statistical Analysis** - Value ranges, distributions, trends

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

# Check credentials match your local setup
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

# Check database schema
psql -U postgres -d uganda_health -c "\d health.*"
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
- **Testing**: 80%+ code coverage
- **Documentation**: Comprehensive docstrings

## License

This project is developed for **educational and research purposes**. The health sector performance data is sourced from official Ugandan government publications and is intended to support public health research, policy development, and healthcare system improvement initiatives.

### Data Sources

- Uganda Ministry of Health publications
- District Health Information System (DHIS2)
- National Health Management Information System

## Support

- **Documentation**: See this README and `/docs` endpoint
- **Issues**: Report bugs and request features on GitHub
- **API Reference**: Interactive docs at http://127.0.0.1:8000/docs

---

**Built with modern data engineering practices for reliable, scalable health analytics.**
