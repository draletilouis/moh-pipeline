# 🔌 API Endpoints for Uganda Health Pipeline

## Context: Health Sector Performance Data

Your pipeline processes **Uganda health sector performance indicators** including:
- Hospital performance metrics (Mulago, Butabika, etc.)
- District health office data
- Regional health statistics
- Time series data from 2016/17 to 2019/20
- **36,201 total measurements** across **152 indicators**

## API Purpose & Use Cases

### **Who Uses This API?**
- **Health Ministry Officials** - Access performance dashboards
- **Hospital Administrators** - Compare facility performance
- **Data Analysts** - Build custom reports and visualizations
- **Public Health Researchers** - Access standardized health metrics
- **Government Agencies** - Monitor health system performance
- **Mobile Apps** - Provide health data to field workers

### **What Problems Does It Solve?**
- **Data Accessibility**: Raw Excel files → API-driven data access
- **Standardization**: Consistent data format across all consumers
- **Real-time Access**: Always up-to-date processed data
- **Performance**: Optimized queries vs. raw data processing
- **Security**: Controlled access to sensitive health data

## 📋 Core API Endpoints

### 0. Service Info & Health

#### `GET /`  — Service banner
Returns API name, version, and status.

```bash
curl http://127.0.0.1:8000/
```

#### `GET /test`  — Simple readiness check
Does not require database connectivity. Lists key data endpoints.

```bash
curl http://127.0.0.1:8000/test
```

#### `GET /health`  — Health check
Verifies database connectivity and reports a simple status.

```bash
curl http://127.0.0.1:8000/health
```

Example response:
```json
{
  "status": "healthy",
  "database": "connected",
  "timestamp": "2025-10-28T13:10:00.744166"
}
```

### 1. Health Indicators Data

#### `GET /health/indicators`
Get health indicator measurements with filtering, sorting and pagination.

Query parameters:
- `indicator` — Filter by indicator name (case-insensitive partial match)
- `period` — Time period label (e.g., "2019/20")
- `year` — Year as integer
- `limit` — Max records to return (1–1000, default 100)
- `offset` — Records to skip (default 0)
- `sort_by` — One of `indicator_name`, `period_label`, `year`, `value`
- `sort_order` — `asc` or `desc`

Examples:
```bash
# First 3 records
curl "http://127.0.0.1:8000/health/indicators?limit=3"

# Filter and sort
curl "http://127.0.0.1:8000/health/indicators?indicator=hospital&sort_by=value&sort_order=desc&limit=10"
```

Response:
```json
[
  {
    "indicator_name": "Mulago National Referral Hospital",
    "period_label": "2019/20",
    "year": 2019,
    "value": 2345.67
  }
]
```

### 2. Indicator Metadata

#### `GET /health/indicators/metadata`
Describe available indicators, coverage, and value ranges.

Example:
```bash
curl http://127.0.0.1:8000/health/indicators/metadata
```

Response (abridged):
```json
{
  "total_indicators": 152,
  "total_records": 36201,
  "indicators": [
    {
      "indicator_name": "Mulago National Referral Hospital",
      "category": "Hospital Performance",
      "data_points": 9,
      "first_period": "2016/17",
      "last_period": "2019/20",
      "value_range": [2000, 2600]
    }
  ]
}
```

### 3. Time Series Analysis

#### `GET /health/indicators/{indicator_name}/timeseries`
Full history for a single indicator, with simple change calculations.

```bash
curl "http://127.0.0.1:8000/health/indicators/Mulago%20National%20Referral%20Hospital/timeseries"
```

Response (abridged):
```json
{
  "indicator_name": "Mulago National Referral Hospital",
  "data_points": 9,
  "time_series": [
    {"period": "2016/17", "year": 2016, "value": 2345.0, "change_pct": null}
  ],
  "statistics": {"average": 2481.0, "min_value": 2000.0, "max_value": 2600.0, "trend": "increasing"}
}
```

### 4. Performance Rankings

#### `GET /health/rankings/top-performers`
Top indicators for a given period.

Query parameters:
- `period` — Period label (default: latest)
- `limit` — 1–50 (default 10)
- `metric` — `total_value` or `avg_value`

```bash
curl "http://127.0.0.1:8000/health/rankings/top-performers?period=2019/20&metric=avg_value&limit=5"
```

### 5. System Statistics

#### `GET /health/stats`
Overview statistics for the dataset.

```bash
curl http://127.0.0.1:8000/health/stats
```

### 6. Data Quality Dashboard

#### `GET /health/quality/dashboard`
High-level quality metrics and a simplified validation summary.

```bash
curl http://127.0.0.1:8000/health/quality/dashboard
```

## 🔐 Security & Access Control

This version of the API is open for local development and testing:
- No authentication or authorization is required.
- CORS is configured permissively for local use.
- Add authentication when exposing beyond local environment.

## 📊 Real-World Use Cases

### **Health Ministry Dashboard**
```javascript
// Fetch key performance indicators
const kpis = await fetch('/health/indicators?period=2019/20&limit=10');
const dashboardData = await kpis.json();

// Display in charts
kpis.forEach(kpi => {
  createChart(kpi.indicator_name, kpi.value);
});
```

### **Hospital Performance App**
```javascript
// Compare hospital performance over time
const hospitalData = await fetch('/health/indicators/Mulago%20National%20Referral%20Hospital/timeseries');
const timeSeries = await hospitalData.json();

// Show trend analysis
displayTrend(timeSeries.time_series, timeSeries.statistics.trend);
```

### **Research Data Export**
```javascript
// Example: get JSON and convert to CSV client-side (no CSV export endpoint yet)
const response = await fetch('/health/indicators?indicator=hospital&period=2019/20');
const data = await response.json();
// Convert data to CSV as needed on the client
```

## 🚀 API Benefits for Your Project

### **For Users:**
- **Standardized Access**: Consistent data format across all tools
- **Real-time Data**: Always access latest processed data
- **Flexible Queries**: Filter, sort, and aggregate as needed
- **Multiple Formats**: JSON, CSV, PDF outputs

### **For Your Pipeline:**
- **Data Validation**: API layer can validate requests
- **Caching Layer**: Improve performance for common queries
- **Monitoring**: Track data usage and performance
- **Versioning**: Support multiple API versions as data evolves

### **For Interviews:**
- **Full-Stack Skills**: Backend API development
- **RESTful Design**: Proper HTTP methods and status codes
- **Data Modeling**: API design reflects dimensional model
- **Documentation**: OpenAPI/Swagger documentation
- **Testing**: Unit tests for API endpoints

This API transforms your data pipeline from a batch processing system into a **real-time data service** that enables various stakeholders to access and utilize Uganda's health performance data effectively! 🎯

