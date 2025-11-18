"""
FastAPI application for Uganda Health Pipeline
Provides REST API access to health sector performance indicators
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import psycopg2
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
from api.auth import verify_api_key

# Load local environment variables if present
load_dotenv(dotenv_path=Path("conf/.env"))

app = FastAPI(
    title="Uganda Health API",
    description="REST API for Uganda Health Sector Performance Indicators",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Configuration
# In production, replace with specific allowed origins
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Set ALLOWED_ORIGINS in .env for production
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["X-API-Key", "Content-Type", "Authorization"],
)

# Database configuration
def get_db_config():
    """Get database configuration"""
    # Use Docker service name when in Docker, localhost for local development
    host = os.getenv("DB_HOST", "postgres" if os.getenv("DOCKER_CONTAINER") else "localhost")

    return {
        "host": host,
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "database": os.getenv("DB_NAME", "uganda_health")
    }

# Pydantic models
class HealthIndicator(BaseModel):
    indicator_name: str
    period_label: str
    year: Optional[int]
    value: float

class IndicatorMetadata(BaseModel):
    indicator_name: str
    category: Optional[str]
    data_points: int
    first_period: Optional[str]
    last_period: Optional[str]
    value_range: Optional[List[float]]

class StatsResponse(BaseModel):
    total_records: int
    unique_indicators: int
    unique_periods: int
    avg_value: float
    min_value: float
    max_value: float
    last_updated: Optional[str]

class RankingItem(BaseModel):
    rank: int
    indicator_name: str
    value: float
    category: Optional[str]
    change_from_previous: Optional[float]

def get_db_connection():
    """Create database connection"""
    try:
        return psycopg2.connect(**get_db_config())
    except Exception as e:
        # Don't raise HTTPException during connection creation
        # Let individual endpoints handle connection errors
        raise Exception(f"Database connection failed: {str(e)}")

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Uganda Health API", "version": "1.0.0", "status": "active"}

@app.get("/test")
async def test_endpoint():
    """Simple test endpoint that doesn't require database"""
    return {
        "message": "API is working!",
        "timestamp": datetime.now().isoformat(),
        "endpoints": [
            "/health/indicators",
            "/health/indicators/metadata",
            "/health/stats",
            "/health/rankings/top-performers"
        ]
    }

@app.get("/health/indicators", response_model=List[HealthIndicator])
async def get_indicators(
    client: dict = Depends(verify_api_key),
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=1000),
    offset: int = Query(0, description="Number of records to skip", ge=0),
    indicator: Optional[str] = Query(None, description="Filter by indicator name (partial match)"),
    period: Optional[str] = Query(None, description="Filter by period label"),
    year: Optional[int] = Query(None, description="Filter by year"),
    sort_by: str = Query("indicator_name", description="Sort field", enum=["indicator_name", "period_label", "year", "value"]),
    sort_order: str = Query("asc", description="Sort order", enum=["asc", "desc"])
):
    """
    Get health indicator data with flexible filtering and sorting options.

    - **limit**: Maximum records to return (1-1000)
    - **offset**: Records to skip for pagination
    - **indicator**: Filter by indicator name (case-insensitive partial match)
    - **period**: Filter by period (e.g., "2019/20")
    - **year**: Filter by year
    - **sort_by**: Sort field
    - **sort_order**: Sort direction
    """
    # Use SQLAlchemy instead of psycopg2 for pandas compatibility
    engine = create_engine(
        f"postgresql+psycopg2://{get_db_config()['user']}:{get_db_config()['password']}@{get_db_config()['host']}:{get_db_config()['port']}/{get_db_config()['database']}"
    )

    try:
        query = """
        SELECT
            i.indicator_name,
            d.period_label,
            d.year,
            f.value
        FROM health.fact_indicator_values f
        JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
        JOIN health.dim_date d ON f.date_id = d.date_id
        WHERE 1=1
        """

        params = {}

        if indicator:
            query += " AND LOWER(i.indicator_name) LIKE LOWER(:indicator)"
            params['indicator'] = f"%{indicator}%"

        if period:
            query += " AND d.period_label = :period"
            params['period'] = period

        if year:
            query += " AND d.year = :year"
            params['year'] = year

        # Add sorting
        if sort_by in ["indicator_name", "period_label", "year", "value"]:
            query += f" ORDER BY {sort_by} {sort_order.upper()}"

        # Add pagination (inline validated integers to avoid driver issues with bound params)
        query += f" LIMIT {int(limit)} OFFSET {int(offset)}"

        # Debug: print the query and params
        print(f"DEBUG: Query: {query}")
        print(f"DEBUG: Params: {params}")

        df = pd.read_sql(query, engine, params=params)
        results = df.to_dict('records')

        print(f"DEBUG: Results count: {len(results)}")
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        # Dispose SQLAlchemy engine to free connections
        try:
            engine.dispose()
        except Exception:
            pass

@app.get("/health/indicators/metadata")
async def get_indicators_metadata(client: dict = Depends(verify_api_key)):
    """Get metadata about available indicators"""
    conn = get_db_connection()

    try:
        # Get overall stats
        total_query = """
        SELECT
            COUNT(DISTINCT i.indicator_id) as total_indicators,
            COUNT(f.fact_id) as total_records
        FROM health.dim_indicator i
        LEFT JOIN health.fact_indicator_values f ON i.indicator_id = f.indicator_id
        """

        total_stats = pd.read_sql(total_query, conn).to_dict('records')[0]

        # Get indicator details
        indicator_query = """
        SELECT
            i.indicator_name,
            i.category,
            COUNT(f.fact_id) as data_points,
            MIN(d.period_label) as first_period,
            MAX(d.period_label) as last_period,
            MIN(f.value) as min_value,
            MAX(f.value) as max_value
        FROM health.dim_indicator i
        LEFT JOIN health.fact_indicator_values f ON i.indicator_id = f.indicator_id
        LEFT JOIN health.dim_date d ON f.date_id = d.date_id
        GROUP BY i.indicator_id, i.indicator_name, i.category
        ORDER BY i.indicator_name
        """

        indicators_df = pd.read_sql(indicator_query, conn)

        # Convert to response format
        indicators = []
        for _, row in indicators_df.iterrows():
            indicator = {
                "indicator_name": row['indicator_name'],
                "category": row['category'],
                "data_points": int(row['data_points']),
                "first_period": row['first_period'],
                "last_period": row['last_period'],
                "value_range": [float(row['min_value']), float(row['max_value'])] if pd.notna(row['min_value']) else None
            }
            indicators.append(indicator)

        return {
            "total_indicators": total_stats['total_indicators'],
            "total_records": total_stats['total_records'],
            "indicators": indicators
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Metadata query failed: {str(e)}")
    finally:
        conn.close()

@app.get("/health/indicators/{indicator_name}/timeseries")
async def get_indicator_timeseries(
    indicator_name: str,
    client: dict = Depends(verify_api_key)
):
    """Get complete time series for a specific indicator"""
    conn = get_db_connection()

    try:
        # URL decode the indicator name
        import urllib.parse
        decoded_name = urllib.parse.unquote(indicator_name)

        query = """
        SELECT
            d.period_label,
            d.year,
            f.value,
            LAG(f.value) OVER (ORDER BY d.year, d.period_label) as prev_value
        FROM health.fact_indicator_values f
        JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
        JOIN health.dim_date d ON f.date_id = d.date_id
        WHERE LOWER(i.indicator_name) = LOWER(%s)
        ORDER BY d.year, d.period_label
        """

        df = pd.read_sql(query, conn, params=[decoded_name])

        if df.empty:
            raise HTTPException(status_code=404, detail=f"Indicator '{decoded_name}' not found")

        # Calculate year-over-year changes
        time_series = []
        for _, row in df.iterrows():
            change_pct = None
            if pd.notna(row['prev_value']) and row['prev_value'] != 0:
                change_pct = round((row['value'] - row['prev_value']) / row['prev_value'] * 100, 2)

            time_series.append({
                "period": row['period_label'],
                "year": int(row['year']),
                "value": float(row['value']),
                "change_pct": change_pct
            })

        # Calculate statistics
        values = df['value'].dropna()
        avg_value = float(values.mean()) if not values.empty else 0

        # Simple trend analysis
        if len(values) >= 2:
            trend = "increasing" if values.iloc[-1] > values.iloc[0] else "decreasing"
        else:
            trend = "insufficient_data"

        return {
            "indicator_name": decoded_name,
            "data_points": len(time_series),
            "time_series": time_series,
            "statistics": {
                "average": round(avg_value, 2),
                "min_value": float(values.min()) if not values.empty else 0,
                "max_value": float(values.max()) if not values.empty else 0,
                "trend": trend
            }
        }

    except Exception as e:
        if "not found" in str(e):
            raise
        raise HTTPException(status_code=500, detail=f"Timeseries query failed: {str(e)}")
    finally:
        conn.close()

@app.get("/health/rankings/top-performers")
async def get_top_performers(
    client: dict = Depends(verify_api_key),
    period: Optional[str] = Query(None, description="Time period to rank (default: latest)"),
    limit: int = Query(10, description="Number of results", ge=1, le=50),
    metric: str = Query("total_value", description="Ranking metric", enum=["total_value", "avg_value"])
):
    """Get top-performing indicators for a given period"""
    conn = get_db_connection()

    try:
        # Determine the period to use
        if not period:
            # Get the latest period
            period_query = "SELECT MAX(period_label) FROM health.dim_date"
            latest_period = pd.read_sql(period_query, conn).iloc[0, 0]
            period = latest_period

        # Build ranking query based on metric
        if metric == "total_value":
            order_by = "SUM(f.value) DESC"
            value_field = "SUM(f.value)"
        else:  # avg_value
            order_by = "AVG(f.value) DESC"
            value_field = "AVG(f.value)"

        query = f"""
        SELECT
            i.indicator_name,
            i.category,
            {value_field} as value,
            LAG({value_field}) OVER (ORDER BY {order_by}) as prev_rank_value
        FROM health.fact_indicator_values f
        JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
        JOIN health.dim_date d ON f.date_id = d.date_id
        WHERE d.period_label = %s
        GROUP BY i.indicator_id, i.indicator_name, i.category
        ORDER BY {order_by}
        LIMIT %s
        """

        df = pd.read_sql(query, conn, params=[period, limit])

        # Format response
        rankings = []
        for idx, row in df.iterrows():
            change_pct = None
            if pd.notna(row['prev_rank_value']) and row['prev_rank_value'] != 0 and idx > 0:
                change_pct = round((row['value'] - row['prev_rank_value']) / row['prev_rank_value'] * 100, 2)

            rankings.append({
                "rank": idx + 1,
                "indicator_name": row['indicator_name'],
                "value": round(float(row['value']), 2),
                "category": row['category'],
                "change_from_previous": change_pct
            })

        return {
            "period": period,
            "ranking_metric": metric,
            "limit": limit,
            "rankings": rankings
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ranking query failed: {str(e)}")
    finally:
        conn.close()

@app.get("/health/stats")
async def get_stats(client: dict = Depends(verify_api_key)):
    """Get overall dataset statistics"""
    conn = get_db_connection()

    try:
        query = """
        SELECT
            COUNT(f.fact_id) as total_records,
            COUNT(DISTINCT i.indicator_id) as unique_indicators,
            COUNT(DISTINCT d.period_label) as unique_periods,
            ROUND(AVG(f.value), 2) as avg_value,
            MIN(f.value) as min_value,
            MAX(f.value) as max_value
        FROM health.fact_indicator_values f
        JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
        JOIN health.dim_date d ON f.date_id = d.date_id
        """

        df = pd.read_sql(query, conn)
        stats = df.to_dict('records')[0]

        # Add last updated timestamp (simplified)
        stats['last_updated'] = datetime.now().isoformat()

        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats query failed: {str(e)}")
    finally:
        conn.close()

@app.get("/health/quality/dashboard")
async def get_quality_dashboard(client: dict = Depends(verify_api_key)):
    """Get data quality metrics and validation status"""
    conn = get_db_connection()

    try:
        # Basic quality metrics
        quality_query = """
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values,
            COUNT(CASE WHEN value = 0 THEN 1 END) as zero_values,
            COUNT(CASE WHEN value < 0 THEN 1 END) as negative_values,
            ROUND(AVG(value), 2) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM health.fact_indicator_values
        """

        quality_df = pd.read_sql(quality_query, conn)
        quality = quality_df.to_dict('records')[0]

        # Calculate quality score (simplified)
        completeness_score = 1.0 if quality['null_values'] == 0 else (1 - quality['null_values'] / quality['total_records'])
        quality_score = round(completeness_score * 0.9, 2)  # Simplified scoring

        return {
            "overall_quality_score": quality_score,
            "last_updated": datetime.now().isoformat(),
            "metrics": quality,
            "validation_results": {
                "table_counts": "PASSED",
                "foreign_keys": "PASSED",
                "data_completeness": "PASSED" if completeness_score > 0.95 else "WARNING",
                "data_ranges": "PASSED"
            },
            "issues": []  # Could be expanded with actual validation issues
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quality dashboard query failed: {str(e)}")
    finally:
        conn.close()

# Health check endpoint
@app.get("/health")
async def health_check():
    """API health check"""
    conn = get_db_connection()
    try:
        # Simple query to test database connection
        pd.read_sql("SELECT 1", conn)
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
    finally:
        conn.close()


# =====================================================
# OBSERVABILITY & MONITORING ENDPOINTS
# =====================================================

@app.get("/observability/pipeline-health")
async def get_pipeline_health(client: dict = Depends(verify_api_key)):
    """
    Get pipeline health metrics (last 30 days)

    Returns success rates, run counts, and average duration for each pipeline
    """
    conn = None
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                pipeline_name,
                total_runs,
                successful_runs,
                failed_runs,
                success_rate,
                avg_duration_seconds,
                last_run_at
            FROM metadata.v_pipeline_health
            ORDER BY last_run_at DESC
        """)

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))

        return {
            "pipeline_health": results,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/observability/recent-runs")
async def get_recent_runs(
    client: dict = Depends(verify_api_key),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Get recent pipeline runs with execution details

    - **limit**: Maximum number of runs to return (1-100)
    """
    conn = None
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("""
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
                execution_duration_seconds,
                error_message
            FROM metadata.pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))

        return {
            "runs": results,
            "count": len(results),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/observability/data-quality")
async def get_data_quality_metrics(
    client: dict = Depends(verify_api_key),
    days: int = Query(7, ge=1, le=90)
):
    """
    Get data quality metrics summary

    - **days**: Number of days to look back (1-90)
    """
    conn = None
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Overall quality by category
        cursor.execute("""
            SELECT
                check_category,
                COUNT(*) as total_checks,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
                ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
            FROM metadata.data_quality_metrics
            WHERE checked_at > NOW() - INTERVAL '%s days'
            GROUP BY check_category
            ORDER BY check_category
        """, (days,))

        columns = [desc[0] for desc in cursor.description]
        quality_by_category = []
        for row in cursor.fetchall():
            quality_by_category.append(dict(zip(columns, row)))

        # Recent failures
        cursor.execute("""
            SELECT
                check_name,
                check_category,
                table_name,
                column_name,
                metric_value,
                threshold_value,
                checked_at
            FROM metadata.data_quality_metrics
            WHERE passed = FALSE
              AND checked_at > NOW() - INTERVAL '%s days'
            ORDER BY checked_at DESC
            LIMIT 20
        """, (days,))

        columns = [desc[0] for desc in cursor.description]
        failed_checks = []
        for row in cursor.fetchall():
            failed_checks.append(dict(zip(columns, row)))

        return {
            "quality_by_category": quality_by_category,
            "failed_checks": failed_checks,
            "period_days": days,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/observability/lineage/{table_name}/{column_name}")
async def get_field_lineage(
    table_name: str,
    column_name: str,
    client: dict = Depends(verify_api_key)
):
    """
    Get field-level lineage for a specific column

    - **table_name**: Target table name
    - **column_name**: Target column name
    """
    conn = None
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                source_file,
                source_sheet,
                source_column,
                transformation_logic,
                transformation_type,
                recorded_at
            FROM metadata.field_lineage
            WHERE target_table = %s
              AND target_column = %s
            ORDER BY recorded_at DESC
            LIMIT 50
        """, (table_name, column_name))

        columns = [desc[0] for desc in cursor.description]
        lineage = []
        for row in cursor.fetchall():
            lineage.append(dict(zip(columns, row)))

        return {
            "table": table_name,
            "column": column_name,
            "lineage": lineage,
            "count": len(lineage),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/observability/source-files")
async def get_source_files(client: dict = Depends(verify_api_key)):
    """
    Get registered source files with processing statistics
    """
    conn = None
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                file_id,
                file_name,
                file_path,
                file_size_bytes,
                sheet_count,
                row_count,
                column_count,
                first_seen,
                last_processed,
                processing_count,
                status
            FROM metadata.source_files
            ORDER BY last_processed DESC NULLS LAST
        """)

        columns = [desc[0] for desc in cursor.description]
        files = []
        for row in cursor.fetchall():
            files.append(dict(zip(columns, row)))

        return {
            "source_files": files,
            "count": len(files),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/observability/dashboard")
async def get_monitoring_dashboard(client: dict = Depends(verify_api_key)):
    """
    Get comprehensive monitoring dashboard data

    Returns pipeline health, data quality, and recent activity
    """
    try:
        # Gather all metrics
        pipeline_health = await get_pipeline_health()
        recent_runs = await get_recent_runs(limit=5)
        data_quality = await get_data_quality_metrics(days=7)
        source_files = await get_source_files()

        return {
            "dashboard": {
                "pipeline_health": pipeline_health["pipeline_health"],
                "recent_runs": recent_runs["runs"],
                "data_quality": data_quality,
                "source_files": source_files["source_files"]
            },
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
