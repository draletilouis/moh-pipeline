#!/bin/bash

# Local data loading script
# Runs the ETL pipeline against a locally running PostgreSQL instance

set -e

echo "🚀 Starting Uganda Health Pipeline Data Load"
echo "=============================================="

# Wait for database to be ready
DB_HOST=${DB_HOST:-${PG_HOST:-localhost}}
DB_PORT=${DB_PORT:-${PG_PORT:-5432}}
DB_USER=${DB_USER:-${PG_USER:-postgres}}

echo "⏳ Waiting for PostgreSQL to be ready at ${DB_HOST}:${DB_PORT}..."
if command -v pg_isready >/dev/null 2>&1; then
  until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
  done
  echo "✅ PostgreSQL is ready!"
else
  echo "pg_isready not found; proceeding without readiness check."
fi

# Run the ETL pipeline
echo "📥 Running ETL Pipeline..."

echo "Step 1: Extracting Excel data..."
python ingestion/load-excel.py

echo "Step 2: Transforming data..."
python transform/clean_and_unpivot.py

echo "Step 3: Loading to PostgreSQL..."
python warehouse/load_to_postgres.py

echo "Step 4: Running data analysis..."
python analyze_data.py || echo "(Optional) analyze_data.py skipped/failed"

echo "✅ Pipeline completed successfully!"
echo "🎉 Data loaded and ready for API access!"
echo ""
echo "API Documentation: http://localhost:8000/docs"

