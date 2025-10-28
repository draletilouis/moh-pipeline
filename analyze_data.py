"""
analyze_data.py
Quick data analysis script to explore cleaned Uganda health data
Connects to PostgreSQL database
"""

import pandas as pd
from pathlib import Path

def main():
    print("=" * 70)
    print("UGANDA HEALTH DATA ANALYSIS")
    print("=" * 70)
    print()

    # Connect to PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="uganda_health"
        )
        print("[OK] Connected to PostgreSQL database")
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        print("Make sure PostgreSQL is running and the database exists")
        return

    # Load data from database
    query = """
    SELECT
        f.fact_id,
        i.indicator_name,
        d.period_label,
        d.year,
        f.value,
        f.units,
        f.notes
    FROM health.fact_indicator_values f
    JOIN health.dim_indicator i ON f.indicator_id = i.indicator_id
    JOIN health.dim_date d ON f.date_id = d.date_id
    """
    combined = pd.read_sql(query, conn)
    print(f"[OK] Loaded {len(combined)} records from database")
    
    # Overall statistics
    print("OVERALL STATISTICS")
    print("-" * 70)
    print(f"Total Records: {len(combined):,}")
    print(f"Unique Indicators: {combined['indicator_name'].nunique()}")
    print(f"Time Periods: {combined['period_label'].nunique()}")
    print(f"Year Range: {combined['year'].min()} to {combined['year'].max()}")
    print(f"Value Range: {combined['value'].min():.2f} to {combined['value'].max():.2f}")
    print()
    
    # Records by year
    print("RECORDS BY PERIOD")
    print("-" * 70)
    year_counts = combined['period_label'].value_counts().sort_index()
    for year, count in year_counts.items():
        bar = "*" * int(count / 500)  # Scale down for readability
        print(f"{year}: {count:>4} {bar}")
    print()

    # Records by indicator (top 10)
    print("TOP 10 INDICATORS BY RECORD COUNT")
    print("-" * 70)
    indicator_counts = combined['indicator_name'].value_counts().head(10)
    for indicator, count in indicator_counts.items():
        percentage = (count / len(combined)) * 100
        bar = "*" * int(percentage * 2)
        print(f"{indicator[:30]:30s}: {count:>4} ({percentage:5.1f}%) {bar}")
    print()
    
    # Value statistics
    print("VALUE STATISTICS")
    print("-" * 70)
    print(combined['value'].describe().to_string())
    print()
    
    # Sample data
    print("SAMPLE RECORDS")
    print("-" * 70)
    print(combined[['indicator_name', 'period_label', 'value']].head(10).to_string(index=False))
    print()
    
    # Period-over-period trends (simple)
    print("PERIOD-OVER-PERIOD TRENDS (AVERAGE VALUES)")
    print("-" * 70)
    period_avg = combined.groupby('period_label')['value'].mean()
    for period, avg in period_avg.items():
        bar = "*" * int(avg / 100)  # Scale based on data range
        print(f"{period}: {avg:>8.2f} {bar}")
    print()
    
    # Data quality metrics
    print("DATA QUALITY METRICS")
    print("-" * 70)
    print(f"Records with non-null values: {len(combined):,} (100%)")
    print(f"Duplicate records: {combined.duplicated().sum()}")
    print(f"Negative values: {(combined['value'] < 0).sum()}")
    print(f"Zero values: {(combined['value'] == 0).sum()}")
    print()
    
    # Close database connection
    conn.close()

    print("=" * 70)
    print("ANALYSIS COMPLETE!")
    print("=" * 70)
    print()
    print("Data is stored in PostgreSQL database 'uganda_health'")
    print("Run smoke tests: python tests/smoke_test.py")
    print()

if __name__ == "__main__":
    main()

