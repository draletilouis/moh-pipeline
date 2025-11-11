"""
warehouse/load_to_postgres.py
Loads cleaned CSVs into Postgres warehouse.
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from observability import ObservedPipeline, DataQualityValidator

load_dotenv(dotenv_path=Path("conf/.env"))  # make sure you copy conf/db_env.example -> conf/.env

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB = os.getenv("PG_DB", "uganda_health")

ENGINE = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}", echo=False)

SCHEMA_SQL = Path("warehouse/schema.sql").read_text()
OBSERVABILITY_SCHEMA = Path("warehouse/observability_schema.sql").read_text()

def create_schema():
    print("Creating/verifying schema...")
    schema_path = Path("warehouse/schema.sql")
    observability_path = Path("warehouse/observability_schema.sql")

    print(f"Schema file path: {schema_path.absolute()}")
    print(f"Schema file exists: {schema_path.exists()}")
    print(f"Observability schema exists: {observability_path.exists()}")

    global SCHEMA_SQL, OBSERVABILITY_SCHEMA
    SCHEMA_SQL = schema_path.read_text()
    print(f"Schema SQL length: {len(SCHEMA_SQL)} characters")

    if not SCHEMA_SQL.strip():
        print("ERROR: Schema SQL is empty!")
        return

    # Create main warehouse schema
    with ENGINE.connect() as conn:
        conn.execute(text(SCHEMA_SQL))
        conn.commit()
    print("[SUCCESS] Warehouse schema ensured.")

    # Create observability schema
    if observability_path.exists():
        OBSERVABILITY_SCHEMA = observability_path.read_text()
        with ENGINE.connect() as conn:
            conn.execute(text(OBSERVABILITY_SCHEMA))
            conn.commit()
        print("[SUCCESS] Observability schema ensured.")
    else:
        print("[WARNING] Observability schema not found - skipping")

def upsert_dim_indicator(df: pd.DataFrame):
    # df expected to have column 'indicator' or similar
    if "indicator" not in df.columns:
        # try first column name as indicator
        df = df.rename(columns={df.columns[0]: "indicator"})
    unique = df["indicator"].dropna().unique()
    with ENGINE.begin() as conn:
        for u in unique:
            conn.execute(text("""
                INSERT INTO health.dim_indicator (indicator_key, indicator_name)
                VALUES (:k, :n)
                ON CONFLICT (indicator_key) DO NOTHING
            """), {"k": u.lower().replace(" ", "_")[:255], "n": u})
    print("dim_indicator upsert completed.")

def upsert_date(df: pd.DataFrame):
    # df must have 'year_label' like '2016/17' - convert to a date of start of period
    labels = df["year_label"].dropna().unique()
    entries = []
    for lab in labels:
        # try parse e.g., "2016/17" -> 2016-07-01 as canonical (adjust as needed)
        try:
            if "/" in lab:
                start = int(str(lab).split("/")[0])
                date_value = f"{start}-07-01"  # choose mid-year marker
                year = start
            else:
                year = int(str(lab).strip())
                date_value = f"{year}-01-01"
        except Exception:
            date_value = None
            year = None
        entries.append({"year": year, "period_label": lab, "date_value": date_value})
    with ENGINE.begin() as conn:
        for e in entries:
            conn.execute(text("""
                INSERT INTO health.dim_date (year, period_label, date_value)
                VALUES (:year, :label, :dv)
                ON CONFLICT DO NOTHING
            """), {"year": e["year"], "label": e["period_label"], "dv": e["date_value"]})
    print("dim_date upserted.")

def load_fact(df: pd.DataFrame):
    # df should have columns: indicator, year_label, value, optional location columns
    # Resolve indicator_id and date_id
    temp_table = "health._temp_load"
    with ENGINE.begin() as conn:
        conn.execute(text("CREATE TEMP TABLE IF NOT EXISTS _temp_load (indicator TEXT, year_label TEXT, value NUMERIC, location TEXT)"))
        # insert rows
        df_to_insert = df.rename(columns={df.columns[0]: "indicator"})
        records = df_to_insert.to_dict(orient="records")
        for r in records:
            conn.execute(text("INSERT INTO _temp_load (indicator, year_label, value, location) VALUES (:indicator, :year_label, :value, :location)"),
                         {"indicator": r.get("indicator"), "year_label": r.get("year_label"), "value": r.get("value"), "location": r.get("location")})
        # insert into dim tables as needed
        conn.execute(text("""
            INSERT INTO health.dim_indicator (indicator_key, indicator_name)
            SELECT DISTINCT lower(indicator), indicator FROM _temp_load
            ON CONFLICT (indicator_name) DO NOTHING
        """))
        conn.execute(text("""
            INSERT INTO health.dim_date (period_label, date_value)
            SELECT DISTINCT year_label, NULL::DATE FROM _temp_load
            ON CONFLICT DO NOTHING
        """))
        # final insert into fact (simple approach: join on names)
        conn.execute(text("""
            INSERT INTO health.fact_indicator_values (indicator_id, date_id, value, units, notes)
            SELECT i.indicator_id, d.date_id, t.value, NULL, NULL
            FROM _temp_load t
            LEFT JOIN health.dim_indicator i ON lower(i.indicator_name) = lower(t.indicator)
            LEFT JOIN health.dim_date d ON d.period_label = t.year_label
        """))
    print("Fact load complete.")

def main():
    # Use ObservedPipeline context manager
    with ObservedPipeline('uganda_health_etl', 'load') as observer:

        print("Starting data load process...")
        create_schema()

        # iterate cleaned files
        clean_dir = Path("data/clean")
        csv_files = list(clean_dir.glob("*_clean.csv"))
        print(f"Found {len(csv_files)} clean CSV files")

        total_records = 0
        records_loaded = 0
        files_processed = 0

        for path in csv_files:
            print(f"\nProcessing {path.name}...")
            df = pd.read_csv(path)
            print(f"  - Loaded {len(df)} rows, {len(df.columns)} columns")

            total_records += len(df)

            # ensure columns exist: indicator, year_label, value
            if df.shape[1] >= 3:
                # attempt to map first -> indicator, year_label, value
                df = df.rename(columns={df.columns[0]: "indicator", df.columns[1]: "year_label", df.columns[2]: "value"})
                print(f"  - Mapped columns: {list(df.columns)}")

            # Data quality checks before loading
            validator = DataQualityValidator(observer)

            # Check completeness of key fields
            key_fields = ['indicator', 'year_label', 'value']
            for field in key_fields:
                if field in df.columns:
                    null_count = df[field].isnull().sum()
                    null_pct = null_count / len(df)
                    observer.log_quality_check(
                        check_name=f'warehouse_completeness_{field}',
                        passed=null_pct == 0,  # Should be 0 nulls at this stage
                        check_category='completeness',
                        table_name='fact_indicator_values',
                        column_name=field,
                        metric_value=1 - null_pct,
                        threshold_value=1.0,
                        row_count=len(df),
                        failure_count=int(null_count)
                    )

            # Load dimensions and facts
            upsert_dim_indicator(df)
            upsert_date(df)
            load_fact(df)

            # Track lineage
            observer.track_lineage(
                target_table='fact_indicator_values',
                target_column='value',
                source_file=str(path),
                source_column='value',
                transformation_logic='Loaded from clean CSV via upsert',
                transformation_type='direct_copy'
            )

            # Check referential integrity
            with ENGINE.connect() as conn:
                orphan_check = conn.execute(text("""
                    SELECT COUNT(*) FROM health.fact_indicator_values
                    WHERE indicator_id IS NULL OR date_id IS NULL
                """)).scalar()

                observer.log_quality_check(
                    check_name='warehouse_referential_integrity',
                    passed=orphan_check == 0,
                    check_category='consistency',
                    table_name='fact_indicator_values',
                    metric_value=1.0 if orphan_check == 0 else 0.0,
                    threshold_value=1.0,
                    failure_count=int(orphan_check),
                    details={'orphan_records': int(orphan_check)}
                )

            records_loaded += len(df)
            files_processed += 1
            print(f"  [SUCCESS] Completed processing {path.name}")

        # Final warehouse statistics
        with ENGINE.connect() as conn:
            fact_count = conn.execute(text("SELECT COUNT(*) FROM health.fact_indicator_values")).scalar()
            indicator_count = conn.execute(text("SELECT COUNT(*) FROM health.dim_indicator")).scalar()
            date_count = conn.execute(text("SELECT COUNT(*) FROM health.dim_date")).scalar()

            observer.log_quality_check(
                check_name='warehouse_fact_count',
                passed=fact_count > 0,
                check_category='consistency',
                table_name='fact_indicator_values',
                metric_value=float(fact_count),
                threshold_value=1.0,
                details={
                    'fact_records': int(fact_count),
                    'indicators': int(indicator_count),
                    'time_periods': int(date_count)
                }
            )

        print(f"\n[SUCCESS] Data load complete!")
        print(f"  Files processed: {files_processed}")
        print(f"  Records input: {total_records}")
        print(f"  Warehouse stats: {fact_count} facts, {indicator_count} indicators, {date_count} periods")

        # Complete the run
        observer.complete_run(
            status='success',
            records_input=total_records,
            records_processed=records_loaded,
            records_loaded=int(fact_count)
        )

if __name__ == "__main__":
    main()
