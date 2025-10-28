"""
warehouse/load_to_postgres.py
Loads cleaned CSVs into Postgres warehouse.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path("conf/.env"))  # make sure you copy conf/db_env.example -> conf/.env

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB = os.getenv("PG_DB", "uganda_health")

ENGINE = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}", echo=False)

SCHEMA_SQL = Path("warehouse/schema.sql").read_text()

def create_schema():
    print("Creating/verifying schema...")
    schema_path = Path("warehouse/schema.sql")
    print(f"Schema file path: {schema_path.absolute()}")
    print(f"Schema file exists: {schema_path.exists()}")

    global SCHEMA_SQL
    SCHEMA_SQL = schema_path.read_text()
    print(f"Schema SQL length: {len(SCHEMA_SQL)} characters")

    if not SCHEMA_SQL.strip():
        print("ERROR: Schema SQL is empty!")
        return

    with ENGINE.connect() as conn:
        conn.execute(text(SCHEMA_SQL))
        conn.commit()
    print("Schema ensured.")

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
    print("Starting data load process...")
    create_schema()

    # iterate cleaned files
    clean_dir = Path("data/clean")
    csv_files = list(clean_dir.glob("*_clean.csv"))
    print(f"Found {len(csv_files)} clean CSV files")

    for path in csv_files:
        print(f"\nProcessing {path.name}...")
        df = pd.read_csv(path)
        print(f"  - Loaded {len(df)} rows, {len(df.columns)} columns")

        # ensure columns exist: indicator, year_label, value
        if df.shape[1] >= 3:
            # attempt to map first -> indicator, year_label, value
            df = df.rename(columns={df.columns[0]: "indicator", df.columns[1]: "year_label", df.columns[2]: "value"})
            print(f"  - Mapped columns: {list(df.columns)}")

        upsert_dim_indicator(df)
        upsert_date(df)
        load_fact(df)
        print(f"  - Completed processing {path.name}")

    print("\nData load process complete!")

if __name__ == "__main__":
    main()
