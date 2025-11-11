"""
observability/init_observability.py
Initialize observability schema in database
"""

import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path("conf/.env"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB = os.getenv("PG_DB", "uganda_health")

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    echo=False
)


def init_observability_schema():
    """Initialize observability schema"""
    print("Initializing observability schema...")

    schema_path = Path("warehouse/observability_schema.sql")

    if not schema_path.exists():
        print(f"ERROR: Schema file not found at {schema_path.absolute()}")
        return False

    schema_sql = schema_path.read_text()
    print(f"Read schema SQL ({len(schema_sql)} characters)")

    try:
        with ENGINE.begin() as conn:
            conn.execute(text(schema_sql))

        print("[SUCCESS] Observability schema created successfully!")

        # Verify tables were created
        with ENGINE.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'metadata'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]

        print(f"\nCreated {len(tables)} metadata tables:")
        for table in tables:
            print(f"  - metadata.{table}")

        return True

    except Exception as e:
        print(f"[ERROR] Error creating schema: {e}")
        return False


if __name__ == "__main__":
    success = init_observability_schema()
    exit(0 if success else 1)
