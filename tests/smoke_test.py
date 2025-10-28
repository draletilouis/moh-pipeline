"""
tests/smoke_test.py
Basic smoke tests to verify the pipeline ran successfully
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_raw_files_exist():
    """Check that raw CSV files were created"""
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        print("[FAIL] FAIL: data/raw directory doesn't exist")
        return False
    
    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        print("[FAIL] FAIL: No CSV files found in data/raw")
        return False
    
    print(f"[OK] PASS: Found {len(csv_files)} raw CSV file(s)")
    for f in csv_files:
        print(f"   - {f.name}")
    return True

def test_clean_files_exist():
    """Check that cleaned CSV files were created"""
    clean_dir = Path("data/clean")
    if not clean_dir.exists():
        print("[FAIL] FAIL: data/clean directory doesn't exist")
        return False
    
    csv_files = list(clean_dir.glob("*_clean.csv"))
    if not csv_files:
        print("[FAIL] FAIL: No cleaned CSV files found in data/clean")
        return False
    
    print(f"[OK] PASS: Found {len(csv_files)} cleaned CSV file(s)")
    for f in csv_files:
        print(f"   - {f.name}")
    return True

def test_clean_data_structure():
    """Verify cleaned data has expected columns"""
    import pandas as pd
    clean_dir = Path("data/clean")
    
    expected_cols = ['year_label', 'value']  # Common columns after unpivot
    
    for csv_file in clean_dir.glob("*_clean.csv"):
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                print(f"⚠️  WARN: {csv_file.name} is empty")
                continue
            
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                print(f"[FAIL] FAIL: {csv_file.name} missing columns: {missing_cols}")
                print(f"   Found columns: {list(df.columns)}")
                return False
            
            # Check for data quality
            null_values = df['value'].isna().sum()
            total_rows = len(df)
            
            print(f"[OK] PASS: {csv_file.name} structure valid")
            print(f"   - Rows: {total_rows}")
            print(f"   - Columns: {list(df.columns)}")
            print(f"   - Null values: {null_values}/{total_rows}")
            
        except Exception as e:
            print(f"[FAIL] FAIL: Error reading {csv_file.name}: {e}")
            return False
    
    return True

def test_database_connection():
    """Test connection to PostgreSQL"""
    try:
        from sqlalchemy import create_engine, text
        load_dotenv(dotenv_path=Path("conf/.env"))
        
        PG_HOST = os.getenv("PG_HOST", "localhost")
        PG_PORT = os.getenv("PG_PORT", "5432")
        PG_USER = os.getenv("PG_USER", "postgres")
        PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
        PG_DB = os.getenv("PG_DB", "uganda_health")
        
        engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
            echo=False
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        print("[OK] PASS: Database connection successful")
        return True
        
    except Exception as e:
        print(f"[FAIL] FAIL: Database connection failed: {e}")
        return False

def test_schema_exists():
    """Check that the health schema and tables exist"""
    try:
        from sqlalchemy import create_engine, text, inspect
        load_dotenv(dotenv_path=Path("conf/.env"))
        
        PG_HOST = os.getenv("PG_HOST", "localhost")
        PG_PORT = os.getenv("PG_PORT", "5432")
        PG_USER = os.getenv("PG_USER", "postgres")
        PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
        PG_DB = os.getenv("PG_DB", "uganda_health")
        
        engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
            echo=False
        )
        
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        
        if 'health' not in schemas:
            print("[FAIL] FAIL: 'health' schema not found")
            return False
        
        print("[OK] PASS: 'health' schema exists")
        
        # Check for expected tables
        expected_tables = ['dim_indicator', 'dim_date', 'dim_location', 'fact_indicator_values']
        existing_tables = inspector.get_table_names(schema='health')
        
        for table in expected_tables:
            if table in existing_tables:
                print(f"   [OK] Table: health.{table}")
            else:
                print(f"   [MISSING] Missing table: health.{table}")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] FAIL: Schema check failed: {e}")
        return False

def test_data_loaded():
    """Check that data was loaded into warehouse tables"""
    try:
        from sqlalchemy import create_engine, text
        load_dotenv(dotenv_path=Path("conf/.env"))
        
        PG_HOST = os.getenv("PG_HOST", "localhost")
        PG_PORT = os.getenv("PG_PORT", "5432")
        PG_USER = os.getenv("PG_USER", "postgres")
        PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
        PG_DB = os.getenv("PG_DB", "uganda_health")
        
        engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
            echo=False
        )
        
        tables_to_check = [
            ('health.dim_indicator', 'indicator_id'),
            ('health.dim_date', 'date_id'),
            ('health.fact_indicator_values', 'fact_id')
        ]
        
        all_passed = True
        for table, id_col in tables_to_check:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table}"))
                count = result.fetchone()[0]
                
                if count > 0:
                    print(f"[OK] PASS: {table} has {count} row(s)")
                else:
                    print(f"⚠️  WARN: {table} is empty")
                    all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"[FAIL] FAIL: Data load check failed: {e}")
        return False

def main():
    """Run all smoke tests"""
    print("=" * 60)
    print("SMOKE TESTS - Uganda Health Pipeline")
    print("=" * 60)
    print()
    
    tests = [
        ("Raw files exist", test_raw_files_exist),
        ("Clean files exist", test_clean_files_exist),
        ("Clean data structure", test_clean_data_structure),
        ("Database connection", test_database_connection),
        ("Schema exists", test_schema_exists),
        ("Data loaded", test_data_loaded),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n--- Test: {test_name} ---")
        try:
            passed = test_func()
            results.append((test_name, passed))
        except Exception as e:
            print(f"[FAIL] FAIL: Unexpected error: {e}")
            results.append((test_name, False))
        print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for test_name, passed in results:
        status = "[OK] PASS" if passed else "[FAIL] FAIL"
        print(f"{status}: {test_name}")
    
    print()
    print(f"Total: {passed_count}/{total_count} tests passed")
    print("=" * 60)
    
    # Return exit code
    sys.exit(0 if passed_count == total_count else 1)

if __name__ == "__main__":
    main()

