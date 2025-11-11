"""
ingestion/load_excel.py
Reads the provided Excel file and writes each sheet to data/raw/<sheet_name>.csv
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from observability import ObservedPipeline, HealthDataValidator

INPUT_FILE = Path("data/source/Selected_health_sector_performance_indicators_201617_201920.xlsx")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_sheet_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in name).strip().replace(" ", "_")

def main():
    # Use ObservedPipeline context manager for automatic tracking
    with ObservedPipeline('uganda_health_etl', 'ingestion', str(INPUT_FILE)) as observer:

        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"Expected source file at {INPUT_FILE}. Place your .xlsx there.")

        # Register source file
        xls = pd.ExcelFile(INPUT_FILE)
        schema_info = {
            'sheet_names': xls.sheet_names,
            'sheet_count': len(xls.sheet_names)
        }

        file_id = observer.register_source_file(
            file_path=str(INPUT_FILE),
            sheet_count=len(xls.sheet_names),
            schema_fingerprint=schema_info
        )

        total_rows = 0
        sheets_processed = 0

        # Process each sheet
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, header=None)
            out_name = sanitize_sheet_name(sheet) + ".csv"
            out_path = RAW_DIR / out_name

            # Write CSV
            df.to_csv(out_path, index=False)
            print(f"Wrote raw sheet {sheet} -> {out_path} ({len(df)} rows)")

            # Track lineage for this sheet
            observer.track_lineage(
                target_table='raw_csv',
                target_column='all_columns',
                source_file=str(INPUT_FILE),
                source_sheet=sheet,
                source_column='all_columns',
                transformation_logic='Direct extraction from Excel to CSV',
                transformation_type='direct_copy'
            )

            # Basic data quality check on raw data
            validator = HealthDataValidator(observer)

            # Check completeness
            null_pct = df.isnull().sum().sum() / (len(df) * len(df.columns))
            observer.log_quality_check(
                check_name=f'raw_completeness_{sheet}',
                passed=null_pct < 0.5,  # Raw data can have more nulls
                check_category='completeness',
                table_name=f'raw_{sheet}',
                metric_value=1 - null_pct,
                threshold_value=0.5,
                row_count=len(df),
                details={'sheet_name': sheet, 'columns': len(df.columns)}
            )

            total_rows += len(df)
            sheets_processed += 1

        # Complete run with statistics
        observer.complete_run(
            status='success',
            records_input=total_rows,
            records_processed=total_rows,
            records_loaded=total_rows
        )

        print(f"\n[SUCCESS] Ingestion complete: {sheets_processed} sheets, {total_rows} total rows")

if __name__ == "__main__":
    main()
