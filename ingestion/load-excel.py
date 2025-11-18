"""
ingestion/load_excel.py
Reads ALL Excel files from data/source/ and writes each sheet to data/raw/<filename>_<sheet_name>.csv
Supports multiple Excel files for scalable data ingestion
"""

import os
import sys
import pandas as pd
from pathlib import Path
import logging

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from observability import ObservedPipeline, HealthDataValidator

# Configure paths
SOURCE_DIR = Path("data/source")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def sanitize_sheet_name(name: str) -> str:
    """Sanitize sheet name to be filesystem-safe"""
    return "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in name).strip().replace(" ", "_")


def process_excel_file(excel_file: Path, observer: ObservedPipeline) -> tuple[int, int]:
    """
    Process a single Excel file and extract all sheets to CSV

    Args:
        excel_file: Path to Excel file
        observer: ObservedPipeline instance for tracking

    Returns:
        Tuple of (total_rows, sheets_processed)
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"Processing: {excel_file.name}")
    logger.info(f"{'='*70}")

    # Read Excel file
    xls = pd.ExcelFile(excel_file)
    schema_info = {
        'sheet_names': xls.sheet_names,
        'sheet_count': len(xls.sheet_names)
    }

    logger.info(f"Found {len(xls.sheet_names)} sheets: {', '.join(xls.sheet_names)}")

    # Register source file
    file_id = observer.register_source_file(
        file_path=str(excel_file),
        sheet_count=len(xls.sheet_names),
        schema_fingerprint=schema_info
    )

    total_rows = 0
    sheets_processed = 0
    file_stem = excel_file.stem  # Filename without extension

    # Process each sheet
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet, header=None)

            # Create unique CSV name: filename_sheetname.csv
            # This prevents conflicts when multiple files have sheets with same names
            out_name = sanitize_sheet_name(f"{file_stem}_{sheet}") + ".csv"
            out_path = RAW_DIR / out_name

            # Write CSV
            df.to_csv(out_path, index=False, encoding='utf-8')
            logger.info(f"  ✓ Sheet '{sheet}' -> {out_name} ({len(df)} rows × {len(df.columns)} cols)")

            # Track lineage for this sheet
            observer.track_lineage(
                target_table='raw_csv',
                target_column='all_columns',
                source_file=str(excel_file),
                source_sheet=sheet,
                source_column='all_columns',
                transformation_logic=f'Direct extraction from Excel file {excel_file.name}, sheet {sheet} to CSV',
                transformation_type='direct_copy'
            )

            # Basic data quality check on raw data
            validator = HealthDataValidator(observer)

            # Check completeness
            if len(df) > 0 and len(df.columns) > 0:
                null_pct = df.isnull().sum().sum() / (len(df) * len(df.columns))
                observer.log_quality_check(
                    check_name=f'raw_completeness_{file_stem}_{sheet}',
                    passed=null_pct < 0.5,  # Raw data can have more nulls
                    check_category='completeness',
                    table_name=f'raw_{file_stem}_{sheet}',
                    metric_value=1 - null_pct,
                    threshold_value=0.5,
                    row_count=len(df),
                    details={
                        'source_file': excel_file.name,
                        'sheet_name': sheet,
                        'columns': len(df.columns)
                    }
                )

            total_rows += len(df)
            sheets_processed += 1

        except Exception as e:
            logger.error(f"  ✗ Failed to process sheet '{sheet}': {str(e)}")
            # Continue processing other sheets
            continue

    return total_rows, sheets_processed


def main():
    """Main ingestion function - processes all Excel files in source directory"""

    logger.info("="*70)
    logger.info("UGANDA HEALTH DATA INGESTION")
    logger.info("="*70)

    # Find all Excel files in source directory
    excel_files = list(SOURCE_DIR.glob("*.xlsx")) + list(SOURCE_DIR.glob("*.xls"))

    if not excel_files:
        raise FileNotFoundError(
            f"No Excel files found in {SOURCE_DIR}. "
            f"Please place .xlsx or .xls files in the data/source/ directory."
        )

    logger.info(f"Found {len(excel_files)} Excel file(s) to process:")
    for f in excel_files:
        logger.info(f"  • {f.name}")

    # Process all files
    grand_total_rows = 0
    grand_total_sheets = 0
    files_processed = 0
    files_failed = 0

    for excel_file in excel_files:
        # Use ObservedPipeline context manager for automatic tracking
        # Each file gets its own pipeline run for better observability
        with ObservedPipeline('uganda_health_etl', 'ingestion', str(excel_file)) as observer:
            try:
                total_rows, sheets_processed = process_excel_file(excel_file, observer)

                # Complete run with statistics
                observer.complete_run(
                    status='success',
                    records_input=total_rows,
                    records_processed=total_rows,
                    records_loaded=total_rows
                )

                grand_total_rows += total_rows
                grand_total_sheets += sheets_processed
                files_processed += 1

                logger.info(f"✓ {excel_file.name}: {sheets_processed} sheets, {total_rows:,} rows")

            except Exception as e:
                logger.error(f"✗ Failed to process {excel_file.name}: {str(e)}")
                observer.complete_run(
                    status='failed',
                    records_input=0,
                    records_processed=0,
                    records_loaded=0,
                    error_message=str(e)
                )
                files_failed += 1
                # Continue processing other files
                continue

    # Final summary
    logger.info("")
    logger.info("="*70)
    logger.info("INGESTION SUMMARY")
    logger.info("="*70)
    logger.info(f"Files Processed:    {files_processed}/{len(excel_files)}")
    logger.info(f"Files Failed:       {files_failed}")
    logger.info(f"Total Sheets:       {grand_total_sheets}")
    logger.info(f"Total Rows:         {grand_total_rows:,}")
    logger.info(f"Output Directory:   {RAW_DIR.absolute()}")
    logger.info("="*70)

    if files_failed > 0:
        logger.warning(f"⚠ {files_failed} file(s) failed to process. Check logs above for details.")

    if files_processed == 0:
        raise RuntimeError("All files failed to process. Ingestion unsuccessful.")

if __name__ == "__main__":
    main()
