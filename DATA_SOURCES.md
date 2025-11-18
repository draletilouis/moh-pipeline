# Data Sources Guide

## Overview

The Uganda Health Pipeline supports **multiple Excel files** as data sources. Simply place any number of `.xlsx` or `.xls` files in the `data/source/` directory, and the pipeline will automatically process them all.

## How It Works

### 1. Automatic Discovery

The ingestion stage automatically discovers **all Excel files** in the source directory:

```
data/source/
├── health_2016_2020.xlsx
├── health_2020_2022.xlsx
├── district_indicators.xlsx
└── regional_summary.xlsx
```

### 2. Multi-File Processing

Each Excel file is processed independently:
- All sheets from each file are extracted
- Each sheet becomes a CSV file: `{filename}_{sheetname}.csv`
- Naming prevents conflicts between files with identical sheet names

**Example:**

```
Input Files:
  health_2016_2020.xlsx (5 sheets)
  districts.xlsx (2 sheets)

Output CSVs:
  data/raw/health_2016_2020_Sheet1.csv
  data/raw/health_2016_2020_Sheet2.csv
  data/raw/health_2016_2020_Sheet3.csv
  data/raw/health_2016_2020_Sheet4.csv
  data/raw/health_2016_2020_Sheet5.csv
  data/raw/districts_Summary.csv
  data/raw/districts_Details.csv
```

### 3. Automatic Consolidation

The transform stage automatically:
- Finds all CSV files in `data/raw/`
- Processes and unpivots each one
- Combines all data into a single clean dataset
- Loads everything to the warehouse

## Usage

### Adding New Data Sources

Simply copy Excel files to the source directory:

```bash
# Windows
copy "C:\path\to\new_data.xlsx" "data\source\"

# Linux/Mac
cp /path/to/new_data.xlsx data/source/
```

Then run the pipeline:

```bash
python run_pipeline.py
```

### Expected Output

```
======================================================================
UGANDA HEALTH DATA INGESTION
======================================================================
Found 3 Excel file(s) to process:
  • health_2016_2020.xlsx
  • health_2020_2022.xlsx
  • districts.xlsx

======================================================================
Processing: health_2016_2020.xlsx
======================================================================
Found 5 sheets: Sheet1, Sheet2, Sheet3, Sheet4, Sheet5
  ✓ Sheet 'Sheet1' -> health_2016_2020_Sheet1.csv (150 rows × 8 cols)
  ✓ Sheet 'Sheet2' -> health_2016_2020_Sheet2.csv (200 rows × 6 cols)
  ...
✓ health_2016_2020.xlsx: 5 sheets, 1,250 rows

======================================================================
Processing: health_2020_2022.xlsx
======================================================================
Found 3 sheets: Summary, Details, Trends
  ✓ Sheet 'Summary' -> health_2020_2022_Summary.csv (100 rows × 5 cols)
  ...
✓ health_2020_2022.xlsx: 3 sheets, 750 rows

======================================================================
INGESTION SUMMARY
======================================================================
Files Processed:    3/3
Files Failed:       0
Total Sheets:       10
Total Rows:         3,500
Output Directory:   C:\...\data\raw
======================================================================
```

## File Requirements

### Supported Formats
- ✅ `.xlsx` (Excel 2007+)
- ✅ `.xls` (Excel 97-2003)

### File Structure
- No specific naming convention required
- Any number of sheets per file
- Each sheet should contain:
  - One or more indicator columns
  - Year columns (e.g., `2016/17`, `2017/18`, `2019`, `2020`)
  - Data values

### Automatic Detection
The pipeline intelligently detects:
- Header rows (even if not first row)
- Year columns (flexible format: `2016/17`, `2016-17`, `2016`)
- Indicator columns
- Empty rows/columns (automatically removed)

## Advanced Features

### 1. Idempotent Processing

Re-running the pipeline with the same files:
- ✅ No duplicates created
- ✅ Uses UPSERT logic
- ✅ MD5 hash tracking detects file changes

### 2. Partial Failure Handling

If one file fails:
- ⚠️ Error logged for that file
- ✅ Other files continue processing
- ✅ Pipeline completes with summary of failures

### 3. Observability

Each file tracked separately:
- Individual pipeline runs in metadata
- File-level lineage tracking
- Quality checks per file
- Success/failure status

### 4. Quality Validation

For each file and sheet:
- Completeness checks (null value detection)
- Row/column counts validated
- Data type consistency
- Results logged to metadata schema

## Examples

### Example 1: Single File (Original)

```bash
data/source/
└── health_indicators.xlsx

# Output:
# data/raw/health_indicators_Sheet1.csv
# data/raw/health_indicators_Sheet2.csv
# etc.
```

### Example 2: Multiple Files (New!)

```bash
data/source/
├── hospitals_2016_2020.xlsx
├── districts_2016_2020.xlsx
└── regions_2021_2022.xlsx

# Output:
# data/raw/hospitals_2016_2020_Performance.csv
# data/raw/hospitals_2016_2020_Indicators.csv
# data/raw/districts_2016_2020_Summary.csv
# data/raw/districts_2016_2020_Details.csv
# data/raw/regions_2021_2022_Regional.csv
# data/raw/regions_2021_2022_National.csv
```

### Example 3: Incremental Updates

```bash
# Initial run
data/source/health_2016_2020.xlsx

# Later: Add new data
data/source/health_2020_2022.xlsx  # New file added

# Run pipeline again
python run_pipeline.py

# Result: Both files processed, new data added to warehouse
```

## Best Practices

### 1. File Naming
Use descriptive names:
- ✅ `hospitals_performance_2016_2020.xlsx`
- ✅ `district_health_indicators_2021.xlsx`
- ❌ `data.xlsx`
- ❌ `file1.xlsx`

### 2. Consistent Structure
Within a data domain, use consistent:
- Column names
- Date formats
- Indicator naming

### 3. Data Organization
Consider organizing by:
- **Time period**: `health_2016_2020.xlsx`, `health_2020_2022.xlsx`
- **Geographic level**: `districts.xlsx`, `regions.xlsx`, `national.xlsx`
- **Data type**: `hospitals.xlsx`, `clinics.xlsx`, `community_health.xlsx`

### 4. Backup Sources
Keep a copy of source files:
```bash
data/
├── source/          # Active files
└── archive/         # Historical files
    ├── 2022/
    ├── 2023/
    └── 2024/
```

## Troubleshooting

### No Files Found

**Error:**
```
FileNotFoundError: No Excel files found in data/source
```

**Solution:**
- Verify files are in `data/source/` directory
- Check file extensions (`.xlsx` or `.xls`)
- Ensure files aren't in subdirectories

### File Processing Failed

**Error:**
```
✗ Failed to process health_data.xlsx: [error details]
```

**Solutions:**
- Check file isn't corrupted (try opening in Excel)
- Verify file isn't password-protected
- Ensure file has at least one sheet with data
- Check file permissions

### Duplicate Data

**Issue:** Same data appearing multiple times

**Causes:**
- Same file with different names
- Re-running pipeline creates duplicates

**Solution:**
- UPSERT logic should prevent this
- Check `dim_indicator` table for duplicate indicator names
- Review warehouse loading logic

## Migration Guide

### Upgrading from Single-File Setup

**Before:**
```python
# Old: Hardcoded single file
INPUT_FILE = Path("data/source/specific_file.xlsx")
```

**After:**
```python
# New: All files in directory
excel_files = list(SOURCE_DIR.glob("*.xlsx"))
```

**Steps:**
1. Existing file continues to work
2. Add new files to `data/source/` as needed
3. Pipeline automatically processes all

**No breaking changes!** Your existing setup continues to work.

## Performance Considerations

### Large Files
- Each file processed sequentially
- Consider splitting very large files (>50MB)
- Monitor memory usage with many files

### Many Files
- Current: Sequential processing
- Future: Parallel processing option
- Observability tracks each file separately

### Optimization Tips
1. Keep files under 50MB each
2. Limit to 10-20 files per run
3. Archive old data files
4. Use consistent date ranges per file

## Monitoring

Check ingestion success via:

```bash
# CLI Dashboard
python observability/monitor_dashboard.py files

# API
curl -H "X-API-Key: your-key" \
  http://localhost:8000/observability/source-files
```

View:
- Files processed
- Processing timestamps
- Success/failure status
- Row counts
- MD5 hashes (change detection)

---

## Summary

✅ **Zero configuration** - Just drop Excel files in `data/source/`
✅ **Automatic discovery** - All `.xlsx` and `.xls` files processed
✅ **No name conflicts** - Files prefixed with filename
✅ **Resilient** - One file failure doesn't stop others
✅ **Observable** - Complete tracking and lineage
✅ **Idempotent** - Safe to re-run
✅ **Scalable** - Add unlimited files

**Ready to scale!** 🚀
