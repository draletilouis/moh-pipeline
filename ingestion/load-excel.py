"""
ingestion/load_excel.py
Reads the provided Excel file and writes each sheet to data/raw/<sheet_name>.csv
"""

import os
import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/source/Selected_health_sector_performance_indicators_201617_201920.xlsx")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_sheet_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in name).strip().replace(" ", "_")

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Expected source file at {INPUT_FILE}. Place your .xlsx there.")
    xls = pd.ExcelFile(INPUT_FILE)
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        out_name = sanitize_sheet_name(sheet) + ".csv"
        out_path = RAW_DIR / out_name
        df.to_csv(out_path, index=False)
        print(f"Wrote raw sheet {sheet} -> {out_path}")

if __name__ == "__main__":
    main()
