"""
transform/clean_and_unpivot.py
Takes CSVs from data/raw/, applies cleaning & unpivot to long format, writes to data/clean/.
"""

import os
import pandas as pd
from pathlib import Path
import numpy as np

RAW_DIR = Path("data/raw")
CLEAN_DIR = Path("data/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def detect_header_and_clean(df: pd.DataFrame):
    """
    Heuristic: find first row that contains year-like strings (e.g., '2016/17' or '2016').
    Otherwise assume row 0 is header.
    Returns dataframe with proper column names and dropped top meta rows.
    """
    header_row = 0
    found_years = False
    
    for i in range(min(10, len(df))):
        row = df.iloc[i].astype(str).tolist()
        # Check if this row has multiple year-like values (2016/17 format)
        year_count = sum(1 for x in row if "/" in str(x) and any(ch.isdigit() for ch in str(x)))
        if year_count >= 2:  # Need at least 2 year columns
            header_row = i
            found_years = True
            break
    
    if found_years:
        # Use the year row as column names, making them unique
        new_cols = []
        year_cols = df.iloc[header_row].tolist()
        col_counts = {}
        for i, col in enumerate(year_cols):
            if pd.notna(col) and str(col).strip():
                col_name = str(col).strip()
                # Make duplicate column names unique
                if col_name in col_counts:
                    col_counts[col_name] += 1
                    new_cols.append(f"{col_name}_{col_counts[col_name]}")
                else:
                    col_counts[col_name] = 0
                    new_cols.append(col_name)
            else:
                new_cols.append(f"col_{i}")
        df.columns = new_cols
        df = df.iloc[header_row+1:].reset_index(drop=True)
    
    # Drop all-empty columns
    df = df.dropna(axis=1, how="all")
    # Drop all-empty rows
    df = df.dropna(axis=0, how="all")
    # Trim whitespace in column names
    df.columns = [str(c).strip() for c in df.columns]
    return df

def unpivot_years(df: pd.DataFrame, id_cols: list):
    """
    Melt year/value columns. Detect columns that look like years or '2016/17' patterns.
    """
    # Identify year columns
    year_cols = [c for c in df.columns if isinstance(c, str) and ("/" in c and any(ch.isdigit() for ch in c) or c.strip().isdigit())]
    if not year_cols:
        # fallback: take columns that contain digits
        year_cols = [c for c in df.columns if any(ch.isdigit() for ch in str(c))]
    
    # Ensure id_cols exist in dataframe
    valid_id_cols = [col for col in id_cols if col in df.columns]
    if not valid_id_cols:
        valid_id_cols = [df.columns[0]]
    
    # Manual melt to avoid pandas bug with newer versions
    records = []
    for idx, row in df.iterrows():
        for year_col in year_cols:
            record = {id_col: row[id_col] for id_col in valid_id_cols}
            record['year_label'] = year_col
            record['value'] = row[year_col]
            records.append(record)
    
    long = pd.DataFrame(records)
    return long

def process_file(path: Path):
    print(f"Processing {path}")
    # read with header=None so we can find header heuristically
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path, header=None, encoding="latin1")
    # If first column mostly empty, try shift
    # Convert to pandas DataFrame if not
    # Heuristic header detection
    try:
        df2 = detect_header_and_clean(df)
    except Exception:
        # fallback: assume first row is header
        df.columns = df.iloc[0].astype(str).tolist()
        df = df.iloc[1:].reset_index(drop=True)
        df2 = df

    # determine id columns: text columns to keep (first column usually indicator)
    id_col_candidates = [c for c in df2.columns if not any(ch.isdigit() for ch in str(c))]
    if not id_col_candidates:
        id_cols = [df2.columns[0]]
    else:
        id_cols = [id_col_candidates[0]]

    long = unpivot_years(df2, id_cols=id_cols)
    # normalize: remove rows with empty values
    long = long.dropna(subset=["value"])
    # clean whitespace
    first_col = long.columns[0]  # Use actual first column name
    long[first_col] = long[first_col].astype(str).str.strip()
    long["value"] = pd.to_numeric(long["value"].astype(str).str.replace(",", "").str.strip(), errors="coerce")
    long = long.dropna(subset=["value"])
    return long

def main():
    for path in RAW_DIR.glob("*.csv"):
        try:
            long = process_file(path)
            if long is None or long.empty:
                print(f"Skipping {path.name} - no valid data")
                continue
            out_name = CLEAN_DIR / path.name.replace(".csv", "_clean.csv")
            long.to_csv(out_name, index=False)
            print(f"Wrote cleaned file {out_name}")
        except Exception as e:
            print(f"Error processing {path.name}: {e}")
            print(f"Skipping {path.name}")

if __name__ == "__main__":
    main()
