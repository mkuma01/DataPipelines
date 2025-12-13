"""pandas_schema_converter.py

Read CSV files from a directory into pandas DataFrames, normalize columns,
clean escaped commas in the description column ("\," -> ","), and write
out a JSON schema file for each CSV.

Usage:
    python3 pandas_schema_converter.py --csv_dir path/to/csvs

This script is tolerant of CSVs where commas are either quoted ("a, b") or
backslash-escaped (a\, b).
"""

import os
import argparse
import json
import pandas as pd


def csv_to_json_schema(csv_dir: str, output_dir: str | None = None):
    csv_dir = os.path.abspath(csv_dir)
    if output_dir:
        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = None

    for fname in os.listdir(csv_dir):
        if not fname.lower().endswith('.csv'):
            continue
        file_path = os.path.join(csv_dir, fname)
        try:
            # Use engine='python' which supports escapechar; keep values as strings
            df = pd.read_csv(
                file_path,
                dtype=str,
                keep_default_na=False,
                encoding='utf-8',
                escapechar='\\',
                quotechar='"',
                sep=',',
                engine='python'
            )
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            continue

        # Normalize column names
        df.columns = df.columns.str.strip()

        # Ensure expected columns exist (case-insensitive)
        cols_lower = {c.lower(): c for c in df.columns}
        required = ['name', 'type', 'mode', 'description']
        missing = [r for r in required if r not in cols_lower]
        if missing:
            # Try to map by lowercased names present in the DataFrame
            missing = [r for r in required if r not in cols_lower]
            if missing:
                # If required columns are missing, skip this file
                print(f"Skipping {fname}: missing columns {missing}")
                continue

        # Work with canonical column names
        name_col = cols_lower['name']
        type_col = cols_lower['type']
        mode_col = cols_lower['mode']
        desc_col = cols_lower['description']

        # Replace NaNs / None with empty string and strip
        df[name_col] = df[name_col].fillna('').astype(str).str.strip()
        df[type_col] = df[type_col].fillna('').astype(str).str.strip()
        df[mode_col] = df[mode_col].fillna('').astype(str).str.strip()
        df[desc_col] = df[desc_col].fillna('').astype(str)

        # Unescape backslash-escaped commas ("\\,") -> ","
        df[desc_col] = df[desc_col].str.replace('\\,', ',', regex=False).str.strip()

        json_schema = []
        for _, row in df.iterrows():
            field_name = row[name_col]
            field_type = row[type_col]
            mode = row[mode_col]
            description = row[desc_col]

            # Skip rows that don't have at least a name or type
            if not field_name and not field_type:
                continue

            json_schema.append({
                'name': field_name,
                'type': field_type,
                'mode': mode,
                'description': description
            })

        base = os.path.splitext(fname)[0]
        out_name = f"{base}_schema_pandas.json"
        out_path = os.path.join(output_dir or csv_dir, out_name)
        try:
            with open(out_path, 'w', encoding='utf-8') as fh:
                json.dump(json_schema, fh, indent=2, ensure_ascii=False)
            print(f"Wrote schema for {fname} -> {out_path}")
        except Exception as e:
            print(f"Failed to write {out_path}: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert CSV schema files into JSON using pandas')
    parser.add_argument('--csv_dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--output_dir', required=False, help='Directory to write JSON schema files into')
    args = parser.parse_args()

    csv_to_json_schema(args.csv_dir, args.output_dir)
