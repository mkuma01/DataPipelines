import csv
import json
import os
import argparse

def csv_to_json_schema(csv_file_path:str)->str:
    
    # Open with newline='' and explicit encoding so csv module handles newlines and quoting correctly.
    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csv_file:
        # Configure DictReader to handle quoted fields and common escape sequences.
        # If your CSV uses backslash to escape delimiters (e.g. "some\,text"),
        # setting escapechar='\\' and quoting=csv.QUOTE_NONE can be used, but
        # most well-formed CSVs will have quoted fields (quotechar '"') and
        # the default QUOTE_MINIMAL works fine. Here we set escapechar so that
        # any backslash-escaped commas within unquoted fields get unescaped.
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"', escapechar='\\')
        if csv_reader.fieldnames:
            csv_reader.fieldnames = [fn.strip() for fn in csv_reader.fieldnames]
        json_schema = []
        for row in csv_reader:
            # Skip empty rows
            if not row:
                continue

            # Extract fields safely and sanitize description
            field_name = (row.get("name", "") or "").strip()
            field_type = (row.get("type", "") or "").strip()
            mode = (row.get("mode", "") or "").strip()
            description = (row.get("description", "") or "")

            # If the CSV has backslash-escaped commas (like "some\,text"),
            # csv.DictReader may return the literal backslash in the string.
            # Replace the escaped sequence '\,' with a plain comma.
            description = description.replace('\\,', ',').strip()

            # Skip rows missing key information
            if not field_name and not field_type:
                continue

            field_schema = {
                "name": field_name,
                "type": field_type,
                "mode": mode,
                "description": description
            }
            json_schema.append(field_schema)
        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        output_file_path = os.path.join(os.path.dirname(csv_file_path),f"{base_name}_schema.json")
        with open(output_file_path, mode='w') as json_file:
            json.dump(json_schema, json_file, indent=2)

        print(f"JSON schema file has been written to {output_file_path}")
        #return json_schema

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CSV schema to JSON schema.')
    parser.add_argument('--csv_file_path',required=True, type=str, help='Path to the input CSV file.')
    args = parser.parse_args()

    for filename in os.listdir(args.csv_file_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(args.csv_file_path, filename)
            csv_to_json_schema(file_path)
    
