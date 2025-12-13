import csv
from logging import info

# 1. Extraction (E) - Generator to read data from a large CSV file
def extract_data_from_csv(filepath):
    """
    Yields each row from a CSV file as a dictionary.
    """
    with open(filepath, 'r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

# 2. Transformation (T) - Generator to transform the extracted data
def transform_user_data(data_rows):
    """
    Transforms user data by converting age to integer and creating a full name.
    """
    for row in data_rows:
        try:
            row['age'] = int(row['age'])
            row['full_name'] = f"{row['first_name']} {row['last_name']}"
            yield row
        except (ValueError, KeyError) as e:
            info(f"Skipping row due to error: {row} - {e}")
            continue

# 3. Loading (L) - Function to load the transformed data (could also be a generator for streaming to another system)
def load_data_to_console(transformed_data):
    """
    Prints each transformed data row to the console.
    """
    info("Loading transformed data:")
    for record in transformed_data:
        print(record)

# Example Usage
if __name__ == "__main__":
    # Create a dummy CSV file for demonstration
    with open('users.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'last_name', 'age', 'city'])
        writer.writerow(['Alice', 'Smith', '30', 'New York'])
        writer.writerow(['Bob', 'Johnson', '24', 'London'])
        writer.writerow(['Charlie', 'Brown', 'invalid_age', 'Paris']) # Invalid data
        writer.writerow(['Diana', 'Prince', '28', 'Themyscira'])

    # Build the ETL pipeline using generators
    extracted_records = extract_data_from_csv('users.csv')
    transformed_records = transform_user_data(extracted_records)
    load_data_to_console(transformed_records)