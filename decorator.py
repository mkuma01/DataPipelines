from __future__ import annotations

import json
import pendulum

from airflow.sdk import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    tags=["etl_example"],
)
def simple_etl_pipeline():
    """
    This DAG demonstrates a simple ETL pipeline using Airflow decorators.
    It extracts data, transforms it, and then loads it.
    """

    @task()
    def extract_data():
        """
        Extract task: Simulates fetching data from a source.
        Returns a dictionary of raw data.
        """
        raw_data = {
            "product_a": 100,
            "product_b": 150,
            "product_c": 75,
        }
        print(f"Extracted raw data: {raw_data}")
        return raw_data

    @task()
    def transform_data(data: dict):
        """
        Transform task: Simulates data cleaning and enrichment.
        Calculates the total value and adds a processing timestamp.
        """
        total_value = sum(data.values())
        transformed_data = {
            "processed_timestamp": pendulum.now().isoformat(),
            "total_sales_value": total_value,
            "original_products": data,
        }
        print(f"Transformed data: {transformed_data}")
        return transformed_data

    @task()
    def load_data(data: dict):
        """
        Load task: Simulates loading transformed data into a destination.
        Prints the data to the logs for demonstration.
        """
        print(f"Loading data to destination: {json.dumps(data, indent=2)}")
        # In a real ETL, this would involve writing to a database, data lake, etc.

    # Define the task dependencies using Pythonic function calls
    extracted_info = extract_data()
    transformed_info = transform_data(extracted_info)
    load_data(transformed_info)

# Instantiate the DAG
simple_etl_pipeline()