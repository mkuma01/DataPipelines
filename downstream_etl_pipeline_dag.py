from __future__ import annotations
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.core.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="downstream_etl_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl", "postgres", "sensor"],
) as dag:
    
    # Task 1: Wait for a specific file to appear in the S3 bucket.
    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        aws_conn_id="aws_default",
        bucket_name="your-s3-bucket",
        bucket_key=f"data/sales_data_{{{{ ds }}}}.csv",
        timeout=600,  # Wait for up to 10 minutes
        poke_interval=60,  # Check every 60 seconds
    )

    # Task 2: Wait for the upstream DAG to finish.
    wait_for_upstream_dag = ExternalTaskSensor(
        task_id="wait_for_upstream_dag",
        external_dag_id="upstream_data_ingestion_dag",
        external_task_id=None,  # Wait for the entire DAG to succeed
        execution_delta=timedelta(hours=0), # Wait for the same execution date
        allowed_states=['success'],
        poke_interval=60,
        timeout=600,
    )

    # Task 3: Create the final destination table in Postgres.
    create_destination_table = PostgresOperator(
        task_id="create_destination_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS processed_sales (
                id INT,
                product VARCHAR(250),
                sales INT
            );
        """
    )
    
    # Task 4: Extract, transform, and load data.
    @task
    def extract_transform_load(execution_date: datetime):
        """Extracts data from S3, transforms it, and loads it into Postgres."""
        s3_hook = S3Hook(aws_conn_id="aws_default")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Extract data from S3
        bucket_name = "your-s3-bucket"
        s3_key = f"data/sales_data_{execution_date.strftime('%Y%m%d')}.csv"
        file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)

        # Simple Transformation (e.g., converting to a list of tuples)
        rows = []
        lines = file_content.strip().split('\n')[1:] # Skip header
        for line in lines:
            parts = line.split(',')
            rows.append((int(parts[0]), parts[1], int(parts[2])))

        # Load data into Postgres
        postgres_hook.insert_rows(
            table="processed_sales",
            rows=rows,
            target_fields=["id", "product", "sales"]
        )
        print("Data extracted, transformed, and loaded successfully.")

    etl_task = extract_transform_load(execution_date="{{ ds }}")

    # Define task dependencies
    [wait_for_s3_file, wait_for_upstream_dag] >> create_destination_table >> etl_task
