from __future__ import annotations
import csv
import io
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="upstream_data_ingestion_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl", "s3"],
) as dag:
    
    # Task 1: Create a table in the Postgres database to store pipeline status.
    create_status_table = PostgresOperator(
        task_id="create_status_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pipeline_status (
                execution_date timestamp PRIMARY KEY,
                dag_id VARCHAR(250),
                status VARCHAR(50)
            );
        """
    )

    # Task 2: Generate sample data and upload it to S3.
    @task
    def upload_data_to_s3(execution_date: datetime):
        """Generates a CSV file and uploads it to S3."""
        s3_hook = S3Hook(aws_conn_id="aws_default")
        bucket_name = "your-s3-bucket"
        s3_key = f"data/sales_data_{execution_date.strftime('%Y%m%d')}.csv"
        
        # Create an in-memory file for the CSV data
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write header
        csv_writer.writerow(["id", "product", "sales"])
        # Write some dummy data
        csv_writer.writerow(["1", "Widget A", "100"])
        csv_writer.writerow(["2", "Gadget B", "250"])
        
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Uploaded {s3_key} to S3 bucket {bucket_name}")
        return s3_key
    
    upload_task = upload_data_to_s3(execution_date="{{ ds }}")

    # Task 3: Update the Postgres database with the pipeline status.
    update_pipeline_status = PostgresOperator(
        task_id="update_pipeline_status",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO pipeline_status (execution_date, dag_id, status)
            VALUES ('{{ ds }}', 'upstream_data_ingestion_dag', 'completed')
            ON CONFLICT (execution_date) DO UPDATE SET status = 'completed';
        """,
    )

    create_status_table >> upload_task >> update_pipeline_status
