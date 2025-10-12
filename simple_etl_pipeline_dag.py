from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define functions for ETL steps
def extract_data(**kwargs):
    """
    Simulates extracting data from a source.
    In a real scenario, this would involve connecting to a database, API, or file system.
    """
    print("Extracting data...")
    sample_data = [
        {"id": 1, "name": "Alice", "value": 10},
        {"id": 2, "name": "Bob", "value": 20},
        {"id": 3, "name": "Charlie", "value": 30},
    ]
    kwargs['ti'].xcom_push(key='extracted_data', value=sample_data)
    print("Data extracted.")

def transform_data(**kwargs):
    """
    Simulates transforming the extracted data.
    This could involve cleaning, aggregating, or enriching the data.
    """
    print("Transforming data...")
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    transformed_data = []
    for row in extracted_data:
        row['value_doubled'] = row['value'] * 2
        transformed_data.append(row)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    print("Data transformed.")

def load_data(**kwargs):
    """
    Simulates loading the transformed data into a destination.
    This could be a data warehouse, another database, or a file.
    """
    print("Loading data...")
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    for row in transformed_data:
        print(f"Loading row: {row}")
    print("Data loaded.")

# Define the DAG
with DAG(
    dag_id='simple_etl_pipeline_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'sample'],
) as dag:
    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task