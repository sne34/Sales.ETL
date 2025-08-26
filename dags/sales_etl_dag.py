from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

# Default arguments
default_args = {
    "owner": "sneha",
    "start_date": datetime(2025, 8, 26),
    "retries": 1,
}

# DAG definition
with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # runs daily
    catchup=False,
) as dag:

    # Tasks
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
