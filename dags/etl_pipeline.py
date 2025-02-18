from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# The DAG is at /opt/airflow/airflow/dags/etl_pipeline.py
# The project root is /opt/airflow
project_root = os.path.abspath("/opt/airflow")
if project_root not in sys.path:
    sys.path.append(project_root)

from src.etl.extract_mongodb import get_validated_mongodb_data
from src.etl.extract_postgresql import get_validated_postgres_data
from src.etl.transform import get_final_combined_data
from src.etl.load_s3 import initiate_uploading_final_data_to_s3

# Define default_args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "ETL_Pipeline",
    default_args=default_args,
    description="Extract, Transform and Load data to S3",
    schedule_interval="@daily",
    catchup=False,
)

# Define Tasks
extract_mongo_task = PythonOperator(
    task_id="extract_mongo",
    python_callable=get_validated_mongodb_data,
    dag=dag,
)

extract_postgres_task = PythonOperator(
    task_id="extract_postgres",
    python_callable=get_validated_postgres_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=get_final_combined_data,
    dag=dag,
)

load_s3_task = PythonOperator(
    task_id="load_to_s3",
    python_callable=initiate_uploading_final_data_to_s3,
    dag=dag,
)

# Define Dependencies
[extract_mongo_task, extract_postgres_task] >> transform_task >> load_s3_task
