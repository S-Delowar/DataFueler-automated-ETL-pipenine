from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os

import pandas as pd

# The DAG is at /opt/airflow/airflow/dags/etl_pipeline.py
# The project root is /opt/airflow
project_root = os.path.abspath("/opt/airflow")
if project_root not in sys.path:
    sys.path.append(project_root)

from src.etl.extract_mongodb import get_validated_mongodb_data
from src.etl.extract_postgresql import get_validated_postgres_data
from src.etl.transform import get_final_combined_data
from src.etl.load_s3 import upload_dataframe_to_s3

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



with DAG(
    "etl_pipeline_with_tasks",
    default_args=default_args,
    description="ETL pipeline that extracts from MongoDB and PostgreSQL, transforms, and loads to S3 using @task",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    @task
    def extract_mongodb():
        """
        Extract validated MongoDB data.
        Returns the data as a JSON string.
        """
        mongo_df = get_validated_mongodb_data()
        # Convert DataFrame to JSON for XCom passing
        return mongo_df.to_json()

    @task
    def extract_postgres():
        """
        Extract validated PostgreSQL data.
        Returns the data as a JSON string.
        """
        postgres_df = get_validated_postgres_data()
        return postgres_df.to_json()

    @task
    def transform_data(mongo_json: str, postgres_json: str):
        """
        Transform the data by combining MongoDB and PostgreSQL datasets.
        Returns the final combined data as a JSON string.
        """
        # Convert JSON back to DataFrames
        mongo_df = pd.read_json(mongo_json)
        postgres_df = pd.read_json(postgres_json)
        final_df = get_final_combined_data(mongo_df, postgres_df)
        
        final_df = final_df.reset_index(drop=True)
        return final_df.to_json()

    @task
    def load_to_s3(final_json: str):
        """
        Load the transformed data into an S3 bucket.
        """
        bucket_name = os.getenv("S3_BUCKET_NAME")
        s3_key = "Processed_Data/fuel_consumption_data.csv"
        final_df = pd.read_json(final_json)
        upload_dataframe_to_s3(final_df, bucket_name, s3_key)

    # Define task dependencies using the bitshift operators
    mongo_json = extract_mongodb()
    postgres_json = extract_postgres()
    final_json = transform_data(mongo_json, postgres_json)
    load_to_s3(final_json)





# # Define DAG
# dag = DAG(
#     "ETL_Pipeline",
#     default_args=default_args,
#     description="Extract, Transform and Load data to S3",
#     schedule_interval="@daily",
#     catchup=False,
# )

# # Define Tasks
# extract_mongo_task = PythonOperator(
#     task_id="extract_mongo",
#     python_callable=get_validated_mongodb_data,
#     dag=dag,
# )

# extract_postgres_task = PythonOperator(
#     task_id="extract_postgres",
#     python_callable=get_validated_postgres_data,
#     dag=dag,
# )

# transform_task = PythonOperator(
#     task_id="transform_data",
#     python_callable=get_final_combined_data,
#     dag=dag,
# )

# load_s3_task = PythonOperator(
#     task_id="load_to_s3",
#     python_callable=initiate_uploading_final_data_to_s3,
#     dag=dag,
# )

# # Define Dependencies
# [extract_mongo_task, extract_postgres_task] >> transform_task >> load_s3_task
