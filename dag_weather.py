import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from dags.py_scripts.callables_weather import fetch_data, upload_to_minio, transform_duckdb, load_postgres

with DAG(
    dag_id="weather_pipeline",
    description="download data from API, save raw data to S3 bucket, transform data and load data in postgres",
    start_date=datetime.datetime.now() - datetime.timedelta(minutes=1),
    schedule=datetime.timedelta(minutes=5),
    default_args={
        "owner": "Michael",
        "retries": 4,
        "retry_delay": datetime.timedelta(seconds=30),
    },
) as dag:

    create_folder = BashOperator(
        task_id="create_folder",
        bash_command="mkdir -p /home/$(whoami)/lezioni_airflow/raw_data",
    )

    fetch_data_task = PythonOperator(task_id="fetch_data", python_callable=fetch_data)


    upload_s3 = PythonOperator(task_id="upload_to_minio", python_callable=upload_to_minio)

    transform = PythonOperator(task_id = "transform_duckdb", python_callable= transform_duckdb)

    load = PythonOperator(task_id="load_to_postgres", python_callable=load_postgres)

    create_folder >> fetch_data_task >> upload_s3 >> transform >> load


    