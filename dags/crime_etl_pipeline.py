# dags/crime_etl_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import load_lake_to_staging

from src.load.warehouse_loader import merge_staging_to_warehouse
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '1_hybrid_etl_pipeline',
    default_args=default_args,
    description='Flow: API -> MinIO (Lake) -> Postgres (Staging) -> Postgres (Warehouse)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_to_datalake',
        python_callable=fetch_and_upload_crime_data
    )

    t2_staging = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_lake_to_staging
    )

    t3_warehouse = PythonOperator(
        task_id='merge_to_warehouse',
        python_callable=merge_staging_to_warehouse
    )

    t1_extract >> t2_staging >> t3_warehouse