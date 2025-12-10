from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# --- Updated Imports for Warehouse Architecture ---
from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area
# We assume you will rename/update the governance check to validate DB tables
from governance.quality_checks.raw_validation import validate_raw_json_structure as validate_bronze_quality 
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '1_ingest_lapd_crime_data',
    default_args=default_args,
    description='Warehouse Pipeline: API -> Bronze -> Silver -> Gold (Postgres)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lapd', 'warehouse', 'postgres']
) as dag:

    # Task 1: Extract (API -> Postgres Bronze Schema)
    ingest_task = PythonOperator(
        task_id='1_extract_to_bronze',
        python_callable=fetch_and_upload_crime_data
    )

    # Task 2: Validate (Check Row Counts / Nulls in Bronze Table)
    validate_task = PythonOperator(
        task_id='2_validate_bronze_quality',
        python_callable=validate_bronze_quality
    )

    # Task 3: Transform (Bronze Table -> Silver Table)
    transform_task = PythonOperator(
        task_id='3_transform_to_silver',
        python_callable=clean_and_load_to_silver
    )

    # Task 4: Aggregate (Silver Table -> Gold Star Schema)
    aggregate_task = PythonOperator(
        task_id='4_build_gold_schema',
        python_callable=aggregate_crime_by_area
    )

    ingest_task >> validate_task >> transform_task >> aggregate_task