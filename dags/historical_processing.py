from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area
from governance.quality_checks.raw_validation import validate_raw_json_structure as validate_bronze_quality
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '2_manual_history_processing',
    default_args=default_args,
    description='Pipeline for Historical CSV -> Postgres',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['history', 'manual', 'postgres']
) as dag:

    # Task 1: Validation (Check the historical table in Bronze)
    validate_task = PythonOperator(
        task_id='validate_historical_data',
        python_callable=validate_bronze_quality
    )

    # Task 2: Transform (Bronze -> Silver)
    # The function clean_and_load_to_silver now reads from DB directly
    transform_task = PythonOperator(
        task_id='process_historical_bronze',
        python_callable=clean_and_load_to_silver
    )

    # Task 3: Aggregate (Silver -> Gold)
    aggregate_task = PythonOperator(
        task_id='aggregate_history_gold',
        python_callable=aggregate_crime_by_area
    )

    validate_task >> transform_task >> aggregate_task