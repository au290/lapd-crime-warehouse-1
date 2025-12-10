from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# [FIX] Import new modules
from src.transform.gold_aggregator import merge_staging_to_warehouse
from governance.quality_checks.raw_validation import validate_staging_quality
from src.utils.callbacks import send_failure_alert, send_success_alert

def trigger_ingest_script():
    # Helper to run the ingest script
    import sys
    sys.path.append('/opt/airflow/scripts')
    from ingest_historical import upload_historical_data
    upload_historical_data()

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '2_manual_history_processing',
    default_args=default_args,
    description='Flow: CSV -> Postgres (Staging) -> Postgres (Warehouse)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['history', 'staging', 'warehouse']
) as dag:

    # Task 1: Load CSV to Staging
    t1_ingest = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=trigger_ingest_script
    )

    # Task 2: Validate Staging
    t2_validate = PythonOperator(
        task_id='validate_staging_data',
        python_callable=validate_staging_quality
    )

    # Task 3: Merge Staging to Warehouse
    t3_merge = PythonOperator(
        task_id='merge_to_warehouse',
        python_callable=merge_staging_to_warehouse
    )

    t1_ingest >> t2_validate >> t3_merge