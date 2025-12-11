from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add src to path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# --- IMPORTS ---
# Existing ETL modules
from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import load_lake_to_staging
from src.load.warehouse_loader import merge_staging_to_warehouse
# NEW Training module
from src.machine_learning.daily_retrain import train_daily_model
# Callbacks
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '5_daily_end_to_end_system',
    default_args=default_args,
    description='Full Pipeline: API -> Lake -> Staging -> Warehouse -> Retrain Model',
    schedule_interval='0 9 * * *', # Runs daily at 9:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'end-to-end']
) as dag:

    # 1. EXTRACT (API to MinIO)
    t1_extract = PythonOperator(
        task_id='1_extract_from_api',
        python_callable=fetch_and_upload_crime_data
    )

    # 2. TRANSFORM (MinIO to Postgres Staging)
    t2_staging = PythonOperator(
        task_id='2_load_to_staging',
        python_callable=load_lake_to_staging
    )

    # 3. LOAD (Staging to Warehouse Fact/Dim)
    t3_warehouse = PythonOperator(
        task_id='3_merge_to_warehouse',
        python_callable=merge_staging_to_warehouse
    )

    # 4. TRAIN (Retrain XGBoost using new daily script)
    t4_train = PythonOperator(
        task_id='4_daily_model_retrain',
        python_callable=train_daily_model
    )

    # --- PIPELINE FLOW ---
    t1_extract >> t2_staging >> t3_warehouse >> t4_train