from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.machine_learning.train_prophet import train_and_save_model

# [FIX] Import Callback
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-scientist',
    'depends_on_past': False,
    'retries': 1,
    # [FIX] Pasang Callback
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '3_train_forecasting_model',
    default_args=default_args,
    description='Retrain Prophet Model using Data from Warehouse',
    schedule_interval='0 6 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['machine-learning', 'prophet', 'postgres']
) as dag:

    train_task = PythonOperator(
        task_id='train_prophet_model_db',
        python_callable=train_and_save_model
    )

    train_task