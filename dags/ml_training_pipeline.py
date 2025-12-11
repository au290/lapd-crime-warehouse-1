from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# [CHANGE] Import the NEW XGBoost Training Function
from src.machine_learning.deploy_champion import train_and_deploy
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-scientist',
    'depends_on_past': False,
    'retries': 1,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '3_train_forecasting_model',
    default_args=default_args,
    description='Retrain XGBoost Champion Model using Data from Warehouse',
    schedule_interval='0 6 * * 1', # Run every Monday at 6 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['machine-learning', 'xgboost', 'postgres']
) as dag:

    train_task = PythonOperator(
        task_id='train_xgboost_champion',
        python_callable=train_and_deploy
    )

    train_task