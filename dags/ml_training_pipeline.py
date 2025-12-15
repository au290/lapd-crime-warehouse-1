from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Imports
from src.machine_learning.deploy_champion import train_and_deploy
from src.machine_learning.train_rf import run_experiment  # [UPDATED]
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
    description='Train Comparison Models (Split Tasks to avoid OOM)',
    schedule_interval='0 6 * * 1', 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['machine-learning', 'experiment']
) as dag:

    # 1. Baseline Champion (XGB Safe)
    # This runs from the original deploy_champion.py file
    t1_champion = PythonOperator(
        task_id='1_train_champion_xgb_safe',
        python_callable=train_and_deploy
    )

    # 2. Random Forest (Safe 2023)
    t2_rf_safe = PythonOperator(
        task_id='2_train_rf_safe',
        python_callable=run_experiment,
        op_kwargs={'target_model': 'rf_safe_2023'} # Passes argument to function
    )

    # 3. Random Forest (Full/Risky)
    t3_rf_full = PythonOperator(
        task_id='3_train_rf_full',
        python_callable=run_experiment,
        op_kwargs={'target_model': 'rf_full_latest'}
    )

    # 4. XGBoost (Full/Risky)
    t4_xgb_full = PythonOperator(
        task_id='4_train_xgb_full',
        python_callable=run_experiment,
        op_kwargs={'target_model': 'xgboost_full_latest'}
    )

    # Run Sequentially to conserve RAM:
    t1_champion >> t2_rf_safe >> t3_rf_full >> t4_xgb_full