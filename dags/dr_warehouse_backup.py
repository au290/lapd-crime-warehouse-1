from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# [FIX] Import Callback
from src.utils.callbacks import send_failure_alert, send_success_alert

def trigger_backup_wrapper():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from backup_warehouse import perform_backup
    perform_backup()

default_args = {
    'owner': 'infrastructure',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # [FIX] Pasang Callback di sini
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert 
}

with DAG(
    '99_disaster_recovery_backup',
    default_args=default_args,
    description='Performs pg_dump of the Warehouse Database',
    schedule_interval='0 23 * * *', 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['disaster-recovery', 'postgres']
) as dag:

    backup_task = PythonOperator(
        task_id='execute_db_backup',
        python_callable=trigger_backup_wrapper
    )

    backup_task