from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from sqlalchemy import create_engine, text

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import Module ETL & Governance Saja (Tanpa ML)
from src.load.warehouse_loader import merge_staging_to_warehouse
from governance.quality_checks.raw_validation import validate_staging_quality
from src.utils.callbacks import send_failure_alert, send_success_alert

def trigger_ingest_script():
    import sys
    sys.path.append('/opt/airflow/scripts')
    try:
        from ingest_historical import upload_historical_data
        upload_historical_data()
    except ImportError:
        raise ImportError("Module 'ingest_historical' not found!")

def run_business_logic_check_sql(**kwargs):
    # Validasi SQL-Based (Anti OOM / Hemat RAM)
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    print("👮 Memulai pemeriksaan Business Logic (Mode SQL)...")
    
    anomalies = []
    with engine.connect() as conn:
        total = conn.execute(text("SELECT count(*) FROM staging.crime_buffer")).scalar()
        if total == 0:
            print("⚠️ Staging kosong.")
            return

        # 1. Time Travel
        bad_dates = conn.execute(text("SELECT count(*) FROM staging.crime_buffer WHERE date_rptd < date_occ")).scalar()
        if bad_dates > 0: anomalies.append(f"⚠️ Waktu Invalid: {bad_dates} kasus.")

        # 2. Umur Negatif
        bad_age = conn.execute(text("SELECT count(*) FROM staging.crime_buffer WHERE NULLIF(REGEXP_REPLACE(vict_age, '[^0-9.-]', '', 'g'), '')::FLOAT < 0")).scalar()
        if bad_age > 0: anomalies.append(f"⚠️ Umur Invalid: {bad_age} kasus.")

        # 3. Null Island
        bad_loc = conn.execute(text("SELECT count(*) FROM staging.crime_buffer WHERE NULLIF(REGEXP_REPLACE(lat, '[^0-9.-]', '', 'g'), '')::FLOAT = 0 AND NULLIF(REGEXP_REPLACE(lon, '[^0-9.-]', '', 'g'), '')::FLOAT = 0")).scalar()
        if bad_loc > 0: anomalies.append(f"⚠️ Lokasi Invalid (0,0): {bad_loc} kasus.")

    if anomalies:
        print(f"🚨 Ditemukan Anomali:\n" + "\n".join(anomalies))
    else:
        print("✅ Data Historis Valid.")

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
}

with DAG(
    '2_manual_history_processing',
    default_args=default_args,
    description='Flow: CSV -> Staging -> Check -> Warehouse', # Deskripsi diperbaiki
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['history', 'staging', 'warehouse'] # Tag ML dihapus
) as dag:

    # 1. Ingest CSV
    t1_ingest = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=trigger_ingest_script
    )

    # 2. Quality Checks (Raw)
    t2_validate = PythonOperator(
        task_id='validate_staging_raw',
        python_callable=validate_staging_quality
    )

    # 3. Quality Checks (Logic/Business)
    t3_logic = PythonOperator(
        task_id='validate_business_logic',
        python_callable=run_business_logic_check_sql
    )

    # 4. Load to Warehouse
    t4_merge = PythonOperator(
        task_id='merge_to_warehouse',
        python_callable=merge_staging_to_warehouse
    )

    # Alur: Ingest -> Validate -> Logic Check -> Load (Selesai)
    t1_ingest >> t2_validate >> t3_logic >> t4_merge