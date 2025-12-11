from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from sqlalchemy import create_engine, text

def clean_database_metadata():
    # Koneksi ke Warehouse
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🧹 Memulai Pembersihan Metadata ML yang Error...")
    
    with engine.connect() as conn:
        # Hapus Tabel (jika ada) beserta isinya
        print("   -> Dropping tables...")
        conn.execute(text("DROP TABLE IF EXISTS warehouse.model_registry CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS warehouse.model_metrics CASCADE;"))
        
        # [PENTING] Hapus Sequence yang nyangkut (Zombie Sequence)
        print("   -> Dropping orphaned sequences...")
        conn.execute(text("DROP SEQUENCE IF EXISTS warehouse.model_registry_id_seq CASCADE;"))
        conn.execute(text("DROP SEQUENCE IF EXISTS warehouse.model_metrics_id_seq CASCADE;"))
        
        print("✅ Database bersih! Siap untuk Training ulang.")

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
}

with DAG(
    '00_fix_ml_database_error',
    default_args=default_args,
    description='Jalankan SEKALI saja untuk fix error sequence ML',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance']
) as dag:

    fix_task = PythonOperator(
        task_id='nuke_bad_metadata',
        python_callable=clean_database_metadata
    )