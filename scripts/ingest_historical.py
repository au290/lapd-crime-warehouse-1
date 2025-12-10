import pandas as pd
from sqlalchemy import create_engine, text
import os

# [FIX] Gunakan path absolut agar file terbaca di dalam Docker
# Pastikan file 'data_historis.csv' Anda simpan di folder 'scripts/' laptop Anda
LOCAL_CSV_PATH = "/opt/airflow/scripts/data_historis.csv"
DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def upload_historical_data():
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"❌ Error: File {LOCAL_CSV_PATH} not found.")
        print("   -> Pastikan file csv ada di folder 'scripts/' dan volume docker sudah dimount.")
        return

    print("🔌 Connecting to Postgres (Staging Area)...")
    engine = create_engine(DB_CONN)
    
    # [FIX] Reset Staging Table (Agar tidak duplikat/error tipe data)
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        print("   -> Cleaning up old staging table...")
        # Drop table dengan CASCADE untuk membersihkan metadata lama
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))

    print(f"📖 Reading {LOCAL_CSV_PATH}...")
    chunk_size = 10000
    total_rows = 0
    
    try:
        with pd.read_csv(LOCAL_CSV_PATH, chunksize=chunk_size) as reader:
            for i, chunk in enumerate(reader):
                chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
                
                # Rename columns sesuai Schema
                rename_map = {
                    'area': 'area_id',
                    'premis_cd': 'premis_id',
                    'weapon_used_cd': 'weapon_id',
                    'status': 'status_id'
                }
                chunk.rename(columns=rename_map, inplace=True)
                
                print(f"   -> Uploading chunk {i+1} ({len(chunk)} rows) to 'staging.crime_buffer'...")
                
                # [FIX] Gunakan 'append' karena kita sudah DROP di awal
                chunk.to_sql(
                    'crime_buffer', 
                    engine, 
                    schema='staging', 
                    if_exists='append', 
                    index=False
                )
                total_rows += len(chunk)

        print(f"✅ SUCCESS! Loaded {total_rows} historical records into 'staging.crime_buffer'.")
        
    except Exception as e:
        print(f"❌ Error during upload: {e}")

if __name__ == "__main__":
    upload_historical_data()