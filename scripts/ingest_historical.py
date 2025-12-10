import pandas as pd
from sqlalchemy import create_engine, text
import os

LOCAL_CSV_PATH = "data_historis.csv"
DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def upload_historical_data():
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"❌ Error: File {LOCAL_CSV_PATH} not found.")
        return

    print("🔌 Connecting to Postgres (Staging Area)...")
    engine = create_engine(DB_CONN)
    
    # [FIX] Ensure staging schema exists
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))

    print(f"📖 Reading {LOCAL_CSV_PATH}...")
    chunk_size = 10000
    total_rows = 0
    
    try:
        with pd.read_csv(LOCAL_CSV_PATH, chunksize=chunk_size) as reader:
            for i, chunk in enumerate(reader):
                chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
                
                # [FIX] Rename columns to match Staging Schema
                rename_map = {
                    'area': 'area_id',
                    'premis_cd': 'premis_id',
                    'weapon_used_cd': 'weapon_id',
                    'status': 'status_id'
                }
                chunk.rename(columns=rename_map, inplace=True)
                
                print(f"   -> Uploading chunk {i+1} ({len(chunk)} rows) to 'staging.crime_buffer'...")
                
                # [FIX] Load into 'staging'
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