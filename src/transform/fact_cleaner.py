import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
import json
import os

def load_lake_to_staging(**kwargs):
    # 1. Connections
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
    # 2. Get Latest File
    bucket = "raw-lake"
    try:
        objects = minio_client.list_objects(bucket)
        latest_obj = max(objects, key=lambda x: x.last_modified, default=None)
    except Exception as e:
        print(f"⚠️ Error accessing MinIO: {e}")
        return
    
    if not latest_obj:
        print("⚠️ Data Lake is empty.")
        return

    print(f"📥 Processing latest file: {latest_obj.object_name}")
    try:
        response = minio_client.get_object(bucket, latest_obj.object_name)
        file_name = latest_obj.object_name.lower()
        
        if file_name.endswith('.csv'):
            df = pd.read_csv(response)
        else:
            data = json.loads(response.read())
            df = pd.DataFrame(data)
            
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    if df.empty:
        print("⚠️ Data is empty.")
        return

    # 3. CLEANING & MAPPING FOR 7 DIMENSIONS
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Mapping exact columns from API/CSV to Staging
    # We ensure we capture descriptions for our new Dimensions
    rename_map = {
        'area': 'area_id',
        'area_name': 'area_name',
        'crm_cd': 'crm_cd',
        'crm_cd_desc': 'crm_cd_desc',
        'status': 'status_id',
        'status_desc': 'status_desc',
        'premis_cd': 'premis_id',
        'premis_desc': 'premis_desc',
        'weapon_used_cd': 'weapon_id',
        'weapon_desc': 'weapon_desc',
        'date_occ': 'date_occ',
        'time_occ': 'time_occ',
        'vict_age': 'vict_age',
        'lat': 'lat',
        'lon': 'lon'
    }
    
    # Keep only columns we need and rename them
    # Use reindex to ignore missing columns (e.g. if weapon_desc is missing)
    df = df.reindex(columns=rename_map.keys())
    df.rename(columns=rename_map, inplace=True)
    
    # Data Type Conversions
    if 'date_occ' in df.columns:
        df['date_occ'] = pd.to_datetime(df['date_occ'], errors='coerce')
        
    # Standardize IDs (Remove .0 from floats meant to be strings)
    for col in ['area_id', 'crm_cd', 'premis_id', 'weapon_id']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(str)
            df[col] = df[col].replace('0', None) # Handle nulls

    # 4. Load to Staging
    print("🚚 Loading into 'staging.crime_buffer'...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))
        
    df.to_sql('crime_buffer', engine, schema='staging', if_exists='replace', index=False)
    print(f"✅ Loaded {len(df)} rows to Staging (Ready for 7-Dim Schema).")

if __name__ == "__main__":
    load_lake_to_staging()