import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
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
    
    # 2. Get Latest File from Lake
    bucket = "raw-lake"
    try:
        objects = minio_client.list_objects(bucket)
        # Sort by last modified to get the newest file
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
        
        # [MODIFIKASI] Deteksi Format File (CSV vs JSON)
        file_name = latest_obj.object_name.lower()
        
        if file_name.endswith('.csv'):
            print("   -> Detected CSV format. Reading as CSV...")
            # Baca CSV langsung dari response stream
            df = pd.read_csv(response)
        else:
            print("   -> Detected JSON format. Reading as JSON...")
            data = json.loads(response.read())
            df = pd.DataFrame(data)
            
        response.close()
        response.release_conn()
        
    except Exception as e:
        print(f"❌ Error reading file content: {e}")
        return

    if df.empty:
        print("⚠️ Extracted data is empty.")
        return

    # 3. Basic Cleaning for Staging
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Rename columns to match Schema (area -> area_id)
    rename_map = {
        'area': 'area_id',
        'premis_cd': 'premis_id',
        'weapon_used_cd': 'weapon_id',
        'status': 'status_id'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Handle timestamps & IDs
    for col in df.columns:
        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 4. Load to Staging (Truncate & Load pattern)
    print("🚚 Loading into 'staging.crime_buffer'...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        # Drop table dengan CASCADE untuk membersihkan tipe data lama yang orphan
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))
        
    # Gunakan 'replace' (aman karena kita sudah drop manual sebelumnya)
    df.to_sql('crime_buffer', engine, schema='staging', if_exists='replace', index=False)
    print(f"✅ Loaded {len(df)} rows to Staging.")

if __name__ == "__main__":
    load_lake_to_staging()