import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from minio import Minio
from sklearn.cluster import KMeans, MiniBatchKMeans
import os
import re

# --- CONFIGURATION ---
THESIS_CUTOFF_DATE = '2023-12-31'
K_CLUSTERS = 100
BUCKET_NAME = "raw-lake"
TARGET_FILE = "data_historis.csv" # Priorities this file
TEMP_PATH = f"/tmp/{TARGET_FILE}"

def normalize_headers(df):
    """Normalize headers to match the JSON/Database format (snake_case)"""
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
    return df

def load_and_clean_thesis_data(**kwargs):
    print("🎓 STARTING THESIS-SPECIFIC ETL (Historical Mode)...")
    
    # 1. Connections
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
    # 2. LOCATE CORRECT FILE
    # We prioritize 'data_historis.csv' because it has the 2020-2023 data needed.
    file_to_process = None
    try:
        # Check if historical file exists
        minio_client.stat_object(BUCKET_NAME, TARGET_FILE)
        file_to_process = TARGET_FILE
        print(f"✅ Found Historical Data: {TARGET_FILE}")
    except:
        # Fallback to latest (likely API json) if historical missing
        print(f"⚠️ '{TARGET_FILE}' not found. Searching for latest file...")
        try:
            objects = minio_client.list_objects(BUCKET_NAME)
            latest_obj = max(objects, key=lambda x: x.last_modified, default=None)
            if latest_obj:
                file_to_process = latest_obj.object_name
        except Exception as e:
            print(f"❌ MinIO Error: {e}")
            return

    if not file_to_process:
        print("❌ No data found in Data Lake.")
        return

    # 3. DOWNLOAD TO TEMP (Prevents OOM)
    print(f"⬇️ Downloading '{file_to_process}' to {TEMP_PATH}...")
    try:
        minio_client.fget_object(BUCKET_NAME, file_to_process, TEMP_PATH)
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return

    # 4. LOAD DATA (Smart Loader)
    print("📖 Reading data into Pandas...")
    try:
        if file_to_process.endswith('.json'):
            df = pd.read_json(TEMP_PATH)
        else:
            # Low memory mode for large CSVs
            df = pd.read_csv(TEMP_PATH, low_memory=False)
    except Exception as e:
        print(f"❌ Read failed: {e}")
        return

    # Normalize Headers
    df = normalize_headers(df)
    print(f"   -> Raw Rows: {len(df)}")

    # 5. MAPPING & CLEANING
    # Map varied CSV headers to our Schema
    rename_map = {
        'dr_no': 'dr_no', 'drno': 'dr_no',
        'date_occ': 'date_occ', 'dateocc': 'date_occ',
        'time_occ': 'time_occ', 'timeocc': 'time_occ',
        'area': 'area_id', 'area_name': 'area_name',
        'crm_cd': 'crm_cd', 'crm_cd_desc': 'crm_cd_desc',
        'vict_age': 'vict_age', 'victage': 'vict_age',
        'vict_sex': 'vict_sex', 'victsex': 'vict_sex',
        'vict_descent': 'vict_descent', 'victdescent': 'vict_descent',
        'lat': 'lat', 'lon': 'lon',
        'premis_cd': 'premis_id', 'premis_desc': 'premis_desc',
        'weapon_used_cd': 'weapon_id', 'weapon_desc': 'weapon_desc',
        'status': 'status_id', 'status_desc': 'status_desc'
    }
    df = df.rename(columns=rename_map)
    
    # Ensure columns exist
    required_cols = ['dr_no', 'date_occ', 'lat', 'lon', 'vict_age']
    for col in required_cols:
        if col not in df.columns:
            print(f"⚠️ Missing column '{col}'. Filling with defaults.")
            df[col] = np.nan

    # Type Conversion
    df['date_occ'] = pd.to_datetime(df['date_occ'], errors='coerce')
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce').fillna(0)
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce').fillna(0)
    df['vict_age'] = pd.to_numeric(df['vict_age'], errors='coerce').fillna(-1)

    # 6. APPLY THESIS RULES [Strict]
    print(f"✂️ Applying Thesis Cut-off: {THESIS_CUTOFF_DATE}")
    df = df[df['date_occ'] <= THESIS_CUTOFF_DATE]
    
    print("📍 Removing Null Island (0,0)...")
    df = df[(df['lat'] != 0) & (df['lon'] != 0)]
    
    print("🎂 Filtering Age > 0...")
    df = df[df['vict_age'] > 0]
    
    print("🧢 Capping Age at 80...")
    df['vict_age'] = df['vict_age'].apply(lambda x: 80 if x > 80 else x)

    if 'vict_sex' in df.columns:
        valid_sex = ['M', 'F', 'X']
        df = df[df['vict_sex'].isin(valid_sex)]

    print(f"   -> Valid Thesis Rows: {len(df)}")

    if df.empty:
        print("❌ Dataset is empty after cleaning! Check if 'data_historis.csv' is uploaded to MinIO.")
        return

    # 7. FEATURE ENGINEERING (Clustering)
    print(f"🧩 Generating Spatial Clusters (K={K_CLUSTERS})...")
    # Using MiniBatchKMeans for speed/memory efficiency on large data
    kmeans = MiniBatchKMeans(n_clusters=K_CLUSTERS, random_state=42, batch_size=1024)
    df['crime_cluster'] = kmeans.fit_predict(df[['lat', 'lon']])

    # Hour Extraction
    def parse_hour(x):
        try:
            return int(str(int(float(x))).zfill(4)[:2])
        except:
            return 0
    df['hour'] = df['time_occ'].apply(parse_hour)

    # 8. LOAD TO STAGING
    target_table = 'thesis_crime_buffer'
    print(f"🚚 Loading {len(df)} rows into 'staging.{target_table}'...")
    
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text(f"DROP TABLE IF EXISTS staging.{target_table} CASCADE;"))
        
    # Chunked Load to SQL
    chunk_size = 50000
    df.to_sql(target_table, engine, schema='staging', if_exists='replace', index=False, chunksize=chunk_size)
    
    print("✅ Thesis ETL Complete!")
    
    # Cleanup
    if os.path.exists(TEMP_PATH):
        os.remove(TEMP_PATH)

if __name__ == "__main__":
    load_and_clean_thesis_data()