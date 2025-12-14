import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
import os
import re

# --- CONFIGURATION ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "raw-lake"
FILE_NAME = "data_historis.csv"
TEMP_DOWNLOAD_PATH = f"/tmp/{FILE_NAME}" 

DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def normalize_to_alphanumeric(col):
    """
    EXTREME HEADER CLEANING:
    1. Removes BOM (\ufeff)
    2. Converts to lowercase
    3. REMOVES ALL non-alphanumeric characters (spaces, underscores, dots, dashes)
    """
    col = str(col).replace('\ufeff', '').lower()
    col = re.sub(r'[^a-z0-9]', '', col)
    return col

def upload_historical_data():
    print("🚀 STARTING HISTORICAL DATA INGESTION (UNIVERSAL MODE)...")
    
    # 1. SETUP MINIO
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    print(f"📡 Downloading '{FILE_NAME}'...")
    try:
        client.fget_object(BUCKET_NAME, FILE_NAME, TEMP_DOWNLOAD_PATH)
        print("✅ Download successful.")
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return

    # 2. RESET STAGING
    print("🔌 Resetting Staging Table...")
    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))

    # 3. TARGET SCHEMA
    target_schema = [
        'dr_no', 'date_rptd', 'date_occ', 'time_occ', 'area_id', 'area_name',
        'rpt_dist_no', 'part_1_2', 'crm_cd', 'crm_cd_desc', 'mocodes', 
        'vict_age', 'vict_sex', 'vict_descent', 'premis_id', 'premis_desc', 
        'weapon_id', 'weapon_desc', 'status_id', 'status_desc', 
        'crm_cd_1', 'location', 'cross_street', 'lat', 'lon'
    ]

    # 4. UNIVERSAL MAPPING
    # Supports BOTH raw headers (e.g., 'AREA') and fixed headers (e.g., 'area_id')
    csv_mapping = {
        # ID & Dates
        'drno': 'dr_no',
        'daterptd': 'date_rptd',
        'dateocc': 'date_occ',
        'timeocc': 'time_occ',
        
        # Area (Raw vs Fixed)
        'area': 'area_id',      # From "AREA"
        'areaid': 'area_id',    # From "area_id" [NEW]
        'areaname': 'area_name',
        
        # Details
        'rptdistno': 'rpt_dist_no',
        'part12': 'part_1_2',
        'part12': 'part_1_2',    # Handles "part_1-2" -> "part12"
        'crmcd': 'crm_cd',
        'crmcddesc': 'crm_cd_desc',
        'mocodes': 'mocodes',
        'victage': 'vict_age',
        'victsex': 'vict_sex',
        'victdescent': 'vict_descent',
        
        # Premise (Raw vs Fixed)
        'premiscd': 'premis_id',   # From "Premis Cd"
        'premisid': 'premis_id',   # From "premis_id" [NEW]
        'premisdesc': 'premis_desc',
        
        # Weapon (Raw vs Fixed)
        'weaponusedcd': 'weapon_id', # From "Weapon Used Cd"
        'weaponid': 'weapon_id',     # From "weapon_id" [NEW]
        'weapondesc': 'weapon_desc',
        
        # Status & Loc
        'status': 'status_id',
        'statusid': 'status_id',     # From "status_id" [NEW]
        'statusdesc': 'status_desc',
        'crmcd1': 'crm_cd_1',
        'location': 'location',
        'crossstreet': 'cross_street',
        'lat': 'lat',
        'lon': 'lon'
    }

    # 5. ETL PROCESS
    chunk_size = 50000
    total_rows = 0
    
    try:
        # Debug: Print normalized columns
        df_test = pd.read_csv(TEMP_DOWNLOAD_PATH, nrows=1)
        normalized_cols = [normalize_to_alphanumeric(c) for c in df_test.columns]
        print(f"🔍 DEBUG: CSV Headers Normalized: {normalized_cols}")
        
        with pd.read_csv(TEMP_DOWNLOAD_PATH, chunksize=chunk_size, dtype=str) as reader:
            for i, chunk in enumerate(reader):
                # A. Normalize Headers
                chunk.columns = [normalize_to_alphanumeric(c) for c in chunk.columns]
                
                # B. Rename Columns
                chunk.rename(columns=csv_mapping, inplace=True)
                
                # C. Fill Missing Target Cols
                for col in target_schema:
                    if col not in chunk.columns:
                        chunk[col] = None
                        
                # D. Select & Type Cast
                chunk = chunk[target_schema]
                for col in ['lat', 'lon', 'vict_age']:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                # E. Load
                chunk.to_sql('crime_buffer', engine, schema='staging', if_exists='append', index=False)
                total_rows += len(chunk)
                print(f"   -> Chunk {i+1} loaded. Total: {total_rows:,.0f} rows.")

        print(f"🎉 SUCCESS! {total_rows:,.0f} rows loaded to Staging.")
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)
            
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    upload_historical_data()