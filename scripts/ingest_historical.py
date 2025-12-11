import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
import os

# --- KONFIGURASI ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "raw-lake"
FILE_NAME = "data_historis.csv" 
TEMP_DOWNLOAD_PATH = f"/tmp/{FILE_NAME}" 

DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def upload_historical_data():
    print("🚀 MEMULAI PROSES HISTORICAL DATA (SAFE MODE)...")
    
    # 1. SETUP MINIO CLIENT
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    # 2. DOWNLOAD DARI MINIO
    print(f"📡 Downloading '{FILE_NAME}' from MinIO...")
    try:
        client.stat_object(BUCKET_NAME, FILE_NAME)
        client.fget_object(BUCKET_NAME, FILE_NAME, TEMP_DOWNLOAD_PATH)
        print(f"✅ Download berhasil! Lokasi: {TEMP_DOWNLOAD_PATH}")
    except Exception as e:
        print(f"❌ Gagal download: {e}")
        return

    # 3. PERSIAPAN DATABASE
    print("🔌 Resetting Staging Table...")
    engine = create_engine(DB_CONN)
    
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))

    # 4. PROSES ETL
    chunk_size = 50000
    total_rows = 0
    error_chunks = 0
    
    # Mapping dengan Key HURUF KECIL (Lowercase) agar aman
    rename_map = {
        'dr_no': 'dr_no',
        'date rptd': 'date_rptd',
        'date occ': 'date_occ',
        'time occ': 'time_occ',
        'area': 'area_id',          # <-- Target Mapping yang penting
        'area name': 'area_name',
        'rpt dist no': 'rpt_dist_no',
        'part 1-2': 'part_1_2',
        'crm cd': 'crm_cd',
        'crm cd desc': 'crm_cd_desc',
        'mocodes': 'mocodes',
        'vict age': 'vict_age',
        'vict sex': 'vict_sex',
        'vict descent': 'vict_descent',
        'premis cd': 'premis_id',
        'premis desc': 'premis_desc',
        'weapon used cd': 'weapon_id',
        'weapon desc': 'weapon_desc',
        'status': 'status_id',
        'status desc': 'status_desc',
        'crm cd 1': 'crm_cd_1',
        'crm cd 2': 'crm_cd_2',
        'crm cd 3': 'crm_cd_3',
        'crm cd 4': 'crm_cd_4',
        'location': 'location',
        'cross street': 'cross_street',
        'lat': 'lat',
        'lon': 'lon'
    }

    print(f"📖 Reading & Ingesting CSV...")
    
    try:
        # Gunakan iterator
        with pd.read_csv(TEMP_DOWNLOAD_PATH, chunksize=chunk_size, dtype=str) as reader:
            for i, chunk in enumerate(reader):
                try:
                    # [FIX UTAMA] Normalisasi Header ke Lowercase
                    chunk.columns = [c.strip().lower() for c in chunk.columns]
                    
                    # Filter & Rename
                    available_cols = [c for c in chunk.columns if c in rename_map]
                    chunk = chunk[available_cols].copy()
                    chunk.rename(columns=rename_map, inplace=True)

                    # Pastikan kolom vital ada
                    if 'area_id' not in chunk.columns:
                        print(f"   ⚠️ Warning: Chunk {i+1} kehilangan kolom 'area_id'. Cek header CSV!")

                    # Cleaning Numerik (Lat/Lon/Age)
                    for col in ['lat', 'lon', 'vict_age']:
                        if col in chunk.columns:
                            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                    # Load ke Staging
                    chunk.to_sql(
                        'crime_buffer', 
                        engine, 
                        schema='staging', 
                        if_exists='append', 
                        index=False
                    )
                    
                    total_rows += len(chunk)
                    print(f"   -> Chunk {i+1} loaded. Total: {total_rows:,.0f} rows.")
                    
                except Exception as e:
                    print(f"   ❌ Error on Chunk {i+1}: {e}")
                    error_chunks += 1
                    continue

        print(f"🎉 SELESAI! Berhasil memuat {total_rows:,.0f} baris.")
        
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)
            
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)

if __name__ == "__main__":
    upload_historical_data()