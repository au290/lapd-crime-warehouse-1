import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
import os

# --- KONFIGURASI ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "raw-lake"
FILE_NAME = "data_historis.csv"  # Nama file di dalam MinIO
TEMP_DOWNLOAD_PATH = f"/tmp/{FILE_NAME}" # Lokasi simpan sementara di container

DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def upload_historical_data():
    print("🚀 MEMULAI PROSES HISTORICAL DATA (SOURCE: MINIO)...")
    
    # 1. SETUP MINIO CLIENT
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    # 2. DOWNLOAD DARI MINIO KE CONTAINER (TEMPORARY)
    print(f"📡 Downloading '{FILE_NAME}' from MinIO bucket '{BUCKET_NAME}'...")
    try:
        # Cek apakah file ada
        client.stat_object(BUCKET_NAME, FILE_NAME)
        # Download ke folder /tmp di dalam container
        client.fget_object(BUCKET_NAME, FILE_NAME, TEMP_DOWNLOAD_PATH)
        print(f"✅ Download berhasil! Disimpan sementara di: {TEMP_DOWNLOAD_PATH}")
    except Exception as e:
        print(f"❌ Gagal mendownload dari MinIO: {e}")
        print("   -> Pastikan Anda sudah upload 'data_historis.csv' ke bucket 'raw-lake' di http://localhost:9001")
        return

    # 3. PERSIAPAN DATABASE
    print("🔌 Connecting to Postgres (Staging Area)...")
    engine = create_engine(DB_CONN)
    
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        print("   -> Cleaning up old staging table...")
        # Drop table dengan CASCADE agar bersih dari sisa error sebelumnya
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))

    # 4. PROSES ETL (BACA FILE TEMP -> CLEANING -> DATABASE)
    print(f"📖 Reading & Ingesting data...")
    chunk_size = 10000
    total_rows = 0
    
    try:
        # Baca dari file hasil download sementara
        with pd.read_csv(TEMP_DOWNLOAD_PATH, chunksize=chunk_size) as reader:
            for i, chunk in enumerate(reader):
                # Standardisasi Header (lowercase, spasi jadi underscore)
                chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
                
                # Rename kolom sesuai Schema Database
                rename_map = {
                    'area': 'area_id',
                    'premis_cd': 'premis_id',
                    'weapon_used_cd': 'weapon_id',
                    'status': 'status_id'
                }
                chunk.rename(columns=rename_map, inplace=True)

                # [PENTING] PEMBERSIHAN DATA NUMERIK (TECHNICAL CLEANING)
                # Mengubah string kosong "" atau teks sampah menjadi NaN (NULL di Database)
                # Ini mencegah error "invalid input syntax" saat masuk ke Warehouse
                numeric_cols = ['lat', 'lon', 'vict_age']
                for col in numeric_cols:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                
                print(f"   -> Uploading chunk {i+1} ({len(chunk)} rows)...")
                
                # Load ke DB Staging
                chunk.to_sql(
                    'crime_buffer', 
                    engine, 
                    schema='staging', 
                    if_exists='append', 
                    index=False
                )
                total_rows += len(chunk)

        print(f"✅ SUCCESS! Loaded {total_rows} historical records into 'staging.crime_buffer'.")
        
        # 5. CLEANUP (HAPUS FILE SEMENTARA)
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)
            print("🧹 Temporary file cleaned up.")
            
    except Exception as e:
        print(f"❌ Error during upload: {e}")
        # Hapus file temp jika error, supaya tidak menuhin disk
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)

if __name__ == "__main__":
    upload_historical_data()