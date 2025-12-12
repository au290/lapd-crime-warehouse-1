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
    print("🚀 MEMULAI PROSES HISTORICAL DATA (BULLETPROOF MODE)...")
    
    # 1. SETUP MINIO & DOWNLOAD
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    print(f"📡 Downloading '{FILE_NAME}' from MinIO...")
    try:
        client.stat_object(BUCKET_NAME, FILE_NAME)
        client.fget_object(BUCKET_NAME, FILE_NAME, TEMP_DOWNLOAD_PATH)
        print(f"✅ Download berhasil: {TEMP_DOWNLOAD_PATH}")
    except Exception as e:
        print(f"❌ Gagal download: {e}")
        return

    # 2. PERSIAPAN DATABASE
    print("🔌 Resetting Staging Table...")
    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("DROP TABLE IF EXISTS staging.crime_buffer CASCADE;"))

    # 3. DEFINISI SCHEMA TARGET (Wajib Ada)
    # Kolom ini HARUS masuk ke DB. Jika di CSV tidak ada, kita isi NULL.
    target_schema = [
        'dr_no', 'date_rptd', 'date_occ', 'time_occ', 'area_id', 'area_name',
        'rpt_dist_no', 'part_1_2', 'crm_cd', 'crm_cd_desc', 'mocodes', 
        'vict_age', 'vict_sex', 'vict_descent', 'premis_id', 'premis_desc', 
        'weapon_id', 'weapon_desc', 'status_id', 'status_desc', 
        'crm_cd_1', 'location', 'cross_street', 'lat', 'lon'
    ]

    # Mapping Header CSV (Huruf Kecil) -> Target Schema
    # Sesuaikan variasi nama kolom disini
    csv_mapping = {
        'dr_no': 'dr_no',
        'date rptd': 'date_rptd', 'date_rptd': 'date_rptd',
        'date occ': 'date_occ', 'date_occ': 'date_occ',
        'time occ': 'time_occ', 'time_occ': 'time_occ',
        'area': 'area_id', 'area ': 'area_id',
        'area name': 'area_name', 'area_name': 'area_name', 
        'crm cd': 'crm_cd', 'crm_cd': 'crm_cd',
        'crm cd desc': 'crm_cd_desc', 'crm_cd_desc': 'crm_cd_desc',
        'premis cd': 'premis_id', 'premis_cd': 'premis_id',
        'premis desc': 'premis_desc', 'premis_desc': 'premis_desc',
        'weapon used cd': 'weapon_id', 'weapon_used_cd': 'weapon_id',
        'weapon desc': 'weapon_desc', 'weapon_desc': 'weapon_desc',
        'status': 'status_id', 
        'status desc': 'status_desc', 'status_desc': 'status_desc',
        'vict age': 'vict_age', 'vict_age': 'vict_age',
        'lat': 'lat', 'lon': 'lon'
    }

    # 4. PROSES ETL
    chunk_size = 50000
    total_rows = 0
    
    try:
        with pd.read_csv(TEMP_DOWNLOAD_PATH, chunksize=chunk_size, dtype=str) as reader:
            for i, chunk in enumerate(reader):
                try:
                    # A. Normalisasi Header CSV
                    chunk.columns = [c.strip().lower() for c in chunk.columns]
                    
                    # B. Rename Kolom yang Dikenali
                    chunk.rename(columns=csv_mapping, inplace=True)
                    
                    # C. [FIX UTAMA] Pastikan Semua Kolom Target Ada
                    for col in target_schema:
                        if col not in chunk.columns:
                            # Jika kolom target tidak ada di CSV, buat kolom baru isinya None
                            chunk[col] = None
                            
                    # D. Filter Hanya Kolom Target (Urutan Sesuai Schema)
                    chunk = chunk[target_schema]

                    # E. Cleaning Numerik
                    for col in ['lat', 'lon', 'vict_age']:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                    # F. Load ke Staging
                    chunk.to_sql('crime_buffer', engine, schema='staging', if_exists='append', index=False)
                    
                    total_rows += len(chunk)
                    print(f"   -> Chunk {i+1} loaded. Total: {total_rows:,.0f} rows.")
                    
                except Exception as e:
                    print(f"   ❌ Error Chunk {i+1}: {e}")
                    continue

        print(f"🎉 SELESAI! {total_rows:,.0f} baris dimuat ke Staging.")
        
        if os.path.exists(TEMP_DOWNLOAD_PATH):
            os.remove(TEMP_DOWNLOAD_PATH)
            
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    upload_historical_data()