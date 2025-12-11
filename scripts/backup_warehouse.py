import os
import subprocess
from datetime import datetime
from minio import Minio  # Pastikan import ini ada

def perform_backup():
    # --- 1. KONFIGURASI DATABASE ---
    PG_HOST = "warehouse"
    PG_PORT = "5432"
    PG_USER = "admin"
    PG_PASS = "admin_password"
    PG_DB = "lapd_warehouse"
    
    # --- 2. KONFIGURASI MINIO (BARU) ---
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BACKUP_BUCKET = "warehouse-backups"

    # Lokasi Sementara di Container
    BACKUP_DIR = "/opt/airflow/backups"
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"warehouse_backup_{timestamp}.sql"
    filepath = os.path.join(BACKUP_DIR, filename)

    print(f"🛡️ STARTING BACKUP... Target: {filename}")
    os.environ["PGPASSWORD"] = PG_PASS

    # --- 3. EKSEKUSI PG_DUMP (LOKAL) ---
    cmd = [
        "pg_dump",
        "-h", PG_HOST,
        "-p", PG_PORT,
        "-U", PG_USER,
        "--clean",
        "--if-exists",
        "-f", filepath,
        PG_DB
    ]

    try:
        subprocess.run(cmd, check=True)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✅ Local Backup Created ({size_mb:.2f} MB). Uploading to MinIO...")

        # --- 4. UPLOAD KE MINIO (OFFSITE) ---
        client = Minio(
            MINIO_ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False
        )

        # Buat bucket jika belum ada
        if not client.bucket_exists(BACKUP_BUCKET):
            client.make_bucket(BACKUP_BUCKET)
            print(f"   -> Bucket '{BACKUP_BUCKET}' created.")

        # Upload file
        client.fput_object(BACKUP_BUCKET, filename, filepath)
        print(f"☁️  SUCCESS! Uploaded to MinIO: {BACKUP_BUCKET}/{filename}")

        # --- 5. CLEANUP (OPSIONAL) ---
        # Hapus file lokal untuk menghemat ruang di container
        os.remove(filepath)
        print("🧹 Local backup file removed.")

    except Exception as e:
        print(f"❌ BACKUP FAILED: {e}")

if __name__ == "__main__":
    perform_backup()