import os
import subprocess
from datetime import datetime

def perform_backup():
    # --- CONFIGURATION ---
    # Credentials (extracted from connection string for pg_dump)
    PG_HOST = "warehouse"
    PG_PORT = "5432"
    PG_USER = "admin"
    PG_PASS = "admin_password" # In production, use .pgpass file
    PG_DB = "lapd_warehouse"
    
    # Backup Location
    BACKUP_DIR = "/opt/airflow/backups"
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"warehouse_backup_{timestamp}.sql"
    filepath = os.path.join(BACKUP_DIR, filename)

    print(f"🛡️ STARTING WAREHOUSE BACKUP: {timestamp}")
    print(f"   Target: {filepath}")

    # Set PGPASSWORD env var so pg_dump doesn't ask for password
    os.environ["PGPASSWORD"] = PG_PASS

    # Construct pg_dump command
    # -h: Host, -p: Port, -U: User, -d: Database, -f: Output File
    # --clean: Include DROP commands (good for full restore)
    # --if-exists: Prevent errors if dropping missing tables
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
        # Run the command
        subprocess.run(cmd, check=True)
        
        file_size = os.path.getsize(filepath) / (1024 * 1024) # Size in MB
        print(f"✅ BACKUP SUCCESS! Saved to {filepath} ({file_size:.2f} MB)")
        
        # Cleanup: Keep only last 7 backups (Optional)
        # ... logic to delete old files ...
        
    except subprocess.CalledProcessError as e:
        print(f"❌ BACKUP FAILED: {e}")
    except FileNotFoundError:
        print("❌ Error: 'pg_dump' tool not found. Please install 'postgresql-client' in your Dockerfile.")

if __name__ == "__main__":
    perform_backup()