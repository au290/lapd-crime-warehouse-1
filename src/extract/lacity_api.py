import pandas as pd
from sodapy import Socrata
from minio import Minio
from io import BytesIO
import json
import os
from datetime import datetime

def fetch_and_upload_crime_data(**kwargs):
    # 1. Config
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BUCKET_NAME = "raw-lake"

    # 2. Extract
    print("📡 Extracting data from LAPD API...")
    client = Socrata("data.lacity.org", None)
    results = client.get("2nrs-mtv8", limit=2000, order="date_occ DESC")
    
    if not results:
        print("⚠️ No data found.")
        return

    # 3. Save to Data Lake (MinIO)
    data_bytes = json.dumps(results).encode('utf-8')
    data_stream = BytesIO(data_bytes)
    
    minio_client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    # Filename includes timestamp
    filename = f"crime_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    print(f"💾 Saving {filename} to MinIO bucket '{BUCKET_NAME}'...")
    minio_client.put_object(
        BUCKET_NAME,
        filename,
        data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print("✅ Extraction to Data Lake complete!")

if __name__ == "__main__":
    fetch_and_upload_crime_data()