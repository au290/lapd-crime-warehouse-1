import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import os
import json

def fetch_and_upload_crime_data(**kwargs):
    # 1. Database Connection
    # We use the environment variable defined in docker-compose
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    # 2. Extract from API
    print("📡 Contacting LAPD API (Socrata)...")
    client = Socrata("data.lacity.org", None) # Public access
    
    # Fetch 2000 most recent records
    results = client.get("2nrs-mtv8", limit=2000, order="date_occ DESC")
    
    if not results:
        print("⚠️ No data found.")
        return

    print(f"✅ Fetched {len(results)} rows.")

    # 3. Load to Warehouse (Bronze Layer)
    df = pd.DataFrame(results)
    
    # Convert all columns to string to ensure 'raw' fidelity (handling mixed types)
    df = df.astype(str)
    
    print("💾 Loading into 'bronze.raw_crime'...")
    
    with engine.connect() as conn:
        # We append to the raw log. 
        # In a real production setup, you might truncate this table or use a landing table.
        df.to_sql('raw_crime', engine, schema='bronze', if_exists='append', index=False)
    
    print("✅ Extract Task Completed successfully.")

if __name__ == "__main__":
    fetch_and_upload_crime_data()