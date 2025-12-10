import pandas as pd
from sqlalchemy import create_engine
import os

# --- CONFIGURATION ---
LOCAL_CSV_PATH = "data_historis.csv"
# We default to the warehouse connection string
DB_CONN = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")

def upload_historical_data():
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"❌ Error: File {LOCAL_CSV_PATH} not found.")
        return

    print("🔌 Connecting to Data Warehouse...")
    engine = create_engine(DB_CONN)

    print(f"📖 Reading {LOCAL_CSV_PATH}...")
    
    # We use chunks to handle large history files efficiently
    chunk_size = 10000
    total_rows = 0
    
    try:
        # Create an iterator to read the file in pieces
        with pd.read_csv(LOCAL_CSV_PATH, chunksize=chunk_size) as reader:
            for i, chunk in enumerate(reader):
                
                # Normalize columns to match our API schema (lowercase, no spaces)
                chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
                
                # Convert to string to ensure raw fidelity (Bronze Layer)
                chunk = chunk.astype(str)
                
                print(f"   -> Uploading chunk {i+1} ({len(chunk)} rows)...")
                
                # Append to the Bronze table
                chunk.to_sql(
                    'raw_crime', 
                    engine, 
                    schema='bronze', 
                    if_exists='append', 
                    index=False
                )
                total_rows += len(chunk)

        print(f"✅ SUCCESS! Loaded {total_rows} historical records into 'bronze.raw_crime'.")
        
    except Exception as e:
        print(f"❌ Error during upload: {e}")

if __name__ == "__main__":
    upload_historical_data()