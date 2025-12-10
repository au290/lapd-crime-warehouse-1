import pandas as pd
from sqlalchemy import create_engine, text
import os

def clean_and_load_to_silver(**kwargs):
    # 1. Database Connection
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🔄 Reading Raw Data from Bronze Layer...")
    
    # In a real incremental load, you would filter by a 'processed' flag or date.
    # For this migration, we process the whole raw table.
    query = "SELECT * FROM bronze.raw_crime"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("⚠️ No raw data to process.")
        return

    # 2. Data Cleaning & Transformation
    # Normalize Headers
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # A. Parse Dates
    for col in df.columns:
        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # B. Numeric Conversions
    numeric_cols = ['lat', 'lon', 'vict_age']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # C. Handle Dimensions Keys (IDs)
    # Ensure IDs are strings and handle 'nan' or '100.0' formatting issues
    id_cols = ['dr_no', 'area_id', 'crm_cd', 'status_id', 'weapon_id', 'premis_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown').astype(str).str.replace(r'\.0$', '', regex=True)
            df[col] = df[col].replace({'nan': 'Unknown', 'None': 'Unknown'})

    # 3. Load to Silver Layer
    print(f"💾 Saving {len(df)} clean rows to 'silver.crime_log'...")
    
    # We use 'replace' to rebuild the Silver layer. 
    # Use 'append' if you are building an incremental pipeline.
    df.to_sql('crime_log', engine, schema='silver', if_exists='replace', index=False)
    
    print("✅ Transformation Complete!")

if __name__ == "__main__":
    clean_and_load_to_silver()