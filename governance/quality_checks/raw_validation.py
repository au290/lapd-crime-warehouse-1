from sqlalchemy import create_engine, text
import os

def validate_bronze_quality(**kwargs):
    # 1. Configuration
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🔍 [RAW CHECK] Verifying Bronze Layer Quality in Warehouse...")
    
    with engine.connect() as conn:
        # 2. Check if Table Exists
        # We query the system catalog to verify the table structure exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'bronze'
                AND    table_name   = 'raw_crime'
            );
        """)).scalar()

        if not table_exists:
            raise ValueError("⛔ CRITICAL: Table 'bronze.raw_crime' NOT FOUND. Extraction likely failed.")

        # 3. Check Row Count
        # We need to ensure we actually ingested data
        row_count = conn.execute(text("SELECT count(*) FROM bronze.raw_crime")).scalar()
        
        if row_count == 0:
            raise ValueError("⛔ CRITICAL: Table 'bronze.raw_crime' is EMPTY (0 records). Pipeline STOP.")
        
        # 4. Check Key Column Presence
        col_exists = conn.execute(text("""
            SELECT count(*)
            FROM information_schema.columns 
            WHERE table_schema = 'bronze' 
            AND table_name = 'raw_crime' 
            AND column_name = 'dr_no';
        """)).scalar()

        if not col_exists:
            raise ValueError("⛔ CRITICAL: Schema Mismatch! Column 'dr_no' is missing.")

    print(f"✅ [RAW CHECK] Passed. Bronze Table contains {row_count} rows.")
    return True

if __name__ == "__main__":
    validate_bronze_quality()