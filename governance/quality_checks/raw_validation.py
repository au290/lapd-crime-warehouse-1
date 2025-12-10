from sqlalchemy import create_engine, text
import os

def validate_staging_quality(**kwargs):
    # 1. Configuration
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🔍 [QUALITY CHECK] Verifying Staging Layer...")
    
    with engine.connect() as conn:
        # 2. Check Staging Table Exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'staging'
                AND    table_name   = 'crime_buffer'
            );
        """)).scalar()

        if not table_exists:
            raise ValueError("⛔ CRITICAL: Table 'staging.crime_buffer' NOT FOUND.")

        # 3. Check Row Count
        row_count = conn.execute(text("SELECT count(*) FROM staging.crime_buffer")).scalar()
        
        if row_count == 0:
            raise ValueError("⛔ CRITICAL: Staging table is EMPTY. Extraction likely failed.")
            
    print(f"✅ [QUALITY CHECK] Passed. Staging contains {row_count} rows.")
    return True

if __name__ == "__main__":
    validate_staging_quality()