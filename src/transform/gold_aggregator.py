from sqlalchemy import create_engine, text
import os

def merge_staging_to_warehouse(**kwargs):
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🏭 Merging Staging Data into Warehouse...")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # 1. Dimensions (Insert New Keys)
            dims = {
                'dim_area': ('area_id', 'area_name'),
                'dim_crime': ('crm_cd', 'crm_cd_desc'),
                'dim_status': ('status_id', 'status_desc')
            }
            
            for table, (id_col, name_col) in dims.items():
                print(f"   -> Updating {table}...")
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS warehouse.{table} ({id_col} TEXT PRIMARY KEY, {name_col} TEXT);
                    
                    INSERT INTO warehouse.{table} ({id_col}, {name_col})
                    SELECT DISTINCT {id_col}, {name_col} FROM staging.crime_buffer
                    WHERE {id_col} IS NOT NULL
                    ON CONFLICT ({id_col}) DO NOTHING;
                """))

            # 2. Fact Table (Upsert / Deduplicate)
            print("   -> Updating fact_crime...")
            # Create Fact Table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.fact_crime (
                    dr_no TEXT PRIMARY KEY,
                    date_occ TIMESTAMP,
                    area_id TEXT,
                    crm_cd TEXT,
                    status_id TEXT,
                    lat FLOAT,
                    lon FLOAT,
                    vict_age FLOAT
                );
            """))

            # Insert new records (Ignore duplicates via ON CONFLICT)
            conn.execute(text("""
                INSERT INTO warehouse.fact_crime (dr_no, date_occ, area_id, crm_cd, status_id, lat, lon, vict_age)
                SELECT DISTINCT dr_no, date_occ, area_id, crm_cd, status_id, lat, lon, vict_age 
                FROM staging.crime_buffer
                WHERE dr_no IS NOT NULL
                ON CONFLICT (dr_no) DO NOTHING;
            """))
            
            trans.commit()
            print("✅ Warehouse Update Complete!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error: {e}")
            raise e

if __name__ == "__main__":
    merge_staging_to_warehouse()