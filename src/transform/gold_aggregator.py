from sqlalchemy import create_engine, text
import os

def aggregate_crime_by_area(**kwargs):
    # 1. Connection
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🌟 Building Gold Layer (Star Schema)...")

    with engine.connect() as conn:
        # Use a transaction block
        trans = conn.begin()
        try:
            # ---------------------------------------------------------
            # 1. DIMENSION TABLES (Created from unique values in Silver)
            # ---------------------------------------------------------
            
            # DIM_AREA
            print("   -> Rebuilding gold.dim_area...")
            conn.execute(text("DROP TABLE IF EXISTS gold.dim_area CASCADE;"))
            conn.execute(text("""
                CREATE TABLE gold.dim_area AS
                SELECT DISTINCT area_id, area_name 
                FROM silver.crime_log
                WHERE area_id != 'Unknown';
            """))
            conn.execute(text("ALTER TABLE gold.dim_area ADD PRIMARY KEY (area_id);"))

            # DIM_CRIME
            print("   -> Rebuilding gold.dim_crime...")
            conn.execute(text("DROP TABLE IF EXISTS gold.dim_crime CASCADE;"))
            conn.execute(text("""
                CREATE TABLE gold.dim_crime AS
                SELECT DISTINCT crm_cd, crm_cd_desc 
                FROM silver.crime_log
                WHERE crm_cd != 'Unknown';
            """))
            conn.execute(text("ALTER TABLE gold.dim_crime ADD PRIMARY KEY (crm_cd);"))

            # DIM_STATUS
            print("   -> Rebuilding gold.dim_status...")
            conn.execute(text("DROP TABLE IF EXISTS gold.dim_status CASCADE;"))
            conn.execute(text("""
                CREATE TABLE gold.dim_status AS
                SELECT DISTINCT status_id, status_desc 
                FROM silver.crime_log
                WHERE status_id != 'Unknown';
            """))
            conn.execute(text("ALTER TABLE gold.dim_status ADD PRIMARY KEY (status_id);"))

            # ---------------------------------------------------------
            # 2. FACT TABLE
            # ---------------------------------------------------------
            print("   -> Rebuilding gold.fact_crime...")
            conn.execute(text("DROP TABLE IF EXISTS gold.fact_crime CASCADE;"))
            conn.execute(text("""
                CREATE TABLE gold.fact_crime AS
                SELECT 
                    dr_no,
                    date_occ,
                    area_id,
                    crm_cd,
                    status_id,
                    weapon_id,
                    premis_id,
                    vict_age,
                    lat,
                    lon
                FROM silver.crime_log;
            """))
            # Add Primary Key
            conn.execute(text("ALTER TABLE gold.fact_crime ADD PRIMARY KEY (dr_no);"))

            trans.commit()
            print("✅ Star Schema Successfully Built in PostgreSQL.")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error building Gold layer: {e}")
            raise e

if __name__ == "__main__":
    aggregate_crime_by_area()