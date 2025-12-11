from sqlalchemy import create_engine, text
import os

def merge_staging_to_warehouse(**kwargs):
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🏭 Building Star Schema (7 Dimensions + 1 Fact)...")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # =========================================
            # 1. CREATE DIMENSION TABLES
            # =========================================
            # Note: We use ::INT::TEXT casting to handle cases where Staging has numbers (1.0) 
            # but Warehouse needs strings ('1').
            
            # --- DIM 1: AREA (Geography) ---
            print("   -> Updating dim_area...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_area (
                    area_id TEXT PRIMARY KEY, 
                    area_name TEXT
                );
                INSERT INTO warehouse.dim_area (area_id, area_name)
                SELECT DISTINCT 
                    area_id::INT::TEXT, -- Cast to clean string
                    area_name 
                FROM staging.crime_buffer
                WHERE area_id IS NOT NULL
                ON CONFLICT (area_id) DO NOTHING;
            """))

            # --- DIM 2: CRIME (Crime Type) ---
            print("   -> Updating dim_crime...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_crime (
                    crm_cd TEXT PRIMARY KEY, 
                    crm_cd_desc TEXT
                );
                INSERT INTO warehouse.dim_crime (crm_cd, crm_cd_desc)
                SELECT DISTINCT 
                    crm_cd::INT::TEXT, 
                    crm_cd_desc 
                FROM staging.crime_buffer
                WHERE crm_cd IS NOT NULL
                ON CONFLICT (crm_cd) DO NOTHING;
            """))

            # --- DIM 3: STATUS (Case Status) ---
            print("   -> Updating dim_status...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_status (
                    status_id TEXT PRIMARY KEY, 
                    status_desc TEXT
                );
                INSERT INTO warehouse.dim_status (status_id, status_desc)
                SELECT DISTINCT status_id, status_desc FROM staging.crime_buffer
                WHERE status_id IS NOT NULL
                ON CONFLICT (status_id) DO NOTHING;
            """))

            # --- DIM 4: PREMISE (Location Type) ---
            print("   -> Updating dim_premise...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_premise (
                    premis_id TEXT PRIMARY KEY, 
                    premis_desc TEXT
                );
                INSERT INTO warehouse.dim_premise (premis_id, premis_desc)
                SELECT DISTINCT 
                    premis_id::INT::TEXT, 
                    premis_desc 
                FROM staging.crime_buffer
                WHERE premis_id IS NOT NULL
                ON CONFLICT (premis_id) DO NOTHING;
            """))

            # --- DIM 5: WEAPON (Weapon Used) ---
            print("   -> Updating dim_weapon...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_weapon (
                    weapon_id TEXT PRIMARY KEY, 
                    weapon_desc TEXT
                );
                INSERT INTO warehouse.dim_weapon (weapon_id, weapon_desc)
                SELECT DISTINCT 
                    weapon_id::INT::TEXT, 
                    weapon_desc 
                FROM staging.crime_buffer
                WHERE weapon_id IS NOT NULL
                ON CONFLICT (weapon_id) DO NOTHING;
            """))
            
            # --- DIM 6: DATE (Generated Calendar) ---
            print("   -> Updating dim_date...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_date (
                    date_id DATE PRIMARY KEY,
                    year INT,
                    month INT,
                    day INT,
                    day_of_week INT,
                    quarter INT
                );
                
                INSERT INTO warehouse.dim_date (date_id, year, month, day, day_of_week, quarter)
                SELECT DISTINCT 
                    date_occ::DATE as date_id,
                    EXTRACT(YEAR FROM date_occ::TIMESTAMP) as year,
                    EXTRACT(MONTH FROM date_occ::TIMESTAMP) as month,
                    EXTRACT(DAY FROM date_occ::TIMESTAMP) as day,
                    EXTRACT(DOW FROM date_occ::TIMESTAMP) as day_of_week,
                    EXTRACT(QUARTER FROM date_occ::TIMESTAMP) as quarter
                FROM staging.crime_buffer
                WHERE date_occ IS NOT NULL
                ON CONFLICT (date_id) DO NOTHING;
            """))

            # --- DIM 7: TIME (Generated Buckets) ---
            print("   -> Updating dim_time...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_time (
                    time_id INT PRIMARY KEY,
                    hour INT,
                    time_of_day TEXT
                );
                
                INSERT INTO warehouse.dim_time (time_id, hour, time_of_day)
                SELECT DISTINCT 
                    time_occ::INT as time_id,
                    FLOOR(time_occ::INT / 100) as hour,
                    CASE 
                        WHEN FLOOR(time_occ::INT / 100) BETWEEN 5 AND 11 THEN 'Morning'
                        WHEN FLOOR(time_occ::INT / 100) BETWEEN 12 AND 16 THEN 'Afternoon'
                        WHEN FLOOR(time_occ::INT / 100) BETWEEN 17 AND 20 THEN 'Evening'
                        ELSE 'Night'
                    END as time_of_day
                FROM staging.crime_buffer
                WHERE time_occ IS NOT NULL
                ON CONFLICT (time_id) DO NOTHING;
            """))

            # =========================================
            # 2. CREATE FACT TABLE
            # =========================================
            print("   -> Updating fact_crime...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.fact_crime (
                    dr_no TEXT PRIMARY KEY,
                    date_occ DATE,    -- FK to dim_date
                    time_occ INT,     -- FK to dim_time
                    area_id TEXT,     -- FK to dim_area
                    crm_cd TEXT,      -- FK to dim_crime
                    status_id TEXT,   -- FK to dim_status
                    premis_id TEXT,   -- FK to dim_premise
                    weapon_id TEXT,   -- FK to dim_weapon
                    lat FLOAT,
                    lon FLOAT,
                    vict_age FLOAT
                );
            """))

            # Insert Data using robust casting
            # [FIX] Added ::INT::TEXT to ID columns in SELECT and WHERE clauses
            conn.execute(text("""
                INSERT INTO warehouse.fact_crime (
                    dr_no, date_occ, time_occ, area_id, crm_cd, status_id, premis_id, weapon_id, lat, lon, vict_age
                )
                SELECT DISTINCT 
                    dr_no, 
                    date_occ::DATE,
                    time_occ::INT,
                    area_id::INT::TEXT, 
                    crm_cd::INT::TEXT, 
                    status_id, 
                    premis_id::INT::TEXT,
                    weapon_id::INT::TEXT,
                    NULLIF(TRIM(lat::TEXT), '')::FLOAT, 
                    NULLIF(TRIM(lon::TEXT), '')::FLOAT, 
                    NULLIF(TRIM(vict_age::TEXT), '')::FLOAT
                FROM staging.crime_buffer
                WHERE dr_no IS NOT NULL
                -- Verify FKs exist (Casting ensures types match)
                AND area_id::INT::TEXT IN (SELECT area_id FROM warehouse.dim_area)
                AND crm_cd::INT::TEXT IN (SELECT crm_cd FROM warehouse.dim_crime)
                AND status_id IN (SELECT status_id FROM warehouse.dim_status)
                ON CONFLICT (dr_no) DO NOTHING;
            """))
            
            trans.commit()
            print("✅ 7-Dimension Star Schema Created Successfully!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error: {e}")
            raise e

if __name__ == "__main__":
    merge_staging_to_warehouse()