from sqlalchemy import create_engine, text
import os

def merge_staging_to_warehouse(**kwargs):
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🏭 Building Star Schema (Robust Mode)...")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # =========================================
            # 1. CREATE DIMENSION TABLES
            # =========================================
            
            # Helper SQL for robust date parsing (Handles ISO and US formats)
            date_parsing_logic = """
                CASE 
                    WHEN date_occ LIKE '%/%' THEN TO_DATE(SPLIT_PART(date_occ, ' ', 1), 'MM/DD/YYYY')
                    ELSE date_occ::DATE 
                END
            """

            # --- DIM 1: AREA ---
            print("   -> Updating dim_area...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_area (area_id TEXT PRIMARY KEY, area_name TEXT);
                INSERT INTO warehouse.dim_area (area_id, area_name)
                SELECT DISTINCT TRIM(area_id)::FLOAT::INT::TEXT, area_name FROM staging.crime_buffer
                WHERE area_id IS NOT NULL ON CONFLICT (area_id) DO NOTHING;
                INSERT INTO warehouse.dim_area (area_id, area_name) VALUES ('0', 'Unknown Area') ON CONFLICT (area_id) DO NOTHING;
            """))

            # --- DIM 2: CRIME ---
            print("   -> Updating dim_crime...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_crime (crm_cd TEXT PRIMARY KEY, crm_cd_desc TEXT);
                INSERT INTO warehouse.dim_crime (crm_cd, crm_cd_desc)
                SELECT DISTINCT TRIM(crm_cd)::FLOAT::INT::TEXT, crm_cd_desc FROM staging.crime_buffer
                WHERE crm_cd IS NOT NULL ON CONFLICT (crm_cd) DO NOTHING;
                INSERT INTO warehouse.dim_crime (crm_cd, crm_cd_desc) VALUES ('0', 'Unknown Crime') ON CONFLICT (crm_cd) DO NOTHING;
            """))

            # --- DIM 3: STATUS ---
            print("   -> Updating dim_status...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_status (status_id TEXT PRIMARY KEY, status_desc TEXT);
                INSERT INTO warehouse.dim_status (status_id, status_desc)
                SELECT DISTINCT TRIM(status_id), status_desc FROM staging.crime_buffer
                WHERE status_id IS NOT NULL ON CONFLICT (status_id) DO NOTHING;
                INSERT INTO warehouse.dim_status (status_id, status_desc) VALUES ('XX', 'Unknown Status') ON CONFLICT (status_id) DO NOTHING;
            """))

            # --- DIM 4: PREMISE ---
            print("   -> Updating dim_premise...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_premise (premis_id TEXT PRIMARY KEY, premis_desc TEXT);
                INSERT INTO warehouse.dim_premise (premis_id, premis_desc)
                SELECT DISTINCT TRIM(premis_id)::FLOAT::INT::TEXT, premis_desc FROM staging.crime_buffer
                WHERE premis_id IS NOT NULL ON CONFLICT (premis_id) DO NOTHING;
                INSERT INTO warehouse.dim_premise (premis_id, premis_desc) VALUES ('0', 'Unknown Premise') ON CONFLICT (premis_id) DO NOTHING;
            """))

            # --- DIM 5: WEAPON ---
            print("   -> Updating dim_weapon...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_weapon (weapon_id TEXT PRIMARY KEY, weapon_desc TEXT);
                INSERT INTO warehouse.dim_weapon (weapon_id, weapon_desc)
                SELECT DISTINCT TRIM(weapon_id)::FLOAT::INT::TEXT, weapon_desc FROM staging.crime_buffer
                WHERE weapon_id IS NOT NULL ON CONFLICT (weapon_id) DO NOTHING;
                INSERT INTO warehouse.dim_weapon (weapon_id, weapon_desc) VALUES ('0', 'Unknown Weapon') ON CONFLICT (weapon_id) DO NOTHING;
            """))
            
            # --- DIM 6: DATE (Using Robust Parsing) ---
            print("   -> Updating dim_date...")
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS warehouse.dim_date (
                    date_id DATE PRIMARY KEY, year INT, month INT, day INT, day_of_week INT, quarter INT
                );
                INSERT INTO warehouse.dim_date (date_id, year, month, day, day_of_week, quarter)
                SELECT DISTINCT 
                    ({date_parsing_logic}) as date_id,
                    EXTRACT(YEAR FROM ({date_parsing_logic})),
                    EXTRACT(MONTH FROM ({date_parsing_logic})),
                    EXTRACT(DAY FROM ({date_parsing_logic})),
                    EXTRACT(DOW FROM ({date_parsing_logic})),
                    EXTRACT(QUARTER FROM ({date_parsing_logic}))
                FROM staging.crime_buffer WHERE date_occ IS NOT NULL ON CONFLICT (date_id) DO NOTHING;
            """))

            # --- DIM 7: TIME ---
            print("   -> Updating dim_time...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_time (time_id INT PRIMARY KEY, hour INT, time_of_day TEXT);
                INSERT INTO warehouse.dim_time (time_id, hour, time_of_day)
                SELECT DISTINCT 
                    time_occ::FLOAT::INT,
                    FLOOR(time_occ::FLOAT::INT / 100),
                    CASE 
                        WHEN FLOOR(time_occ::FLOAT::INT / 100) BETWEEN 5 AND 11 THEN 'Morning'
                        WHEN FLOOR(time_occ::FLOAT::INT / 100) BETWEEN 12 AND 16 THEN 'Afternoon'
                        WHEN FLOOR(time_occ::FLOAT::INT / 100) BETWEEN 17 AND 20 THEN 'Evening'
                        ELSE 'Night'
                    END
                FROM staging.crime_buffer WHERE time_occ IS NOT NULL ON CONFLICT (time_id) DO NOTHING;
            """))

            # =========================================
            # 2. CREATE FACT TABLE
            # =========================================
            print("   -> Updating fact_crime...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.fact_crime (
                    dr_no TEXT PRIMARY KEY,
                    date_occ DATE,
                    time_occ INT,
                    area_id TEXT,
                    crm_cd TEXT,
                    status_id TEXT,
                    premis_id TEXT,
                    weapon_id TEXT,
                    lat FLOAT,
                    lon FLOAT,
                    vict_age FLOAT
                );
            """))

            # [FIX] Apply Date Parsing Logic & TRIM to Fact Table
            conn.execute(text(f"""
                INSERT INTO warehouse.fact_crime (
                    dr_no, date_occ, time_occ, area_id, crm_cd, status_id, premis_id, weapon_id, lat, lon, vict_age
                )
                SELECT DISTINCT 
                    TRIM(dr_no), 
                    ({date_parsing_logic}),
                    time_occ::FLOAT::INT,
                    COALESCE(TRIM(area_id)::FLOAT::INT::TEXT, '0'), 
                    COALESCE(TRIM(crm_cd)::FLOAT::INT::TEXT, '0'), 
                    COALESCE(TRIM(status_id), 'XX'), 
                    COALESCE(TRIM(premis_id)::FLOAT::INT::TEXT, '0'),
                    COALESCE(TRIM(weapon_id)::FLOAT::INT::TEXT, '0'),
                    NULLIF(TRIM(lat::TEXT), '')::FLOAT, 
                    NULLIF(TRIM(lon::TEXT), '')::FLOAT, 
                    NULLIF(TRIM(vict_age::TEXT), '')::FLOAT
                FROM staging.crime_buffer
                WHERE dr_no IS NOT NULL
                ON CONFLICT (dr_no) DO NOTHING;
            """))
            
            trans.commit()
            print("✅ Warehouse populated with Robust Date Parsing!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error: {e}")
            raise e

if __name__ == "__main__":
    merge_staging_to_warehouse()