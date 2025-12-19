from sqlalchemy import create_engine, text
import os

def merge_staging_to_warehouse(**kwargs):
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🏭 Loading Data to Warehouse (Insert Only)...")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Logika parsing tanggal tetap diperlukan untuk INSERT
            date_parsing_logic = """
                CASE 
                    WHEN date_occ::TEXT LIKE '%/%' THEN TO_DATE(SPLIT_PART(date_occ::TEXT, ' ', 1), 'MM/DD/YYYY')
                    ELSE date_occ::DATE 
                END
            """

            # --- DIM 1: AREA (Hanya Insert) ---
            print("   -> Syncing dim_area...")
            conn.execute(text("""
                INSERT INTO warehouse.dim_area (area_id, area_name)
                SELECT DISTINCT TRIM(area_id)::FLOAT::INT::TEXT, area_name 
                FROM staging.crime_buffer
                WHERE area_id IS NOT NULL 
                ON CONFLICT (area_id) DO NOTHING;
            """))

            # --- DIM 2: CRIME (Hanya Insert) ---
            print("   -> Syncing dim_crime...")
            conn.execute(text("""
                INSERT INTO warehouse.dim_crime (crm_cd, crm_cd_desc)
                SELECT DISTINCT TRIM(crm_cd)::FLOAT::INT::TEXT, crm_cd_desc 
                FROM staging.crime_buffer
                WHERE crm_cd IS NOT NULL 
                ON CONFLICT (crm_cd) DO NOTHING;
            """))

            # --- DIM 3: STATUS (Hanya Insert) ---
            print("   -> Syncing dim_status...")
            conn.execute(text("""
                INSERT INTO warehouse.dim_status (status_id, status_desc)
                SELECT DISTINCT TRIM(status_id), status_desc 
                FROM staging.crime_buffer
                WHERE status_id IS NOT NULL 
                ON CONFLICT (status_id) DO NOTHING;
            """))

            # --- DIM 4: PREMISE (Hanya Insert) ---
            print("   -> Syncing dim_premise...")
            conn.execute(text("""
                INSERT INTO warehouse.dim_premise (premis_id, premis_desc)
                SELECT DISTINCT TRIM(premis_id)::FLOAT::INT::TEXT, premis_desc 
                FROM staging.crime_buffer
                WHERE premis_id IS NOT NULL 
                ON CONFLICT (premis_id) DO NOTHING;
            """))

            # --- DIM 5: WEAPON (Hanya Insert) ---
            print("   -> Syncing dim_weapon...")
            conn.execute(text("""
                INSERT INTO warehouse.dim_weapon (weapon_id, weapon_desc)
                SELECT DISTINCT TRIM(weapon_id)::FLOAT::INT::TEXT, weapon_desc 
                FROM staging.crime_buffer
                WHERE weapon_id IS NOT NULL 
                ON CONFLICT (weapon_id) DO NOTHING;
            """))
            
            # --- DIM 6: DATE (Hanya Insert) ---
            print("   -> Syncing dim_date...")
            conn.execute(text(f"""
                INSERT INTO warehouse.dim_date (date_id, year, month, day, day_of_week, quarter)
                SELECT DISTINCT 
                    ({date_parsing_logic}) as date_id,
                    EXTRACT(YEAR FROM ({date_parsing_logic})),
                    EXTRACT(MONTH FROM ({date_parsing_logic})),
                    EXTRACT(DAY FROM ({date_parsing_logic})),
                    EXTRACT(DOW FROM ({date_parsing_logic})),
                    EXTRACT(QUARTER FROM ({date_parsing_logic}))
                FROM staging.crime_buffer 
                WHERE date_occ IS NOT NULL 
                ON CONFLICT (date_id) DO NOTHING;
            """))

            # --- DIM 7: TIME (Hanya Insert) ---
            print("   -> Syncing dim_time...")
            conn.execute(text("""
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
                FROM staging.crime_buffer 
                WHERE time_occ IS NOT NULL 
                ON CONFLICT (time_id) DO NOTHING;
            """))

            # --- FACT TABLE (Hanya Insert) ---
            print("   -> Syncing fact_crime...")
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
            print("✅ Warehouse population complete!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error: {e}")
            raise e

if __name__ == "__main__":
    merge_staging_to_warehouse()