-- 1. CREATE USER (Critical Fix)
-- We must create the 'airflow' user before we can grant it permissions.
-- Setting password to 'airflow' (make sure this matches your Airflow Connection/env vars)
CREATE USER airflow WITH PASSWORD 'airflow';

-- Optional: Give airflow superuser rights for easier local debugging (avoids "permission denied" later)
ALTER USER airflow WITH SUPERUSER;

-- 2. STAGING SCHEMA
CREATE SCHEMA IF NOT EXISTS staging;
-- Now this will work because the user exists
GRANT ALL ON SCHEMA staging TO airflow;

-- 3. WAREHOUSE SCHEMA
CREATE SCHEMA IF NOT EXISTS warehouse;
GRANT ALL ON SCHEMA warehouse TO airflow;

-- 4. UTILS
-- (Ensure your docker-compose POSTGRES_DB is actually named 'lapd_warehouse')
ALTER DATABASE lapd_warehouse SET timezone TO 'America/Los_Angeles';