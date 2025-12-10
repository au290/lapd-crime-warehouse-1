-- docker/postgres/init.sql

-- 1. Create Schemas for our Data Warehouse Layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. Optional: Create a Read-Only User for the Dashboard
-- (Security Best Practice: The dashboard shouldn't be able to drop tables)
CREATE USER dashboard_user WITH PASSWORD 'dash_pass';
GRANT USAGE ON SCHEMA gold TO dashboard_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dashboard_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO dashboard_user;

-- 3. Set Timezone (Optional but good for logs)
ALTER DATABASE lapd_warehouse SET timezone TO 'America/Los_Angeles';