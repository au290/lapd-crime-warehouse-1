-- docker/postgres/init.sql

-- 1. STAGING SCHEMA (Temporary holding area)
CREATE SCHEMA IF NOT EXISTS staging;

-- 2. WAREHOUSE SCHEMA (Final Star Schema)
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 3. UTILS
ALTER DATABASE lapd_warehouse SET timezone TO 'America/Los_Angeles';