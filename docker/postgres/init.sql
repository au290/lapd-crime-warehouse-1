-- 1. SETUP USER & SCHEMA
CREATE USER airflow WITH PASSWORD 'airflow';
ALTER USER airflow WITH SUPERUSER;

CREATE SCHEMA IF NOT EXISTS staging;
GRANT ALL ON SCHEMA staging TO airflow;

CREATE SCHEMA IF NOT EXISTS warehouse;
GRANT ALL ON SCHEMA warehouse TO airflow;

ALTER DATABASE lapd_warehouse SET timezone TO 'America/Los_Angeles';

-- 2. DEFINISI TABEL DIMENSI (DDL)
CREATE TABLE IF NOT EXISTS warehouse.dim_area (
    area_id TEXT PRIMARY KEY, 
    area_name TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_crime (
    crm_cd TEXT PRIMARY KEY, 
    crm_cd_desc TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_status (
    status_id TEXT PRIMARY KEY, 
    status_desc TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_premise (
    premis_id TEXT PRIMARY KEY, 
    premis_desc TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_weapon (
    weapon_id TEXT PRIMARY KEY, 
    weapon_desc TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id DATE PRIMARY KEY, 
    year INT, 
    month INT, 
    day INT, 
    day_of_week INT, 
    quarter INT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_id INT PRIMARY KEY, 
    hour INT, 
    time_of_day TEXT
);

-- 3. DEFINISI FACT TABLE (DDL) + RELASI
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
    vict_age FLOAT,
    
    -- Foreign Keys (Agar Diagram DBeaver Otomatis Terhubung)
    CONSTRAINT fk_area FOREIGN KEY (area_id) REFERENCES warehouse.dim_area(area_id),
    CONSTRAINT fk_crime FOREIGN KEY (crm_cd) REFERENCES warehouse.dim_crime(crm_cd),
    CONSTRAINT fk_status FOREIGN KEY (status_id) REFERENCES warehouse.dim_status(status_id),
    CONSTRAINT fk_premise FOREIGN KEY (premis_id) REFERENCES warehouse.dim_premise(premis_id),
    CONSTRAINT fk_weapon FOREIGN KEY (weapon_id) REFERENCES warehouse.dim_weapon(weapon_id),
    CONSTRAINT fk_date FOREIGN KEY (date_occ) REFERENCES warehouse.dim_date(date_id),
    CONSTRAINT fk_time FOREIGN KEY (time_occ) REFERENCES warehouse.dim_time(time_id)
);

-- Default Values untuk Foreign Keys
INSERT INTO warehouse.dim_area (area_id, area_name) VALUES ('0', 'Unknown Area') ON CONFLICT DO NOTHING;
INSERT INTO warehouse.dim_crime (crm_cd, crm_cd_desc) VALUES ('0', 'Unknown Crime') ON CONFLICT DO NOTHING;
INSERT INTO warehouse.dim_status (status_id, status_desc) VALUES ('XX', 'Unknown Status') ON CONFLICT DO NOTHING;
INSERT INTO warehouse.dim_premise (premis_id, premis_desc) VALUES ('0', 'Unknown Premise') ON CONFLICT DO NOTHING;
INSERT INTO warehouse.dim_weapon (weapon_id, weapon_desc) VALUES ('0', 'Unknown Weapon') ON CONFLICT DO NOTHING;

-- 4. MACHINE LEARNING TABLES (DDL)
-- Tabel Model Registry
CREATE TABLE IF NOT EXISTS warehouse.model_registry (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_blob BYTEA
);

-- Tabel Metrics
CREATE TABLE IF NOT EXISTS warehouse.model_metrics (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mae FLOAT,
    rmse FLOAT,
    training_rows INT
);

-- Indexing
CREATE INDEX IF NOT EXISTS idx_model_name_created ON warehouse.model_registry (model_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name_created ON warehouse.model_metrics (model_name, created_at DESC);