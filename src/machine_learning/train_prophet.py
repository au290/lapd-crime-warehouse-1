import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import os
import json

def train_and_save_model(**kwargs):
    # 1. Connection
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🧠 Starting Prophet Training (Warehouse Mode)...")

    # 2. Load Data from Gold Layer
    # [FILTER] Data 2024/2025 yang tidak lengkap/anomali kita filter di query SQL
    query = """
        SELECT date_occ as ds, count(*) as y
        FROM gold.fact_crime
        WHERE date_occ <= '2023-12-31'
        GROUP BY date_occ
        ORDER BY date_occ
    """
    try:
        daily_counts = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Failed to load training data: {e}")
        return

    if daily_counts.empty:
        print("❌ No data available for training.")
        return

    print(f"📊 Training on {len(daily_counts)} days of history.")

    # 3. Train Prophet
    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m.fit(daily_counts)
    
    print("✅ Model trained successfully!")

    # 4. Serialize Model to Bytes
    model_buffer = BytesIO()
    joblib.dump(m, model_buffer)
    model_bytes = model_buffer.getvalue()

    # 5. Save to Database (Model Registry)
    with engine.connect() as conn:
        # Create Registry Table if not exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.model_registry (
                id SERIAL PRIMARY KEY,
                model_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_blob BYTEA
            );
        """))
        
        # Insert Model
        print("💾 Saving model binary to 'gold.model_registry'...")
        conn.execute(
            text("INSERT INTO gold.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
            {"name": "prophet_crime_v1", "blob": model_bytes}
        )
        conn.commit()
    
    print("✅ Model saved to Database.")

if __name__ == "__main__":
    train_and_save_model()