import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import os
import json

def train_and_save_model(**kwargs):
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    engine = create_engine(db_conn)
    
    print("🧠 Starting Prophet Training (Staging -> Warehouse Mode)...")

    # [FIX] Read from 'warehouse.fact_crime'
    query = """
        SELECT date_occ as ds, count(*) as y
        FROM warehouse.fact_crime
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

    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m.fit(daily_counts)
    
    print("✅ Model trained successfully!")

    model_buffer = BytesIO()
    joblib.dump(m, model_buffer)
    model_bytes = model_buffer.getvalue()

    # [FIX] Save to 'warehouse.model_registry'
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.model_registry (
                id SERIAL PRIMARY KEY,
                model_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_blob BYTEA
            );
        """))
        
        print("💾 Saving model binary to 'warehouse.model_registry'...")
        conn.execute(
            text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
            {"name": "prophet_crime_v1", "blob": model_bytes}
        )
        conn.commit()
    
    print("✅ Model saved to Warehouse.")

if __name__ == "__main__":
    train_and_save_model()