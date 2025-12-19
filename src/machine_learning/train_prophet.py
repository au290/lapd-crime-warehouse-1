import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import os

# --- CONFIGURATION ---
VALID_END_DATE = '2023-12-31' 

def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def check_data_quality(df):
    print("🔍 [ML QUALITY CHECK] Memeriksa kualitas data training (Cleaned)...")
    if len(df) < 100:
        raise ValueError(f"❌ Data terlalu sedikit setelah filtering (Rows: {len(df)}). Cek cut-off date.")
    print(f"✅ Data Valid dari {df['ds'].min().date()} sampai {df['ds'].max().date()}")

def train_and_save_model(**kwargs):
    engine = get_db_engine()
    print("🧠 Starting Prophet Training Pipeline...")

    query = """
        SELECT date_occ as ds, count(*) as y
        FROM warehouse.fact_crime
        WHERE date_occ IS NOT NULL
        GROUP BY date_occ
        ORDER BY date_occ
    """
    try:
        df = pd.read_sql(query, engine)
        df['ds'] = pd.to_datetime(df['ds'])
    except Exception as e:
        print(f"❌ Gagal load data: {e}")
        return

    print(f"   -> Total Raw Rows: {len(df)}")

    # Cut-Off
    print(f"✂️ APPLYING CUT-OFF: Ignoring data after {VALID_END_DATE}...")
    df_clean = df[df['ds'] <= VALID_END_DATE].copy()
    
    check_data_quality(df_clean)

    # Split
    test_days = 90
    train_df = df_clean.iloc[:-test_days]
    test_df = df_clean.iloc[-test_days:]
    
    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m.fit(train_df)

    future = m.make_future_dataframe(periods=test_days)
    forecast = m.predict(future)
    forecast_test = forecast[forecast['ds'].isin(test_df['ds'])]
    comparison = pd.merge(test_df, forecast_test[['ds', 'yhat']], on='ds')
    
    mae = mean_absolute_error(comparison['y'], comparison['yhat'])
    rmse = np.sqrt(mean_squared_error(comparison['y'], comparison['yhat']))
    print(f"📈 Model Performance (Pre-2024): MAE={mae:.2f}, RMSE={rmse:.2f}")

    # Retrain Full
    print("🔄 Retraining full model on all clean data...")
    m_final = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m_final.fit(df_clean) 

    # Save (CLEANED)
    model_buffer = BytesIO()
    joblib.dump(m_final, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Simpan Blob
            print("💾 Saving model to DB...")
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "prophet_crime_v1", "blob": model_bytes}
            )
            
            # Simpan Metrics
            print("📊 Saving metrics to DB...")
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "prophet_crime_v1", "mae": mae, "rmse": rmse, "rows": len(df_clean)}
            )
            
            trans.commit()
            print("✅ Pipeline ML Selesai (Cut-Off Strategy Applied)!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Gagal menyimpan ke DB: {e}")
            raise e

if __name__ == "__main__":
    train_and_save_model()