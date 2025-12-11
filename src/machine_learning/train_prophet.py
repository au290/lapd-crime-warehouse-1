import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import os

# --- CONFIGURATION ---
# Cut-off date to ignore broken/missing data in 2024-2025
VALID_END_DATE = '2023-12-31' 

def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def check_data_quality(df):
    """
    Checks basic quality of the CLEANED dataset.
    """
    print("🔍 [ML QUALITY CHECK] Memeriksa kualitas data training (Cleaned)...")
    
    # 1. Cek Data Count
    if len(df) < 100:
        raise ValueError(f"❌ Data terlalu sedikit setelah filtering (Rows: {len(df)}). Cek cut-off date.")

    # 2. Cek Missing Values
    missing = df.isnull().sum().sum()
    if missing > 0:
        print(f"⚠️ Peringatan: Ditemukan {missing} missing values.")
    else:
        print("✅ Tidak ada missing values.")

    print(f"✅ Data Valid dari {df['ds'].min().date()} sampai {df['ds'].max().date()}")

def train_and_save_model(**kwargs):
    engine = get_db_engine()
    print("🧠 Starting Prophet Training Pipeline...")

    # 1. Load Data dari Warehouse
    # Mengambil semua data yang ada dulu
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

    # 2. [CRITICAL FIX] Apply Cut-Off Date
    # Membuang data setelah 2023 karena kualitas buruk (drop-off)
    print(f"✂️ APPLYING CUT-OFF: Ignoring data after {VALID_END_DATE}...")
    df_clean = df[df['ds'] <= VALID_END_DATE].copy()
    
    print(f"   -> Clean Rows used for Training: {len(df_clean)}")
    
    # Lakukan Quality Check pada data yang SUDAH dibersihkan
    check_data_quality(df_clean)

    # 3. Train/Test Split (Backtesting)
    # Kita ambil 90 hari terakhir dari data BERSIH untuk tes
    test_days = 90
    train_df = df_clean.iloc[:-test_days]
    test_df = df_clean.iloc[-test_days:]
    
    print(f"📊 Split Info:")
    print(f"   Training: {train_df['ds'].min().date()} --to-- {train_df['ds'].max().date()} ({len(train_df)} rows)")
    print(f"   Testing:  {test_df['ds'].min().date()} --to-- {test_df['ds'].max().date()} ({len(test_df)} rows)")

    # 4. Training Model (Evaluasi)
    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m.fit(train_df)

    # 5. Evaluasi pada Test Set
    future = m.make_future_dataframe(periods=test_days)
    forecast = m.predict(future)
    
    # Filter hasil prediksi agar pas dengan tanggal test_df
    forecast_test = forecast[forecast['ds'].isin(test_df['ds'])]
    
    # Gabungkan untuk perbandingan
    comparison = pd.merge(test_df, forecast_test[['ds', 'yhat']], on='ds')
    
    # Hitung Metrics
    mae = mean_absolute_error(comparison['y'], comparison['yhat'])
    rmse = np.sqrt(mean_squared_error(comparison['y'], comparison['yhat']))
    
    print(f"📈 Model Performance (Pre-2024): MAE={mae:.2f}, RMSE={rmse:.2f}")
    print("   (Note: Error ini valid karena dites pada data historis yang lengkap)")

    # 6. Retrain Full Model (Production Ready)
    # Kita train ulang menggunakan SELURUH data bersih (sampai akhir 2023)
    # Model ini akan dipakai dashboard untuk memprediksi 2024/2025 (baseline)
    print("🔄 Retraining full model on all clean data...")
    m_final = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m_final.fit(df_clean) 

    # 7. Simpan Model & Metrics ke Warehouse
    model_buffer = BytesIO()
    joblib.dump(m_final, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Pastikan tabel ada
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.model_registry (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    model_blob BYTEA
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.model_metrics (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    mae FLOAT,
                    rmse FLOAT,
                    training_rows INT
                );
            """))

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