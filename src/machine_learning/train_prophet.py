import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import os

def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def check_data_quality(df):
    """
    [REQ UAS] Quality metrics (missing values, outliers)
    """
    print("🔍 [ML QUALITY CHECK] Memeriksa kualitas data training...")
    
    # 1. Cek Missing Values
    missing = df.isnull().sum().sum()
    if missing > 0:
        print(f"⚠️ Peringatan: Ditemukan {missing} missing values. Prophet bisa menanganinya, tapi harap cek data source.")
    else:
        print("✅ Tidak ada missing values.")

    # 2. Cek Outliers (Menggunakan Z-Score sederhana pada jumlah kejadian)
    mean_y = df['y'].mean()
    std_y = df['y'].std()
    threshold = 3
    outliers = df[(np.abs((df['y'] - mean_y) / std_y)) > threshold]
    
    print(f"ℹ️ Statistik Data: Mean={mean_y:.2f}, Std={std_y:.2f}")
    if not outliers.empty:
        print(f"⚠️ Peringatan: Ditemukan {len(outliers)} data outliers (Z-Score > 3).")
        # Opsional: Bisa di-remove jika mau, tapi Prophet tahan terhadap outlier.
    else:
        print("✅ Data terlihat normal (tidak ada outliers ekstrem).")

def train_and_save_model(**kwargs):
    engine = get_db_engine()
    print("🧠 Starting Prophet Training Pipeline...")

    # 1. Load Data dari Warehouse [REQ: Dataset dari DW]
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

    if df.empty or len(df) < 30:
        print("❌ Data terlalu sedikit untuk training (min 30 hari).")
        return

    # 2. Data Quality Check [REQ: Quality Metrics]
    check_data_quality(df)

    # 3. Train/Test Split (Untuk Evaluasi Akurasi)
    # Kita ambil 30 hari terakhir sebagai data tes (validasi)
    train_df = df.iloc[:-30]
    test_df = df.iloc[-30:]
    
    print(f"📊 Training pada {len(train_df)} hari, Testing pada {len(test_df)} hari.")

    # 4. Training Model
    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m.fit(train_df)

    # 5. Evaluasi / Prediction pada Test Set
    future = m.make_future_dataframe(periods=30)
    forecast = m.predict(future)
    
    # Ambil hasil prediksi yang tanggalnya sama dengan test_df
    forecast_test = forecast[forecast['ds'].isin(test_df['ds'])]
    
    # Gabungkan untuk perbandingan
    comparison = pd.merge(test_df, forecast_test[['ds', 'yhat']], on='ds')
    
    # [REQ: Model Performance Metrics]
    mae = mean_absolute_error(comparison['y'], comparison['yhat'])
    rmse = np.sqrt(mean_squared_error(comparison['y'], comparison['yhat']))
    
    print(f"📈 Model Performance: MAE={mae:.2f}, RMSE={rmse:.2f}")

    # 6. Retrain Full Model (Untuk Production)
    # Setelah tahu akurasinya, kita train ulang dengan SEMUA data untuk prediksi masa depan
    m_final = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m_final.fit(df) # Pakai full data

    # 7. Simpan Model & Metrics ke Warehouse [REQ: Distribusi ke DW]
    model_buffer = BytesIO()
    joblib.dump(m_final, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Tabel Model Registry (Blob)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.model_registry (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    model_blob BYTEA
                );
            """))
            
            # Tabel Model Metrics (Untuk Dashboard BI)
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

            # Simpan Blob Model
            print("💾 Menyimpan model ke 'warehouse.model_registry'...")
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "prophet_crime_v1", "blob": model_bytes}
            )
            
            # Simpan Metrics (Agar bisa dibaca Dashboard)
            print("📊 Menyimpan metrics ke 'warehouse.model_metrics'...")
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "prophet_crime_v1", "mae": mae, "rmse": rmse, "rows": len(train_df)}
            )
            
            trans.commit()
            print("✅ Pipeline ML Selesai & Terdistribusi ke DW!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Gagal menyimpan ke DB: {e}")
            raise e

if __name__ == "__main__":
    train_and_save_model()