import pandas as pd
from prophet import Prophet
from minio import Minio
from io import BytesIO
import joblib
import json
import os
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

def train_and_save_model(**kwargs):
    # 1. Konfigurasi
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    MODEL_BUCKET = "crime-models"
    DATA_BUCKET = "crime-gold"
    
    # [CONFIG BARU] Batas Akhir Data Stabil
    # Kita buang data 2024 & 2025 yang "aneh" agar model tidak bias
    TRAINING_CUTOFF_DATE = "2023-12-31" 
    
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    if not client.bucket_exists(MODEL_BUCKET): client.make_bucket(MODEL_BUCKET)

    print("🧠 Memulai Training Model (dengan Filter Data Stabil)...")

    # 2. Load Data
    try:
        response = client.get_object(DATA_BUCKET, "fact_crime.parquet")
        df = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"❌ Gagal load data: {e}")
        return

    # 3. Preprocessing
    if 'date_occ' in df.columns:
        df['date_occ'] = pd.to_datetime(df['date_occ'])
        
        # [SOLUSI] Filter Data Sampah / Tidak Lengkap
        # Hanya ambil data sebelum 2024 (karena 2024 drop aneh & 2025 belum lengkap)
        df_clean = df[df['date_occ'] <= TRAINING_CUTOFF_DATE]
        print(f"🧹 Data dipotong sampai {TRAINING_CUTOFF_DATE}. (Membuang data 2024-2025 yg anomali)")
        
        daily_counts = df_clean.groupby('date_occ').size().reset_index(name='y')
        daily_counts.rename(columns={'date_occ': 'ds'}, inplace=True)
    else:
        print("❌ Kolom tanggal tidak ditemukan")
        return

    print(f"📊 Data Training Bersih: {len(daily_counts)} hari.")

    # ==========================================
    # TAHAP A: EVALUASI (UJIAN MODEL)
    # ==========================================
    print("📉 Melakukan Evaluasi...")
    
    test_days = 30
    train_df = daily_counts.iloc[:-test_days]
    test_df = daily_counts.iloc[-test_days:]

    # Tambahkan 'yearly_seasonality=True' agar dia paham pola tahunan yang kuat
    m_eval = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m_eval.fit(train_df)

    future_eval = m_eval.make_future_dataframe(periods=test_days)
    forecast_eval = m_eval.predict(future_eval)
    
    pred_y = forecast_eval.iloc[-test_days:]['yhat'].values
    true_y = test_df['y'].values

    mae = mean_absolute_error(true_y, pred_y)
    rmse = np.sqrt(mean_squared_error(true_y, pred_y))
    # Handle division by zero jika ada hari dengan 0 kejahatan
    with np.errstate(divide='ignore', invalid='ignore'):
        mape = np.mean(np.abs((true_y - pred_y) / true_y)) * 100
        if np.isnan(mape): mape = 0

    print(f"✅ HASIL EVALUASI (Data Stabil):")
    print(f"   - MAE: {mae:.2f}")
    print(f"   - MAPE: {mape:.2f}%")

    metrics = {"mae": mae, "rmse": rmse, "mape": mape, "last_trained": str(pd.Timestamp.now())}
    metrics_bytes = json.dumps(metrics).encode('utf-8')
    client.put_object(MODEL_BUCKET, "model_metrics.json", BytesIO(metrics_bytes), len(metrics_bytes), content_type="application/json")

    # ==========================================
    # TAHAP B: FINAL TRAINING
    # ==========================================
    print("🚀 Melatih Model Final...")
    
    m_final = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    m_final.fit(daily_counts) 

    model_buffer = BytesIO()
    joblib.dump(m_final, model_buffer)
    model_buffer.seek(0)
    
    client.put_object(MODEL_BUCKET, "prophet_crime_v1.joblib", model_buffer, len(model_buffer.getbuffer()), content_type="application/octet-stream")
    print("💾 Model Final Tersimpan.")

if __name__ == "__main__":
    train_and_save_model()