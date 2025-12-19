import pandas as pd
import xgboost as xgb
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import holidays
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def create_features(data):
    X = data.copy()
    us_holidays = holidays.US()
    
    X['day_of_week'] = X['ds'].dt.dayofweek
    X['quarter'] = X['ds'].dt.quarter
    X['month'] = X['ds'].dt.month
    X['year'] = X['ds'].dt.year
    X['day_of_year'] = X['ds'].dt.dayofyear
    X['week_of_year'] = X['ds'].dt.isocalendar().week.astype(int)
    X['is_holiday'] = X['ds'].apply(lambda x: 1 if x in us_holidays else 0)
    
    return X.drop(columns=['ds', 'y'], errors='ignore')

def train_daily_model(**kwargs):
    engine = get_db_engine()
    print("🚀 Starting Daily Model Retraining (XGBoost)...")

    # 1. Load Data
    query = """
        SELECT date_occ as ds, count(*) as y
        FROM warehouse.fact_crime
        WHERE date_occ IS NOT NULL
        GROUP BY date_occ
        ORDER BY date_occ
    """
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])

    # 2. Dynamic Cut-Off (Up to Yesterday)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"📅 Training Data Cut-off: {yesterday}")
    
    df_clean = df[df['ds'] <= yesterday].copy()
    print(f"   -> Training on {len(df_clean)} rows.")

    # 3. Prepare Features
    X_train = create_features(df_clean)
    y_train = df_clean['y']

    # 4. Train Model
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=5,
        subsample=0.8,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    print("✅ XGBoost Model Retrained.")

    # 5. Save to Database (CLEANED)
    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Save Model
            print("💾 Saving model binary...")
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "xgboost_crime_v1", "blob": model_bytes}
            )
            
            # Save Log (Placeholder metrics for daily run)
            print("📊 Saving metrics...")
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "xgboost_crime_v1", "mae": 0.0, "rmse": 0.0, "rows": len(df_clean)}
            )
            
            trans.commit()
            print("🎉 New Model Deployed to Warehouse!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ DB Save Failed: {e}")
            raise e

if __name__ == "__main__":
    train_daily_model()