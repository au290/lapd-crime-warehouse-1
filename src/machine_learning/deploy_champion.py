import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import holidays
import os

# --- CONFIGURATION ---
VALID_END_DATE = '2023-12-31' 

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

def train_and_deploy(**kwargs):
    engine = get_db_engine()
    print("🚀 Starting XGBoost Champion Deployment (Real Metrics Mode)...")

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

    # 2. Filter Cut-Off
    df_clean = df[df['ds'] <= VALID_END_DATE].copy()
    print(f"   -> Total Data Points: {len(df_clean)} rows.")

    # 3. Calculate Real Metrics
    test_days = 90
    train_df = df_clean.iloc[:-test_days].copy()
    test_df = df_clean.iloc[-test_days:].copy()

    X_train_split = create_features(train_df)
    y_train_split = train_df['y']
    X_test_split = create_features(test_df)
    y_test_real = test_df['y']

    eval_model = xgb.XGBRegressor(
        objective='reg:squarederror', n_estimators=1000, learning_rate=0.01, 
        max_depth=5, subsample=0.8, n_jobs=-1
    )
    eval_model.fit(X_train_split, y_train_split)

    y_pred = eval_model.predict(X_test_split)
    mae = mean_absolute_error(y_test_real, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test_real, y_pred))
    
    print(f"   📊 Real Performance: MAE={mae:.2f} | RMSE={rmse:.2f}")

    # 4. Retrain Full Model
    print("   -> Retraining on full dataset for deployment...")
    X_full = create_features(df_clean)
    y_full = df_clean['y']
    
    final_model = xgb.XGBRegressor(
        objective='reg:squarederror', n_estimators=1000, learning_rate=0.01, 
        max_depth=5, subsample=0.8, n_jobs=-1
    )
    final_model.fit(X_full, y_full)

    # 5. Save to Database (CLEANED)
    model_buffer = BytesIO()
    joblib.dump(final_model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Save Model
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "xgboost_crime_v1", "blob": model_bytes}
            )
            
            # Save Metrics
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "xgboost_crime_v1", "mae": mae, "rmse": rmse, "rows": len(df_clean)}
            )
            
            trans.commit()
            print("🎉 Champion Model Deployed with REAL metrics!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ DB Save Failed: {e}")
            raise e

if __name__ == "__main__":
    train_and_deploy()