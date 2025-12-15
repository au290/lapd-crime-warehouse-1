import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from io import BytesIO
import joblib
import holidays
import os
import gc # Garbage Collection

# --- CONFIGURATION ---
SAFE_CUTOFF_DATE = '2023-12-31'

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
    return X[['day_of_week', 'quarter', 'month', 'year', 'day_of_year', 'week_of_year', 'is_holiday']]

def train_and_evaluate(df, model_name, algo='rf', cutoff_date=None):
    print(f"\n⚙️ Processing Model: {model_name} [{algo.upper()}]...")
    
    if cutoff_date:
        print(f"   ✂️ Applying Cutoff: {cutoff_date}")
        df_variant = df[df['ds'] <= cutoff_date].copy()
    else:
        print(f"   ⚠️ Using FULL Historical Data (No Cutoff).")
        df_variant = df.copy()

    if df_variant.empty:
        print("   ❌ Error: Dataset empty.")
        return

    # Split (Last 90 Days)
    test_days = 90
    train_df = df_variant.iloc[:-test_days].copy()
    test_df = df_variant.iloc[-test_days:].copy()

    X_train = create_features(train_df)
    y_train = train_df['y']
    
    # Init Model
    if algo == 'xgb':
        model = XGBRegressor(n_estimators=1000, learning_rate=0.01, max_depth=5, 
                             subsample=0.8, n_jobs=-1, objective='reg:squarederror')
    else:
        model = RandomForestRegressor(n_estimators=500, max_depth=10, min_samples_leaf=4, 
                                      n_jobs=-1, random_state=42)
        
    model.fit(X_train, y_train)

    # Evaluate
    X_test = create_features(test_df)
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(test_df['y'], y_pred)
    rmse = np.sqrt(mean_squared_error(test_df['y'], y_pred))
    print(f"   📊 Results: MAE={mae:.2f} | RMSE={rmse:.2f}")

    # Retrain Full
    X_full = create_features(df_variant)
    model.fit(X_full, df_variant['y'])

    # Save to DB
    engine = get_db_engine()
    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text("CREATE TABLE IF NOT EXISTS warehouse.model_registry (id SERIAL PRIMARY KEY, model_name VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, model_blob BYTEA);"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS warehouse.model_metrics (id SERIAL PRIMARY KEY, model_name VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, mae FLOAT, rmse FLOAT, training_rows INT);"))
            conn.execute(text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"), {"name": model_name, "blob": model_bytes})
            conn.execute(text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"), {"name": model_name, "mae": mae, "rmse": rmse, "rows": len(df_variant)})
            trans.commit()
            print(f"   ✅ Saved '{model_name}' to Warehouse.")
        except Exception as e:
            trans.rollback()
            print(f"   ❌ DB Error: {e}")

    # FORCE MEMORY RELEASE
    del model
    del X_train, y_train, X_full, df_variant
    gc.collect()

def run_experiment(target_model="all"):
    """
    Control function to run specific models based on Airflow input.
    """
    engine = get_db_engine()
    
    # Load Data once
    query = "SELECT date_occ as ds, count(*) as y FROM warehouse.fact_crime WHERE date_occ IS NOT NULL GROUP BY date_occ ORDER BY date_occ"
    try:
        df = pd.read_sql(query, engine)
        df['ds'] = pd.to_datetime(df['ds'])
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # Logic to select which model to run
    if target_model == "all" or target_model == "rf_safe_2023":
        train_and_evaluate(df, "rf_safe_2023", algo='rf', cutoff_date=SAFE_CUTOFF_DATE)

    if target_model == "all" or target_model == "rf_full_latest":
        train_and_evaluate(df, "rf_full_latest", algo='rf', cutoff_date=None)

    if target_model == "all" or target_model == "xgboost_full_latest":
        train_and_evaluate(df, "xgboost_full_latest", algo='xgb', cutoff_date=None)

if __name__ == "__main__":
    import sys
    # Allow running via CLI: python train_rf.py rf_safe_2023
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    run_experiment(arg)