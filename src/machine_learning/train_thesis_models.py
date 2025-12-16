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

# --- CONFIGURATION ---
# The thesis data is already cut off at 2023, so we just use it all.
TABLE_SOURCE = "staging.thesis_crime_buffer"

def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def create_features(data):
    """
    Feature Engineering for Daily Forecasting.
    (Aggregated from the clean thesis data)
    """
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

def train_and_evaluate(df, model_name, algo='rf'):
    print(f"\n⚙️ Training {model_name} on Thesis Data...")
    
    if df.empty:
        print("❌ Error: Dataset is empty.")
        return

    # 1. Train/Test Split (Last 90 days of the 2023 data)
    test_days = 90
    train_df = df.iloc[:-test_days].copy()
    test_df = df.iloc[-test_days:].copy()

    # 2. Prepare Features
    X_train = create_features(train_df)
    y_train = train_df['y']
    X_test = create_features(test_df)
    
    # 3. Initialize Model
    if algo == 'xgb':
        model = XGBRegressor(
            n_estimators=1000, 
            learning_rate=0.01, 
            max_depth=5, 
            subsample=0.8, 
            n_jobs=-1, 
            random_state=42
        )
    else:
        model = RandomForestRegressor(
            n_estimators=500, 
            max_depth=10, 
            min_samples_leaf=4, 
            n_jobs=-1, 
            random_state=42
        )
        
    # 4. Train
    model.fit(X_train, y_train)

    # 5. Evaluate
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(test_df['y'], y_pred)
    rmse = np.sqrt(mean_squared_error(test_df['y'], y_pred))
    print(f"   📊 Results: MAE={mae:.2f} | RMSE={rmse:.2f}")

    # 6. Retrain on ALL Thesis Data (for Dashboard Forecast)
    X_full = create_features(df)
    model.fit(X_full, df['y'])

    # 7. Save to Warehouse
    engine = get_db_engine()
    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Upsert into Model Registry
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": model_name, "blob": model_bytes}
            )
            # Log Metrics
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": model_name, "mae": mae, "rmse": rmse, "rows": len(df)}
            )
            trans.commit()
            print(f"   ✅ Saved '{model_name}' to Warehouse.")
        except Exception as e:
            trans.rollback()
            print(f"   ❌ DB Error: {e}")

def run_thesis_training():
    engine = get_db_engine()
    
    # 1. Aggregate Data from Thesis Staging
    # We group by day to create a Time Series dataset
    print(f"📥 Loading data from {TABLE_SOURCE}...")
    query = f"""
        SELECT date_occ as ds, count(*) as y
        FROM {TABLE_SOURCE}
        GROUP BY date_occ
        ORDER BY date_occ
    """
    try:
        df = pd.read_sql(query, engine)
        df['ds'] = pd.to_datetime(df['ds'])
        print(f"   -> Aggregated into {len(df)} days of data.")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # 2. Train Models
    # We name them "thesis_..." so they are distinct in the dashboard
    train_and_evaluate(df, "thesis_xgboost", algo='xgb')
    train_and_evaluate(df, "thesis_rf", algo='rf')

if __name__ == "__main__":
    run_thesis_training()