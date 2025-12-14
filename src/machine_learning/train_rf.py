import pandas as pd
from sklearn.ensemble import RandomForestRegressor
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
    """
    Matches Dashboard Logic (No Lags for easier deployment, or update Dashboard to match)
    For simplicity, we use the same features as the Dashboard currently supports.
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
    
    # Enforce Column Order
    return X[['day_of_week', 'quarter', 'month', 'year', 'day_of_year', 'week_of_year', 'is_holiday']]

def train_random_forest(**kwargs):
    engine = get_db_engine()
    print("🌲 Starting Random Forest Training...")

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

    # 2. Cut-Off (Up to yesterday)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    df_clean = df[df['ds'] <= yesterday].copy()
    print(f"   -> Training on {len(df_clean)} rows.")

    # 3. Prepare Features
    X_train = create_features(df_clean)
    y_train = df_clean['y']

    # 4. Train Model
    print("   -> Fitting RandomForestRegressor...")
    model = RandomForestRegressor(
        n_estimators=500,
        max_depth=10,       # Prevent overfitting
        min_samples_leaf=4, # Smooth predictions
        n_jobs=-1,
        random_state=42
    )
    model.fit(X_train, y_train)
    print("✅ Random Forest Trained.")

    # 5. Save to Database
    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Ensure tables exist
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
            
            # Save Model
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "random_forest_crime_v1", "blob": model_bytes}
            )
            
            # Save Log (Placeholder metrics)
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "random_forest_crime_v1", "mae": 0.0, "rmse": 0.0, "rows": len(df_clean)}
            )
            
            trans.commit()
            print("🎉 Random Forest Deployed to Warehouse!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ DB Save Failed: {e}")
            raise e

if __name__ == "__main__":
    train_random_forest()