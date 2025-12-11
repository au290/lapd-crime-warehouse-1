import pandas as pd
import xgboost as xgb
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
    """
    Must match exactly the features used in the Dashboard!
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
    
    return X.drop(columns=['ds', 'y'], errors='ignore')

def train_and_deploy(**kwargs):
    engine = get_db_engine()
    print("🚀 Starting XGBoost Champion Deployment...")

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

    # 2. Filter Cut-Off (Only clean history)
    df_clean = df[df['ds'] <= VALID_END_DATE].copy()
    print(f"   -> Training on {len(df_clean)} rows (Clean History).")

    # 3. Prepare Features
    X_train = create_features(df_clean)
    y_train = df_clean['y']

    # 4. Train Model (Using BEST PARAMS from Compare V2)
    # Params: {'learning_rate': 0.01, 'max_depth': 5, 'n_estimators': 1000, 'subsample': 0.8}
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=5,
        subsample=0.8,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    print("✅ XGBoost Model Trained Successfully.")

    # 5. Save to Database
    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_bytes = model_buffer.getvalue()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Create Table if missing
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.model_registry (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    model_blob BYTEA
                );
            """))
            
            # Insert Champion Model
            conn.execute(
                text("INSERT INTO warehouse.model_registry (model_name, model_blob) VALUES (:name, :blob)"),
                {"name": "xgboost_crime_v1", "blob": model_bytes}
            )
            
            # Log Metrics (Static reference from V2 battle for now)
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
            
            conn.execute(
                text("INSERT INTO warehouse.model_metrics (model_name, mae, rmse, training_rows) VALUES (:name, :mae, :rmse, :rows)"),
                {"name": "xgboost_crime_v1", "mae": 25.61, "rmse": 32.10, "rows": len(df_clean)}
            )
            
            trans.commit()
            print("🎉 Champion Model Deployed to Warehouse!")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ DB Save Failed: {e}")
            raise e

if __name__ == "__main__":
    train_and_deploy()