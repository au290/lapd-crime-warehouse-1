import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor  # [NEW] Import RF
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine
import holidays
import os

# --- CONFIG ---
VALID_END_DATE = '2023-12-31' 

def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

def evaluate(y_true, y_pred, model_name):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    print(f"   📊 {model_name:<25} | MAE: {mae:.2f} | RMSE: {rmse:.2f}")
    return {'Model': model_name, 'MAE': mae, 'RMSE': rmse}

# --- FEATURE ENGINEERING ---
us_holidays = holidays.US()

def create_features_with_lags(df, lag_days=[1, 7, 14, 30], rolling_days=[7, 30]):
    X = df.copy()
    
    # Calendar
    X['day_of_week'] = X['ds'].dt.dayofweek
    X['quarter'] = X['ds'].dt.quarter
    X['month'] = X['ds'].dt.month
    X['year'] = X['ds'].dt.year
    X['day_of_year'] = X['ds'].dt.dayofyear
    X['is_weekend'] = X['ds'].dt.dayofweek.isin([5, 6]).astype(int)
    X['is_holiday'] = X['ds'].apply(lambda x: 1 if x in us_holidays else 0)
    
    # Cyclical
    X['day_of_week_sin'] = np.sin(2 * np.pi * X['ds'].dt.dayofweek / 7)
    X['day_of_week_cos'] = np.cos(2 * np.pi * X['ds'].dt.dayofweek / 7)
    X['month_sin'] = np.sin(2 * np.pi * X['ds'].dt.month / 12)
    X['month_cos'] = np.cos(2 * np.pi * X['ds'].dt.month / 12)

    # Lags
    for lag in lag_days:
        X[f'lag_{lag}'] = X['y'].shift(lag)

    # Rolling
    for window in rolling_days:
        X[f'rolling_mean_{window}'] = X['y'].shift(1).rolling(window=window).mean()
        X[f'rolling_std_{window}'] = X['y'].shift(1).rolling(window=window).std()

    return X

def recursive_predict(model, train_data, test_days, lag_days, rolling_days):
    history = train_data.copy()
    predictions = []
    
    last_date = history['ds'].max()
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=test_days)
    
    for date in future_dates:
        row = pd.DataFrame({'ds': [date], 'y': [np.nan]}) 
        temp_df = pd.concat([history, row], ignore_index=True)
        features_df = create_features_with_lags(temp_df, lag_days, rolling_days)
        
        X_today = features_df.iloc[[-1]].drop(columns=['ds', 'y'])
        
        pred = model.predict(X_today)[0]
        predictions.append(pred)
        
        row['y'] = pred
        history = pd.concat([history, row], ignore_index=True)
        
    return np.array(predictions)

def run_model_battle():
    engine = get_db_engine()
    print("🥊 STARTING MODEL BATTLE: XGBoost vs Random Forest")
    
    # 1. LOAD DATA
    query = """
        SELECT date_occ as ds, count(*) as y
        FROM warehouse.fact_crime
        WHERE date_occ IS NOT NULL
        GROUP BY date_occ
        ORDER BY date_occ
    """
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])
    
    # 2. FILTER & SPLIT
    df_clean = df[df['ds'] <= VALID_END_DATE].copy()
    test_days = 90
    train_df = df_clean.iloc[:-test_days].copy()
    test_df = df_clean.iloc[-test_days:].copy()
    
    print(f"   Training: {len(train_df)} | Testing: {len(test_df)}")
    print("-" * 70)

    # Shared Config
    lag_days = [1, 7, 14, 30] 
    rolling_days = [7, 30]
    
    # Prepare Training Data (Drop NaNs from lags)
    df_train_features = create_features_with_lags(train_df, lag_days, rolling_days)
    df_train_features.dropna(inplace=True)
    
    X_train = df_train_features.drop(columns=['ds', 'y'])
    y_train = df_train_features['y']

    # ==========================================
    # 1. XGBOOST
    # ==========================================
    print("🚀 Training XGBoost...")
    xgb_model = XGBRegressor(n_estimators=1000, learning_rate=0.02, max_depth=5, n_jobs=-1)
    xgb_model.fit(X_train, y_train)
    
    pred_xgb = recursive_predict(xgb_model, train_df, test_days, lag_days, rolling_days)
    evaluate(test_df['y'], pred_xgb, "XGBoost (Recursive)")

    # ==========================================
    # 2. RANDOM FOREST [NEW]
    # ==========================================
    print("🌲 Training Random Forest...")
    rf_model = RandomForestRegressor(n_estimators=500, max_depth=10, n_jobs=-1, random_state=42)
    rf_model.fit(X_train, y_train)
    
    pred_rf = recursive_predict(rf_model, train_df, test_days, lag_days, rolling_days)
    evaluate(test_df['y'], pred_rf, "Random Forest (Recursive)")
    
    print("-" * 70)

if __name__ == "__main__":
    run_model_battle()