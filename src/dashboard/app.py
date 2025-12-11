import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import joblib
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta
import holidays # IMPORTANT: Add 'holidays' to requirements.txt

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="LAPD Crime Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. WAREHOUSE CONNECTION ---
@st.cache_resource
def get_db_engine():
    db_conn = os.getenv("WAREHOUSE_CONN", "postgresql+psycopg2://admin:admin_password@warehouse:5432/lapd_warehouse")
    return create_engine(db_conn)

# --- 3. XGBOOST HELPER & LOADER ---
def create_features(dates):
    """Generates features for future dates (X_test)"""
    df = pd.DataFrame({'ds': dates})
    us_holidays = holidays.US()
    
    X = df.copy()
    X['day_of_week'] = X['ds'].dt.dayofweek
    X['quarter'] = X['ds'].dt.quarter
    X['month'] = X['ds'].dt.month
    X['year'] = X['ds'].dt.year
    X['day_of_year'] = X['ds'].dt.dayofyear
    X['week_of_year'] = X['ds'].dt.isocalendar().week.astype(int)
    X['is_holiday'] = X['ds'].apply(lambda x: 1 if x in us_holidays else 0)
    
    return X.drop(columns=['ds'], errors='ignore')

@st.cache_data(ttl=3600)
def load_forecast_model():
    engine = get_db_engine()
    try:
        # Fetch the latest XGBoost model
        query = text("""
            SELECT model_blob 
            FROM warehouse.model_registry 
            WHERE model_name = 'xgboost_crime_v1' 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
        if result and result[0]:
            model = joblib.load(BytesIO(result[0]))
            
            # Forecast Strategy:
            # 1. Identify "Today"
            today = pd.to_datetime('today').normalize()
            
            # 2. Create a range from 6 months ago (to see history overlap) to 3 months ahead
            start_view = today - timedelta(days=180)
            end_view = today + timedelta(days=90)
            future_dates = pd.date_range(start=start_view, end=end_view, freq='D')
            
            # 3. Create Features for these dates
            X_future = create_features(future_dates)
            
            # 4. Predict
            preds = model.predict(X_future)
            
            # 5. Format DataFrame
            forecast = pd.DataFrame({
                'ds': future_dates,
                'yhat': preds
            })
            
            # Artificial Confidence Intervals (XGBoost doesn't provide them natively)
            # We estimate +/- 15% based on our RMSE analysis
            forecast['yhat_lower'] = forecast['yhat'] * 0.85
            forecast['yhat_upper'] = forecast['yhat'] * 1.15
            
            return forecast
            
    except Exception as e:
        print(f"⚠️ Model Load Error: {e}")
        return pd.DataFrame()
    return pd.DataFrame()

# --- 4. DATA LOADERS ---
def load_filter_options(engine):
    try:
        with engine.connect() as conn:
            dates = pd.read_sql("SELECT min(date_occ), max(date_occ) FROM warehouse.fact_crime", conn).iloc[0]
            areas = pd.read_sql("SELECT DISTINCT area_name FROM warehouse.dim_area ORDER BY 1", conn)
            crimes = pd.read_sql("SELECT DISTINCT crm_cd_desc FROM warehouse.dim_crime ORDER BY 1", conn)
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist()
    except Exception:
        return None, [], []

def query_analytical_data(engine, start_date, end_date, selected_areas, selected_crimes):
    query = """
        SELECT 
            f.date_occ, da.area_name, dc.crm_cd_desc, 
            ds.status_desc, f.vict_age, f.lat, f.lon
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_status ds ON f.status_id = ds.status_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s
        AND f.lat != 0 AND f.lon != 0
    """
    params = {'start': start_date, 'end': end_date}
    if selected_areas:
        query += " AND da.area_name IN %(areas)s"
        params['areas'] = tuple(selected_areas)
    if selected_crimes:
        query += " AND dc.crm_cd_desc IN %(crimes)s"
        params['crimes'] = tuple(selected_crimes)

    # UNLIMITED DATA
    return pd.read_sql(query, engine, params=params)

def query_trend_data(engine, start_date, end_date, selected_areas, selected_crimes):
    query = """
        SELECT f.date_occ, count(*) as "Jumlah"
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s
    """
    params = {'start': start_date, 'end': end_date}
    if selected_areas:
        query += " AND da.area_name IN %(areas)s"
        params['areas'] = tuple(selected_areas)
    if selected_crimes:
        query += " AND dc.crm_cd_desc IN %(crimes)s"
        params['crimes'] = tuple(selected_crimes)
        
    query += " GROUP BY f.date_occ ORDER BY f.date_occ"
    return pd.read_sql(query, engine, params=params)

def query_kpi_stats(engine, start_date, end_date):
    query = """
        SELECT count(*) as total_cases, avg(vict_age) as avg_age
        FROM warehouse.fact_crime
        WHERE date_occ BETWEEN %(start)s AND %(end)s
    """
    return pd.read_sql(query, engine, params={'start': start_date, 'end': end_date})

# --- 5. MAIN UI ---
def main():
    engine = get_db_engine()
    
    st.sidebar.title("🔍 Filter Panel")
    dates, area_opts, crime_opts = load_filter_options(engine)
    
    if dates is None or pd.isnull(dates[0]):
        st.error("⚠️ Failed to connect to Warehouse or Data is empty.")
        st.stop()

    min_d, max_d = dates
    default_start = max_d - timedelta(days=30)
    
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [default_start, max_d])
    sel_areas = st.sidebar.multiselect("Wilayah (Area)", area_opts)
    sel_crimes = st.sidebar.multiselect("Jenis Kejahatan", crime_opts)
    
    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"Warehouse Mode | Data: {s_date} s/d {e_date}")

    with st.spinner("Processing..."):
        df = query_analytical_data(engine, s_date, e_date, sel_areas, sel_crimes)
        df_trend = query_trend_data(engine, s_date, e_date, sel_areas, sel_crimes)
        kpi_df = query_kpi_stats(engine, s_date, e_date)
        forecast_df = load_forecast_model()

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Cases", f"{kpi_df['total_cases'][0]:,}")
    c2.metric("Avg Age", f"{round(kpi_df['avg_age'][0] or 0)} Yrs")
    top_area = df['area_name'].mode()[0] if not df.empty else "-"
    c3.metric("Hotspot", top_area)
    top_crime = df['crm_cd_desc'].mode()[0] if not df.empty else "-"
    c4.metric("Top Crime", top_crime[:15]+"..." if len(top_crime)>15 else top_crime)

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Geospatial", "📈 Trends", "📋 Raw Data", "🤖 AI Forecast"])

    with tab1:
        st.subheader("Geospatial Distribution")
        if not df.empty:
            # SAMPLING 20%
            map_df = df.sample(frac=0.2, random_state=42)
            st.caption(f"ℹ️ Visualizing {len(map_df):,} points (20% Sample) on CartoDB Dark.")
            
            st.pydeck_chart(pdk.Deck(
                map_style=pdk.map_styles.CARTO_DARK,
                initial_view_state=pdk.ViewState(
                    latitude=34.05, longitude=-118.24, zoom=9.5, pitch=50
                ),
                layers=[
                    pdk.Layer("HeatmapLayer", map_df, get_position=["lon", "lat"], opacity=0.6, radiusPixels=30),
                    pdk.Layer("ScatterplotLayer", map_df, get_position=["lon", "lat"], get_fill_color=[200, 30, 0, 160], get_radius=30, pickable=True)
                ],
                tooltip={"html": "<b>Area:</b> {area_name}<br><b>Type:</b> {crm_cd_desc}"}
            ))
        else:
            st.info("No geospatial data found.")

    with tab2:
        st.subheader("Historical Trends (Smoothed)")
        if not df_trend.empty:
            # SMOOTHING (7-Day Rolling Average)
            df_trend['MA_7'] = df_trend['Jumlah'].rolling(window=7, min_periods=1).mean()
            
            fig = px.area(df_trend, x='date_occ', y='MA_7', template="plotly_dark", labels={'MA_7': 'Daily Cases (7-Day Avg)'})
            fig.update_traces(line_shape='spline', line_width=2, hovertemplate='<b>Date:</b> %{x}<br><b>Avg Cases:</b> %{y:.1f}')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data found.")

    with tab3:
        st.dataframe(df.head(1000), use_container_width=True)

    with tab4:
        st.subheader("🤖 AI Forecast (XGBoost)")
        st.markdown("> **Model:** XGBoost Regressor (Optimized) | **Features:** Cyclical Calendar + Holidays")
        
        if not forecast_df.empty:
            fig = px.line(forecast_df, x='ds', y='yhat', template="plotly_dark", labels={'ds': 'Date', 'yhat': 'Predicted Cases'})
            fig.update_traces(line_color='#00CC96', line_width=3)
            # Add Confidence Interval Areas
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_lower'], mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip')
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_upper'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(0, 204, 150, 0.2)', showlegend=False, hoverinfo='skip')
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Model not found in 'warehouse.model_registry'. Run the training pipeline!")

if __name__ == "__main__":
    main()