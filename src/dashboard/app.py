import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import joblib
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta
import holidays

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
            
            today = pd.to_datetime('today').normalize()
            start_view = today - timedelta(days=180)
            end_view = today + timedelta(days=90)
            future_dates = pd.date_range(start=start_view, end=end_view, freq='D')
            
            X_future = create_features(future_dates)
            preds = model.predict(X_future)
            
            forecast = pd.DataFrame({'ds': future_dates, 'yhat': preds})
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
            weapons = pd.read_sql("SELECT DISTINCT weapon_desc FROM warehouse.dim_weapon WHERE weapon_desc IS NOT NULL ORDER BY 1", conn)
            premises = pd.read_sql("SELECT DISTINCT premis_desc FROM warehouse.dim_premise WHERE premis_desc IS NOT NULL ORDER BY 1", conn)
            
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist(), weapons['weapon_desc'].tolist(), premises['premis_desc'].tolist()
    except Exception as e:
        print(f"Filter Load Error: {e}")
        return None, [], [], [], []

def query_analytical_data(engine, start_date, end_date, sel_areas, sel_crimes, sel_weapons, sel_premises):
    query = """
        SELECT 
            f.date_occ, 
            da.area_name, 
            dc.crm_cd_desc, 
            ds.status_desc, 
            dw.weapon_desc,
            dp.premis_desc,
            dt.time_of_day,
            f.vict_age, f.lat, f.lon
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_status ds ON f.status_id = ds.status_id
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        LEFT JOIN warehouse.dim_time dt ON f.time_occ = dt.time_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s
        AND f.lat != 0 AND f.lon != 0
    """
    params = {'start': start_date, 'end': end_date}
    
    if sel_areas:
        query += " AND da.area_name IN %(areas)s"
        params['areas'] = tuple(sel_areas)
    if sel_crimes:
        query += " AND dc.crm_cd_desc IN %(crimes)s"
        params['crimes'] = tuple(sel_crimes)
    if sel_weapons:
        query += " AND dw.weapon_desc IN %(weapons)s"
        params['weapons'] = tuple(sel_weapons)
    if sel_premises:
        query += " AND dp.premis_desc IN %(premises)s"
        params['premises'] = tuple(sel_premises)

    return pd.read_sql(query, engine, params=params)

def query_trend_data(engine, start_date, end_date):
    query = """
        SELECT date_occ, count(*) as "Jumlah"
        FROM warehouse.fact_crime
        WHERE date_occ BETWEEN %(start)s AND %(end)s
        GROUP BY date_occ ORDER BY date_occ
    """
    return pd.read_sql(query, engine, params={'start': start_date, 'end': end_date})

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
    dates, area_opts, crime_opts, weapon_opts, premis_opts = load_filter_options(engine)
    
    if dates is None or pd.isnull(dates[0]):
        st.error("⚠️ Failed to connect to Warehouse or Data is empty.")
        st.stop()

    min_d, max_d = dates
    default_start = max_d - timedelta(days=30)
    
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [default_start, max_d])
    
    with st.sidebar.expander("📍 Location & Crime Type", expanded=True):
        sel_areas = st.multiselect("Wilayah (Area)", area_opts)
        sel_crimes = st.multiselect("Jenis Kejahatan", crime_opts)
        
    with st.sidebar.expander("🔫 Details (Weapon & Premise)", expanded=False):
        sel_weapons = st.multiselect("Senjata Digunakan", weapon_opts)
        sel_premises = st.multiselect("Lokasi Kejadian (Premise)", premis_opts)
    
    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"7-Dimension Star Schema Mode | Data: {s_date} s/d {e_date}")

    with st.spinner("Executing Star Schema Queries..."):
        df = query_analytical_data(engine, s_date, e_date, sel_areas, sel_crimes, sel_weapons, sel_premises)
        df_trend = query_trend_data(engine, s_date, e_date)
        kpi_df = query_kpi_stats(engine, s_date, e_date)
        forecast_df = load_forecast_model()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Cases", f"{kpi_df['total_cases'][0]:,}")
    c2.metric("Avg Age", f"{round(kpi_df['avg_age'][0] or 0)} Yrs")
    
    # Safe Hotspot
    top_area = "-"
    if not df.empty:
        try:
            top_area = df['area_name'].mode()[0]
        except Exception:
            top_area = "-"
    c3.metric("Hotspot", top_area)
    
    # Safe Weapon
    top_weapon = "-"
    if not df.empty:
        try:
            top_weapon = df['weapon_desc'].mode()[0]
        except Exception:
            top_weapon = "-"
            
    c4.metric("Top Weapon", str(top_weapon)[:15]+"...")

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Geospatial", "📈 Trends", "📋 Raw Data", "🤖 AI Forecast"])

    with tab1:
        st.subheader("Geospatial Distribution")
        if not df.empty:
            map_df = df.sample(frac=0.2, random_state=42)
            
            # [FIX CRITICAL] Convert Timestamp to String
            # Pydeck kadang gagal memproses objek Timestamp, jadi kita ubah ke string dulu
            if 'date_occ' in map_df.columns:
                map_df['date_occ'] = map_df['date_occ'].astype(str)
            
            st.caption(f"ℹ️ Visualizing {len(map_df):,} points (20% Sample).")
            
            st.pydeck_chart(pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=34.05, longitude=-118.24, zoom=9.5, pitch=50),
                layers=[
                    pdk.Layer("HeatmapLayer", map_df, get_position=["lon", "lat"], opacity=0.6, radiusPixels=30),
                    pdk.Layer("ScatterplotLayer", map_df, get_position=["lon", "lat"], get_fill_color=[200, 30, 0, 160], get_radius=30, pickable=True)
                ],
                tooltip={"html": "<b>Area:</b> {area_name}<br><b>Type:</b> {crm_cd_desc}<br><b>Weapon:</b> {weapon_desc}"}
            ))
        else:
            st.info("No geospatial data found.")

    with tab2:
        st.subheader("Historical Trends")
        if not df_trend.empty:
            df_trend['MA_7'] = df_trend['Jumlah'].rolling(window=7, min_periods=1).mean()
            fig = px.area(df_trend, x='date_occ', y='MA_7', template="plotly_dark", labels={'MA_7': 'Daily Cases (7-Day Avg)'})
            fig.update_traces(line_shape='spline', line_width=2)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data found.")

    with tab3:
        st.dataframe(df.head(1000), use_container_width=True)

    with tab4:
        st.subheader("🤖 AI Forecast (City-Wide)")
        st.markdown("> **Note:** This model predicts the **Total City-Wide Volume**. Specific weapon/premise filters do not affect this forecast line.")
        if not forecast_df.empty:
            fig = px.line(forecast_df, x='ds', y='yhat', template="plotly_dark", labels={'ds': 'Date', 'yhat': 'Predicted Cases'})
            fig.update_traces(line_color='#00CC96', line_width=3)
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_lower'], mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip')
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_upper'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(0, 204, 150, 0.2)', showlegend=False, hoverinfo='skip')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Model not found.")

if __name__ == "__main__":
    main()