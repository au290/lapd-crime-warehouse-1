import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import joblib
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta

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

# --- 3. LOAD MODEL (FROM WAREHOUSE) ---
@st.cache_data(ttl=3600)
def load_forecast_model():
    engine = get_db_engine()
    try:
        # [FIX] Read from 'warehouse' schema
        query = text("""
            SELECT model_blob 
            FROM warehouse.model_registry 
            WHERE model_name = 'prophet_crime_v1' 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
        if result and result[0]:
            m = joblib.load(BytesIO(result[0]))
            
            today = pd.to_datetime('today')
            future = m.make_future_dataframe(periods=30)
            forecast = m.predict(future)
            
            start_view = today - timedelta(days=60)
            mask = forecast['ds'] >= start_view
            
            return forecast.loc[mask, ['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    except Exception as e:
        print(f"⚠️ Model Load Error: {e}")
        return pd.DataFrame()
    return pd.DataFrame()

# --- 4. DATA LOADERS (SQL) ---
def load_filter_options(engine):
    try:
        # [FIX] Read from 'warehouse' schema
        with engine.connect() as conn:
            dates = pd.read_sql("SELECT min(date_occ), max(date_occ) FROM warehouse.fact_crime", conn).iloc[0]
            areas = pd.read_sql("SELECT DISTINCT area_name FROM warehouse.dim_area ORDER BY 1", conn)
            crimes = pd.read_sql("SELECT DISTINCT crm_cd_desc FROM warehouse.dim_crime ORDER BY 1", conn)
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist()
    except Exception:
        return None, [], []

# --- 5. ANALYTICAL QUERIES (SQL) ---
def query_analytical_data(engine, start_date, end_date, selected_areas, selected_crimes):
    # [FIX] Read from 'warehouse' schema
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

    query += " LIMIT 50000"
    return pd.read_sql(query, engine, params=params)

def query_trend_data(engine, start_date, end_date, selected_areas, selected_crimes):
    # [FIX] Read from 'warehouse' schema
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
    # [FIX] Read from 'warehouse' schema
    query = """
        SELECT count(*) as total_cases, avg(vict_age) as avg_age
        FROM warehouse.fact_crime
        WHERE date_occ BETWEEN %(start)s AND %(end)s
    """
    return pd.read_sql(query, engine, params={'start': start_date, 'end': end_date})

# --- 6. UI MAIN ---
def main():
    engine = get_db_engine()
    
    st.sidebar.title("🔍 Filter Panel")
    dates, area_opts, crime_opts = load_filter_options(engine)
    
    if dates is None:
        st.error("Gagal terhubung ke Data Warehouse (Postgres).")
        st.stop()

    min_d, max_d = dates
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [min_d, max_d])
    sel_areas = st.sidebar.multiselect("Wilayah (Area)", area_opts)
    sel_crimes = st.sidebar.multiselect("Jenis Kejahatan", crime_opts)
    
    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"Warehouse Mode (Staging -> Warehouse) | Data: {s_date} s/d {e_date}")

    with st.spinner("Executing SQL Queries..."):
        df = query_analytical_data(engine, s_date, e_date, sel_areas, sel_crimes)
        df_trend = query_trend_data(engine, s_date, e_date, sel_areas, sel_crimes)
        kpi_df = query_kpi_stats(engine, s_date, e_date)
        forecast_df = load_forecast_model()

    if df.empty:
        st.warning("Tidak ada data ditemukan.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Kasus", f"{kpi_df['total_cases'][0]:,}")
    c2.metric("Avg Umur", f"{round(kpi_df['avg_age'][0] or 0)} Thn")
    top_area = df['area_name'].mode()[0] if not df.empty else "-"
    c3.metric("Hotspot Area", top_area)
    top_crime = df['crm_cd_desc'].mode()[0] if not df.empty else "-"
    c4.metric("Top Crime", top_crime[:15]+"..." if len(top_crime)>15 else top_crime)

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Peta", "📈 Grafik", "📋 Data", "🤖 AI Forecast"])

    with tab1:
        st.subheader("Geospatial Distribution")
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(latitude=34.05, longitude=-118.24, zoom=9.5),
            layers=[
                pdk.Layer("HeatmapLayer", df, get_position=["lon", "lat"], opacity=0.8, radiusPixels=40),
                pdk.Layer("ScatterplotLayer", df, get_position=["lon", "lat"], get_color="[200, 30, 0, 160]", get_radius=30)
            ]
        ))

    with tab2:
        if not df_trend.empty:
            st.plotly_chart(px.area(df_trend, x='date_occ', y='Jumlah', template="plotly_dark"), use_container_width=True)

    with tab3:
        st.dataframe(df.head(1000), use_container_width=True)

    with tab4:
        st.subheader("🤖 Prophet Forecasting")
        if not forecast_df.empty:
            fig = px.line(forecast_df, x='ds', y='yhat', template="plotly_dark")
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_lower'], mode='lines', line=dict(width=0), showlegend=False)
            fig.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_upper'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(0,100,80,0.2)', showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Model belum tersedia di 'warehouse.model_registry'.")

if __name__ == "__main__":
    main()