import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import joblib
import xgboost as xgb
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

# --- 3. HELPER: DYNAMIC SQL BUILDER ---
def build_query_filters(sel_areas, sel_crimes, sel_weapons, sel_premises):
    clauses = []
    params = {}
    
    if sel_areas:
        clauses.append("da.area_name IN %(areas)s")
        params['areas'] = tuple(sel_areas)
    if sel_crimes:
        clauses.append("dc.crm_cd_desc IN %(crimes)s")
        params['crimes'] = tuple(sel_crimes)
    if sel_weapons:
        clauses.append("dw.weapon_desc IN %(weapons)s")
        params['weapons'] = tuple(sel_weapons)
    if sel_premises:
        clauses.append("dp.premis_desc IN %(premises)s")
        params['premises'] = tuple(sel_premises)
        
    where_sql = " AND ".join(clauses)
    if where_sql:
        where_sql = " AND " + where_sql
        
    return where_sql, params

# --- 4. OPTIMIZED DATA LOADERS ---

def get_kpi_metrics(engine, start_date, end_date, where_sql, params):
    """100% DATA ACCURACY. Calculates aggregations in the Database."""
    q_params = params.copy()
    q_params.update({'start': start_date, 'end': end_date})

    # 1. Base Stats
    sql_base = f"""
        SELECT count(*) as total, avg(vict_age) as avg_age 
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s {where_sql}
    """
    df_base = pd.read_sql(sql_base, engine, params=q_params)

    # 2. Hotspot
    sql_area = f"""
        SELECT da.area_name 
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s {where_sql}
        GROUP BY da.area_name 
        ORDER BY count(*) DESC LIMIT 1
    """
    try:
        top_area = pd.read_sql(sql_area, engine, params=q_params).iloc[0,0]
    except:
        top_area = "-"

    # 3. Top Weapon
    sql_weapon = f"""
        SELECT dw.weapon_desc 
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s {where_sql}
        GROUP BY dw.weapon_desc 
        ORDER BY count(*) DESC LIMIT 1
    """
    try:
        top_weapon = pd.read_sql(sql_weapon, engine, params=q_params).iloc[0,0]
    except:
        top_weapon = "-"
        
    return df_base.iloc[0]['total'], df_base.iloc[0]['avg_age'], top_area, top_weapon

@st.cache_data(ttl=300)
def get_map_data(_engine, start_date, end_date, where_sql, params):
    """10% DATA SAMPLING for Map with LEFT JOIN."""
    q_params = params.copy()
    q_params.update({'start': start_date, 'end': end_date})
    
    query = f"""
        SELECT f.lat, f.lon, da.area_name, dc.crm_cd_desc
        FROM warehouse.fact_crime f TABLESAMPLE BERNOULLI(10) 
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s 
        AND f.lat != 0 AND f.lon != 0
        {where_sql}
    """
    return pd.read_sql(query, _engine, params=q_params)

def get_trend_data(engine, start_date, end_date, where_sql, params):
    """100% DATA ACCURACY for Trends."""
    q_params = params.copy()
    q_params.update({'start': start_date, 'end': end_date})
    
    query = f"""
        SELECT date_occ, count(*) as "Jumlah"
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        WHERE date_occ BETWEEN %(start)s AND %(end)s
        {where_sql}
        GROUP BY date_occ ORDER BY date_occ
    """
    return pd.read_sql(query, engine, params=q_params)

def get_raw_data_preview(engine, start_date, end_date, where_sql, params):
    q_params = params.copy()
    q_params.update({'start': start_date, 'end': end_date})
    
    query = f"""
        SELECT f.dr_no, f.date_occ, da.area_name, dc.crm_cd_desc, dw.weapon_desc, ds.status_desc
        FROM warehouse.fact_crime f
        LEFT JOIN warehouse.dim_area da ON f.area_id = da.area_id
        LEFT JOIN warehouse.dim_crime dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN warehouse.dim_weapon dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN warehouse.dim_premise dp ON f.premis_id = dp.premis_id
        LEFT JOIN warehouse.dim_status ds ON f.status_id = ds.status_id
        WHERE f.date_occ BETWEEN %(start)s AND %(end)s
        {where_sql}
        ORDER BY f.date_occ DESC
        LIMIT 1000
    """
    return pd.read_sql(query, engine, params=q_params)

def get_model_metrics(engine):
    """Fetches the latest performance for EACH model type."""
    try:
        # Uses DISTINCT ON to get the most recent run for each model_name
        query = """
            SELECT DISTINCT ON (model_name) 
                model_name, created_at, mae, rmse, training_rows
            FROM warehouse.model_metrics
            ORDER BY model_name, created_at DESC
        """
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

# --- 5. FORECASTING ---
def create_features(dates):
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
    return X[['day_of_week', 'quarter', 'month', 'year', 'day_of_year', 'week_of_year', 'is_holiday']]

# --- src/dashboard/app.py ---

@st.cache_data(ttl=3600)
def load_forecast_model(model_name):
    """Dynamically loads a specific model from the DB."""
    engine = get_db_engine()
    try:
        query = text("SELECT model_blob FROM warehouse.model_registry WHERE model_name = :name ORDER BY created_at DESC LIMIT 1")
        
        with engine.connect() as conn:
            result = conn.execute(query, {"name": model_name}).fetchone()
            
        if result and result[0]:
            model = joblib.load(BytesIO(result[0]))
            
            # [CHANGE] Set start date to TODAY (Real-time forecast)
            today = pd.to_datetime('today').normalize()
            
            # Generate the next 90 days from today
            future_dates = pd.date_range(start=today, periods=90, freq='D')
            
            # Create features for these future dates
            features = create_features(future_dates)
            
            # Predict
            preds = model.predict(features)
            
            forecast = pd.DataFrame({'ds': future_dates, 'yhat': preds})
            
            # Add Simulated Confidence Intervals
            forecast['yhat_lower'] = forecast['yhat'] * 0.85
            forecast['yhat_upper'] = forecast['yhat'] * 1.15
            
            return forecast
            
    except Exception as e:
        return f"Error loading {model_name}: {str(e)}"
        
    return pd.DataFrame()

def load_filter_options(engine):
    try:
        with engine.connect() as conn:
            if not conn.execute(text("SELECT EXISTS (SELECT 1 FROM warehouse.fact_crime)")).scalar():
                return None, [], [], [], []
            
            dates = pd.read_sql("SELECT min(date_occ), max(date_occ) FROM warehouse.fact_crime", conn).iloc[0]
            areas = pd.read_sql("SELECT DISTINCT area_name FROM warehouse.dim_area ORDER BY 1", conn)
            crimes = pd.read_sql("SELECT DISTINCT crm_cd_desc FROM warehouse.dim_crime ORDER BY 1", conn)
            weapons = pd.read_sql("SELECT DISTINCT weapon_desc FROM warehouse.dim_weapon WHERE weapon_desc IS NOT NULL ORDER BY 1", conn)
            premises = pd.read_sql("SELECT DISTINCT premis_desc FROM warehouse.dim_premise WHERE premis_desc IS NOT NULL ORDER BY 1", conn)
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist(), weapons['weapon_desc'].tolist(), premises['premis_desc'].tolist()
    except Exception:
        return None, [], [], [], []

# --- 6. MAIN APP ---
def main():
    engine = get_db_engine()
    
    st.sidebar.title("🔍 Filter Panel")
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    dates, area_opts, crime_opts, weapon_opts, premis_opts = load_filter_options(engine)
    if dates is None or pd.isnull(dates[0]):
        st.error("⚠️ System Offline or Data Empty")
        st.stop()

    min_d, max_d = dates
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [max_d - timedelta(days=30), max_d])
    
    with st.sidebar.expander("📍 Location & Crime Type", expanded=True):
        sel_areas = st.multiselect("Wilayah", area_opts)
        sel_crimes = st.multiselect("Jenis Kejahatan", crime_opts)
    with st.sidebar.expander("🔫 Details", expanded=False):
        sel_weapons = st.multiselect("Senjata", weapon_opts)
        sel_premises = st.multiselect("Lokasi", premis_opts)

    where_sql, params = build_query_filters(sel_areas, sel_crimes, sel_weapons, sel_premises)
    
    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"⚡ Hybrid Mode: KPIs (100% Data) | Map (10% Sample) | Range: {s_date} to {e_date}")

    with st.spinner("Analyzing Warehouse Data..."):
        total_cases, avg_age, top_area, top_weapon = get_kpi_metrics(engine, s_date, e_date, where_sql, params)
        df_trend = get_trend_data(engine, s_date, e_date, where_sql, params)
        map_df = get_map_data(engine, s_date, e_date, where_sql, params)
        raw_df = get_raw_data_preview(engine, s_date, e_date, where_sql, params)
        # forecast_result = load_forecast_model()
        model_metrics_df = get_model_metrics(engine) # Fetch Model Metrics

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Cases", f"{total_cases:,}")
    c2.metric("Avg Age", f"{round(avg_age or 0)} Yrs")
    c3.metric("Hotspot", top_area)
    c4.metric("Top Weapon", str(top_weapon)[:15]+"...")

    st.divider()

    # ADDED "🏆 Model Perf" TAB
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ Geospatial", "📈 Trends", "📋 Raw Data", "🤖 AI Forecast", "🏆 Model Perf"])

    with tab1:
        if not map_df.empty:
            st.caption(f"Visualizing 10% sample ({len(map_df):,} points) for performance.")
            st.pydeck_chart(pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=34.05, longitude=-118.24, zoom=9.5, pitch=50),
                layers=[
                    pdk.Layer("HeatmapLayer", map_df, get_position=["lon", "lat"], opacity=0.6, radiusPixels=30),
                    pdk.Layer("ScatterplotLayer", map_df, get_position=["lon", "lat"], get_fill_color=[200, 30, 0, 160], get_radius=30, pickable=True)
                ],
                tooltip={"html": "<b>Area:</b> {area_name}<br><b>Type:</b> {crm_cd_desc}"}
            ))
        else:
            st.info("No data found for current filters.")

    with tab2:
        if not df_trend.empty:
            df_trend['MA_7'] = df_trend['Jumlah'].rolling(window=7, min_periods=1).mean()
            fig = px.area(df_trend, x='date_occ', y='MA_7', template="plotly_dark", log_y=True)
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.dataframe(raw_df, use_container_width=True)
        st.caption("Showing top 1,000 recent records.")

    with tab4:
        st.subheader("🤖 AI Forecast Playground")
        
        # 1. Selector for Model
        # You can toggle between your Champion (XGBoost) and Challenger (Random Forest)
        model_choice = st.radio(
            "Select Model to Visualize:", 
            ["xgboost_crime_v1", "random_forest_crime_v1"], 
            horizontal=True
        )
        
        # 2. Load the selected model
        forecast_result = load_forecast_model(model_choice)

        # 3. Display Result
        if isinstance(forecast_result, str):
            st.warning(f"⚠️ Could not load '{model_choice}'. It might not be trained yet.")
            st.caption(f"Details: {forecast_result}")
        # ... inside tab4 ...

        elif not forecast_result.empty:
            st.success(f"✅ Showing Forecast using: **{model_choice}**")
            
            # Plot
            fig = px.line(forecast_result, x='ds', y='yhat', template="plotly_dark", title=f"90-Day Crime Forecast ({model_choice})")
            
            # --- [FIX START] Define Colors Correctly ---
            if 'xgboost' in model_choice:
                line_color = '#00CC96'                # Green
                fill_color = 'rgba(0, 204, 150, 0.2)' # Transparent Green
            else:
                line_color = '#EF553B'                # Red
                fill_color = 'rgba(239, 85, 59, 0.2)' # Transparent Red
            # --- [FIX END] ---

            fig.update_traces(line_color=line_color, line_width=3)
            
            # Add Confidence Interval Cloud
            fig.add_scatter(x=forecast_result['ds'], y=forecast_result['yhat_lower'], mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip')
            
            # Use the pre-calculated 'fill_color' variable here
            fig.add_scatter(
                x=forecast_result['ds'], 
                y=forecast_result['yhat_upper'], 
                mode='lines', 
                line=dict(width=0), 
                fill='tonexty', 
                fillcolor=fill_color, 
                showlegend=False, 
                hoverinfo='skip'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"Model '{model_choice}' not found in database. Run the pipeline '3_train_forecasting_model' first.")


    # NEW TAB: MODEL COMPARISON
    with tab5:
        st.subheader("🏆 Champion vs Challenger")

        if not model_metrics_df.empty:
            # 1. Prepare Data for Chart
            # Melt the dataframe to make it suitable for a grouped bar chart
            chart_df = model_metrics_df.melt(
                id_vars=['model_name', 'created_at'], 
                value_vars=['mae', 'rmse'], 
                var_name='Metric', 
                value_name='Error Value'
            )

            # 2. Side-by-Side Bar Chart
            fig = px.bar(
                chart_df, 
                x='Metric', 
                y='Error Value', 
                color='model_name', # This creates the comparison
                barmode='group',
                text_auto='.2f',
                title="Model Error Comparison (Lower is Better)",
                template="plotly_dark",
                color_discrete_map={
                    'xgboost_crime_v1': '#00CC96', # Green
                    'random_forest_crime_v1': '#EF553B' # Red
                }
            )
            st.plotly_chart(fig, use_container_width=True)

            # 3. Detailed Metrics Table
            st.markdown("### 📋 Detailed Stats")

            # Formatting for display
            display_df = model_metrics_df.copy()
            display_df['mae'] = display_df['mae'].map('{:.2f}'.format)
            display_df['rmse'] = display_df['rmse'].map('{:.2f}'.format)
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')

            st.dataframe(
                display_df[['model_name', 'mae', 'rmse', 'training_rows', 'created_at']], 
                use_container_width=True,
                hide_index=True
            )

            st.info("💡 **Tip:** The model with the lower MAE/RMSE is your current Champion.")
        else:
            st.info("No model metrics found. Please run the training pipelines.")

if __name__ == "__main__":
    main()