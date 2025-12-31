import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2

# --------------------------------------------------
# BASIC SETUP
# --------------------------------------------------
load_dotenv()

st.set_page_config(
    page_title="EnergiFlow: Price–Production Analytics Platform",
    layout="wide"
)

st.title("⚡ EnergiFlow: Price–Production Analytics Platform")
st.caption(
    "Professional analytics dashboard for electricity price and production decision support"
)

# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "55432"),
        database=os.getenv("DB_NAME", "energi_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )

# --------------------------------------------------
# LOAD DATA (OPTION A – DIRECT TABLE)
# --------------------------------------------------
@st.cache_data
def load_dashboard_data():
    conn = get_connection()
    query = """
    SELECT
        source,
        price_area,
        production_mwh,
        co2_per_kwh,
        created_at::date AS day,
        payload->>'ProductionType' AS production_type,
        (payload->>'SpotPriceEUR')::float AS spot_price_eur
    FROM energi_records
    WHERE price_area IS NOT NULL
    ORDER BY created_at DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data
def load_latest_table_data():
    conn = get_connection()
    query = """
    SELECT
        source,
        price_area,
        production_mwh,
        co2_per_kwh,
        created_at::date AS day,
        payload->>'ProductionType' AS production_type,
        (payload->>'SpotPriceEUR')::float AS spot_price_eur
    FROM energi_records
    WHERE price_area IS NOT NULL AND production_mwh IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_dashboard_data()
table_df = load_latest_table_data()

if df.empty:
    st.error("No data available in energi_records.")
    st.stop()

# --------------------------------------------------
# FILTERS
# --------------------------------------------------
st.subheader("🔎 Filters")

col1, col2 = st.columns(2)

with col1:
    selected_area = st.selectbox(
        "Select Price Area",
        options=sorted(df["price_area"].dropna().unique())
    )

with col2:
    date_range = st.date_input(
        "Select Date Range",
        value=(df["day"].min(), df["day"].max())
    )

# ---- FIX: handle single date vs range ----
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date_range[0] if isinstance(date_range, tuple) else date_range
    end_date = start_date

filtered_df = df[
    (df["price_area"] == selected_area) &
    (df["day"] >= start_date) &
    (df["day"] <= end_date)
]

if filtered_df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# --------------------------------------------------
# PROFESSIONAL DATA TABLE
# --------------------------------------------------
st.subheader("📋 Latest Market Data Table")

table_display_df = table_df.rename(columns={
    "day": "Date",
    "price_area": "Area",
    "production_type": "Production Type",
    "production_mwh": "Production (MWh)",
    "spot_price_eur": "Spot Price (EUR)"
})

st.dataframe(
    table_display_df[
        [
            "Date",
            "Area",
            "Production Type",
            "Production (MWh)",
            "Spot Price (EUR)",
        ]
    ],
    width='stretch'
)

# --------------------------------------------------
# PRICE vs PRODUCTION GRAPH
# --------------------------------------------------
st.subheader("📈 Price vs Production Trend")

chart_df = (
    filtered_df
    .groupby("day", as_index=False)
    .agg({
        "production_mwh": "sum",
        "spot_price_eur": "mean"
    })
    .rename(columns={
        "day": "Date",
        "production_mwh": "Production (MWh)",
        "spot_price_eur": "Spot Price (EUR)"
    })
)

st.line_chart(
    chart_df.set_index("Date")
)

# --------------------------------------------------
# DECISION SUPPORT (EXECUTIVE SUMMARY)
# --------------------------------------------------
st.subheader("🧠 Decision Summary")

avg_price = chart_df["Spot Price (EUR)"].mean(skipna=True)
avg_production = chart_df["Production (MWh)"].mean()

global_avg_price = df["spot_price_eur"].mean(skipna=True)
global_avg_production = df["production_mwh"].mean()

if avg_price < global_avg_price and avg_production > global_avg_production:
    decision = "BUY"
    reason = "Lower-than-average price with strong production availability"
elif avg_price > global_avg_price and avg_production < global_avg_production:
    decision = "AVOID"
    reason = "High prices combined with low production"
else:
    decision = "MONITOR"
    reason = "Mixed price–production signals"

decision_df = pd.DataFrame({
    "Area": [selected_area],
    "Avg Price (EUR)": [round(avg_price, 2)],
    "Avg Production (MWh)": [round(avg_production, 2)],
    "Decision": [decision],
    "Reason": [reason]
})

st.table(decision_df)
