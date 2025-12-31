import os
import streamlit as st
import psycopg2
import pandas as pd

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(layout="wide")
st.title("EnergiFlow – Price vs Production Analytics")

# --------------------------------------------------
# Database connection (NO DEFAULTS, FAIL FAST)
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

# --------------------------------------------------
# Load data helpers (RESILIENT)
# --------------------------------------------------
@st.cache_data(ttl=10)
def get_areas():
    try:
        with get_connection() as conn:
            return pd.read_sql(
                """
                SELECT DISTINCT price_area
                FROM public.energi_hourly
                ORDER BY price_area
                """,
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=["price_area"])


@st.cache_data(ttl=10)
def get_data(price_area, hours):
    try:
        with get_connection() as conn:
            return pd.read_sql(
                """
                SELECT
                    hour,
                    price_area,
                    production_type,
                    production_mwh,
                    price_eur
                FROM public.energi_hourly
                WHERE price_area = %s
                  AND hour >= now() - interval '%s hours'
                ORDER BY hour DESC
                LIMIT 10
                """,
                conn,
                params=(price_area, hours),
            )
    except Exception:
        return pd.DataFrame()


# --------------------------------------------------
# UI Controls
# --------------------------------------------------
areas_df = get_areas()

if areas_df.empty:
    st.info("Waiting for data… The pipeline is ready.")
    st.stop()

area = st.selectbox("Select Price Area", areas_df["price_area"])

hours = st.slider(
    "Last N hours",
    min_value=1,
    max_value=72,
    value=24
)

# --------------------------------------------------
# Load main data
# --------------------------------------------------
df = get_data(area, hours)

if df.empty:
    st.warning("No data available for the selected area and time range.")
    st.stop()

# --------------------------------------------------
# Table: Latest Data
# --------------------------------------------------
st.subheader("Latest Energy Data (Last 10 Records)")
st.dataframe(df, use_container_width=True)

# --------------------------------------------------
# Chart: Price vs Production
# --------------------------------------------------
st.subheader("Price vs Production Comparison")

chart_df = df.sort_values("hour").set_index("hour")

st.line_chart(
    chart_df[["production_mwh", "price_eur"]],
    use_container_width=True
)

# --------------------------------------------------
# Decision Table
# --------------------------------------------------
st.subheader("Decision Support Table")

def decision(row):
    if pd.notna(row.price_eur) and pd.notna(row.production_mwh):
        if row.price_eur > 100 and row.production_mwh < 50:
            return "❌ Avoid"
        if row.price_eur < 60:
            return "✅ Good to buy"
    return "⚠️ Risk"

df["Decision"] = df.apply(decision, axis=1)

st.table(
    df[["hour", "price_area", "production_type", "Decision"]]
)
