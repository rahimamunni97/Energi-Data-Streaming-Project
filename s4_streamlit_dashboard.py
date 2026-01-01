import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2
import altair as alt

# --------------------------------------------------
# BASIC SETUP
# --------------------------------------------------
load_dotenv()

st.set_page_config(
    page_title="EnergiFlow: Price–Production Analytics Platform",
    layout="wide"
)

st.title("⚡ EnergiFlow: Price–Production Analytics Platform")
st.caption("Separated streams → daily aggregation → decision support")

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
# LOAD RAW DATA
# --------------------------------------------------
@st.cache_data
def load_elspot_data():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            timestamp::date AS date,
            price_area AS area,
            spot_price_eur AS spot_price,
            source
        FROM energi_records
        WHERE spot_price_eur IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 10;
    """, conn)
    conn.close()
    return df


@st.cache_data
def load_declaration_data():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            timestamp::date AS date,
            price_area AS area,
            production_type,
            production_mwh,
            source
        FROM energi_records
        WHERE production_mwh IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 10;
    """, conn)
    conn.close()
    return df


@st.cache_data
def load_daily_aggregated():
    conn = get_connection()
    df = pd.read_sql("""
        WITH production AS (
            SELECT
                timestamp::date AS date,
                price_area AS area,
                SUM(production_mwh) AS total_production
            FROM energi_records
            WHERE production_mwh IS NOT NULL
            GROUP BY date, area
        ),
        price AS (
            SELECT
                timestamp::date AS date,
                price_area AS area,
                AVG(spot_price_eur) AS avg_price
            FROM energi_records
            WHERE spot_price_eur IS NOT NULL
            GROUP BY date, area
        )
        SELECT
            p.date,
            p.area,
            p.total_production,
            pr.avg_price
        FROM production p
        JOIN price pr
          ON p.date = pr.date
         AND p.area = pr.area
        ORDER BY p.date DESC;
    """, conn)
    conn.close()
    return df

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
elspot_df = load_elspot_data()
decl_df = load_declaration_data()
daily_df = load_daily_aggregated()

# --------------------------------------------------
# RAW TABLES
# --------------------------------------------------
st.subheader("📋 Latest Elspot Price Records")
st.dataframe(elspot_df.rename(columns={
    "date": "Date",
    "area": "Area",
    "spot_price": "Spot Price (EUR)",
    "source": "Source"
}), width="stretch")

st.subheader("📋 Latest Declaration Production Records")
st.dataframe(decl_df.rename(columns={
    "date": "Date",
    "area": "Area",
    "production_type": "Production Type",
    "production_mwh": "Production (MWh)",
    "source": "Source"
}), width="stretch")

# --------------------------------------------------
# DAILY AGGREGATED TABLE
# --------------------------------------------------
st.subheader("📊 Daily Price–Production Aggregation")

st.dataframe(daily_df.rename(columns={
    "date": "Date",
    "area": "Area",
    "total_production": "Total Production (MWh)",
    "avg_price": "Avg Spot Price (EUR)"
}), width="stretch")

# --------------------------------------------------
# TOTAL PRODUCTION SUMMARY
# --------------------------------------------------
st.subheader("📦 Total Production Summary (by Area)")

total_prod_df = (
    daily_df
    .groupby("area", as_index=False)["total_production"]
    .sum()
    .rename(columns={"total_production": "Total Production (MWh)"})
)

st.dataframe(total_prod_df, width="stretch")

# --------------------------------------------------
# DECISION SUPPORT TABLE
# --------------------------------------------------
st.subheader("🧠 Decision Support Summary")

global_avg_price = daily_df["avg_price"].mean()
global_avg_production = daily_df["total_production"].mean()

decision_rows = []

for _, row in total_prod_df.iterrows():
    area = row["area"]
    area_df = daily_df[daily_df["area"] == area]

    avg_price = area_df["avg_price"].mean()
    avg_prod = area_df["total_production"].mean()

    if avg_price < global_avg_price and avg_prod > global_avg_production:
        decision = "BUY"
        reason = "Low price with strong production"
    elif avg_price > global_avg_price and avg_prod < global_avg_production:
        decision = "AVOID"
        reason = "High price and weak production"
    else:
        decision = "MONITOR"
        reason = "Mixed market signals"

    decision_rows.append([
        area,
        round(avg_price, 2),
        round(avg_prod, 2),
        decision,
        reason
    ])

decision_df = pd.DataFrame(decision_rows, columns=[
    "Area",
    "Avg Spot Price (EUR)",
    "Avg Production (MWh)",
    "Decision",
    "Reason"
])

st.dataframe(decision_df, width="stretch")

# --------------------------------------------------
# PRICE vs PRODUCTION GRAPH
# --------------------------------------------------
st.subheader("📈 Price vs Production Comparison")

selected_area = st.selectbox(
    "Select Area for Graph",
    sorted(daily_df["area"].unique())
)

graph_df = daily_df[daily_df["area"] == selected_area]

chart_df = graph_df.melt(
    id_vars="date",
    value_vars=["total_production", "avg_price"],
    var_name="Metric",
    value_name="Value"
)

chart = (
    alt.Chart(chart_df)
    .mark_line(point=True, strokeWidth=3)
    .encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("Value:Q"),
        color=alt.Color("Metric:N", title="Metric"),
        tooltip=["date:T", "Metric:N", "Value:Q"]
    )
    .properties(height=420)
)

st.altair_chart(chart, use_container_width=True)
