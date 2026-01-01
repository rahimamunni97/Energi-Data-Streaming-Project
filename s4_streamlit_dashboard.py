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
    page_title="EnergiFlow: Analysis of Electricity Price and Production Dynamics",
    layout="wide"
)

# ---------- Minimal academic styling ----------
st.markdown("""
<style>
h1 { font-weight: 700; }
h2, h3 { font-weight: 600; }
section[data-testid="stDataFrame"] {
    border: 1px solid #2a2a2a;
    border-radius: 6px;
    padding: 6px;
}
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# TITLE & CONTEXT
# --------------------------------------------------
st.title("EnergiFlow: Analysis of Electricity Price and Production Dynamics")

st.caption(
    "A real-time streaming analytics prototype integrating electricity spot prices "
    "and declared production data for exploratory decision-oriented analysis."
)

st.markdown("""
### Project Context
This dashboard presents the output of a real-time data streaming pipeline that
integrates electricity spot prices (Elspot) with declared electricity production
data. The objective is to explore how market prices and production volumes interact
at regional level, and how such information may support operational monitoring and
analytical decision-making.
""")

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
# LOAD DATA
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
        LIMIT 20;
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
        LIMIT 50;
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
              AND timestamp >= CURRENT_DATE - INTERVAL '2 months'
            GROUP BY date, area
        ),
        price AS (
            SELECT
                timestamp::date AS date,
                price_area AS area,
                AVG(spot_price_eur) AS avg_price
            FROM energi_records
            WHERE spot_price_eur IS NOT NULL
              AND timestamp >= CURRENT_DATE - INTERVAL '2 months'
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
        ORDER BY p.date;
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
# RECENT PRICE DATA
# --------------------------------------------------
st.subheader("Recent Electricity Spot Price Observations")
st.markdown(
    "The table below shows the most recent electricity spot price observations "
    "ingested from the Elspot data stream."
)

st.dataframe(
    elspot_df.rename(columns={
        "date": "Date",
        "area": "Area",
        "spot_price": "Spot Price (EUR)",
        "source": "Source"
    }),
    use_container_width=True
)

# --------------------------------------------------
# RECENT PRODUCTION DATA (CLEANED)
# --------------------------------------------------
st.subheader("Recent Declared Electricity Production Records")
st.markdown(
    "Declared electricity production values aggregated by production type and area."
)

decl_clean = (
    decl_df
    .groupby(["date", "area", "production_type"], as_index=False)
    .agg({"production_mwh": "sum"})
)

st.dataframe(
    decl_clean.rename(columns={
        "date": "Date",
        "area": "Area",
        "production_type": "Production Type",
        "production_mwh": "Production (MWh)"
    }),
    use_container_width=True
)

# --------------------------------------------------
# DAILY AGGREGATION
# --------------------------------------------------
st.subheader("Daily Price–Production Aggregation")
st.markdown(
    "Daily aggregated values combining total declared production and "
    "average spot prices for each bidding area."
)

st.dataframe(
    daily_df.rename(columns={
        "date": "Date",
        "area": "Area",
        "total_production": "Total Production (MWh)",
        "avg_price": "Average Spot Price (EUR)"
    }),
    use_container_width=True
)

# --------------------------------------------------
# TOTAL PRODUCTION SUMMARY
# --------------------------------------------------
st.subheader("Total Declared Production by Area")

total_prod_df = (
    daily_df
    .groupby("area", as_index=False)["total_production"]
    .sum()
    .rename(columns={"total_production": "Total Production (MWh)"})
)

st.dataframe(total_prod_df, use_container_width=True)

# --------------------------------------------------
# INTERPRETATION OF SIGNALS
# --------------------------------------------------
st.subheader("Interpretation of Price–Production Signals")
st.markdown(
    "This section provides a qualitative interpretation of observed price "
    "and production patterns. The interpretations are heuristic and intended "
    "to support analytical reasoning rather than prescribe actions."
)

global_avg_price = daily_df["avg_price"].mean()
global_avg_production = daily_df["total_production"].mean()

decision_rows = []

for _, row in total_prod_df.iterrows():
    area = row["area"]
    area_df = daily_df[daily_df["area"] == area]

    avg_price = area_df["avg_price"].mean()
    avg_prod = area_df["total_production"].mean()

    if avg_price < global_avg_price and avg_prod > global_avg_production:
        interpretation = "Favorable production conditions"
        pattern = "Lower-than-average prices with relatively strong production"
    elif avg_price > global_avg_price and avg_prod < global_avg_production:
        interpretation = "Unfavorable production conditions"
        pattern = "Higher-than-average prices with weaker production"
    else:
        interpretation = "Neutral / monitor"
        pattern = "No clear dominance between price and production signals"

    decision_rows.append([
        area,
        round(avg_price, 2),
        round(avg_prod, 2),
        interpretation,
        pattern
    ])

decision_df = pd.DataFrame(
    decision_rows,
    columns=[
        "Area",
        "Avg Spot Price (EUR)",
        "Avg Production (MWh)",
        "Market Interpretation",
        "Observed Pattern"
    ]
)

st.dataframe(decision_df, use_container_width=True)

# --------------------------------------------------
# PRICE vs PRODUCTION VISUALIZATION
# --------------------------------------------------
st.subheader("Relationship Between Price and Production")

st.markdown(
    "The figure below illustrates the relationship between average daily spot prices "
    "and total declared production for the selected bidding area."
)

selected_area = st.selectbox(
    "Select bidding area",
    sorted(daily_df["area"].unique())
)

graph_df = daily_df[daily_df["area"] == selected_area]

if graph_df["date"].nunique() < 3:
    st.info(
        "The current visualization is based on a limited temporal range. "
        "As additional daily data becomes available, trends and correlations "
        "will become more apparent."
    )

min_date = daily_df["date"].min()
max_date = daily_df["date"].max()

st.caption(
    f"Data shown for period: {min_date} to {max_date}"
)

# ---------------- Dual-axis visualization ----------------

base = alt.Chart(graph_df).encode(
    x=alt.X("date:T", title="Date")
)

if graph_df["date"].nunique() == 1:
    # ---------- Single data point → use points ----------
    price_layer = base.mark_point(
        color="#4FA3FF",
        size=120
    ).encode(
        y=alt.Y(
            "avg_price:Q",
            title="Average Spot Price (EUR)",
            axis=alt.Axis(titleColor="#4FA3FF")
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("avg_price:Q", title="Avg Price (EUR)", format=".2f")
        ]
    )

    production_layer = base.mark_point(
        color="#FFA726",
        size=120
    ).encode(
        y=alt.Y(
            "total_production:Q",
            title="Total Production (MWh)",
            axis=alt.Axis(titleColor="#FFA726")
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("total_production:Q", title="Production (MWh)", format=".2f")
        ]
    )

else:
    # ---------- Multiple data points → use lines ----------
    price_layer = base.mark_line(
        color="#4FA3FF",
        strokeWidth=3,
        point=True
    ).encode(
        y=alt.Y(
            "avg_price:Q",
            title="Average Spot Price (EUR)",
            axis=alt.Axis(titleColor="#4FA3FF")
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("avg_price:Q", title="Avg Price (EUR)", format=".2f")
        ]
    )

    production_layer = base.mark_line(
        color="#FFA726",
        strokeWidth=3,
        point=True
    ).encode(
        y=alt.Y(
            "total_production:Q",
            title="Total Production (MWh)",
            axis=alt.Axis(titleColor="#FFA726")
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("total_production:Q", title="Production (MWh)", format=".2f")
        ]
    )

chart = alt.layer(
    price_layer,
    production_layer
).resolve_scale(
    y="independent"
).properties(
    height=420
)

st.altair_chart(chart, use_container_width=True)

# --------------------------------------------------
# LIMITATIONS & FUTURE WORK
# --------------------------------------------------
st.subheader("Limitations and Future Work")
st.markdown("""
- The current analysis is based on daily aggregation and does not capture intra-day
  price volatility.
- Interpretation rules are heuristic and intended for exploratory analysis.
- Future work may include longer historical windows, correlation analysis, and
  predictive or forecasting models.
""")
