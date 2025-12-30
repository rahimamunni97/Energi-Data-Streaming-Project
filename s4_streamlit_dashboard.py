import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2

load_dotenv()

# --------------------------------------------------
# Database connection + query
# --------------------------------------------------
def get_data():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "55432"),
        database=os.getenv("DB_NAME", "energi_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )

    query = """
    SELECT
        r.timestamp,
        r.price_area,
        (r.payload->>'SpotPriceEUR')::float AS price,
        q.quality_status,
        q.reason
    FROM energi_records r
    JOIN energi_quality q ON r.event_id = q.event_id
    ORDER BY r.timestamp DESC
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


# --------------------------------------------------
# Streamlit UI
# --------------------------------------------------
st.set_page_config(page_title="Energi Data Quality Dashboard", layout="wide")

st.title("Energi Data Quality Dashboard")

st.markdown("""
This dashboard shows **cleaned energy price data** together with a
**data quality decision**.  
Energy service companies can **choose which data to trust and use**.
""")

# Refresh button
if st.button("🔄 Refresh data"):
    st.rerun()

# Load data
df = get_data()

if df.empty:
    st.warning("No data available yet. Make sure Kafka producer & consumer are running.")
    st.stop()

# --------------------------------------------------
# QUALITY FILTER (THIS IS WHAT YOU WANTED)
# --------------------------------------------------
st.subheader("✅ Select data usability")

selected_quality = st.multiselect(
    "Choose which data you want to include:",
    options=["USABLE", "RISKY", "DENIED"],
    default=["USABLE"]
)

filtered_df = df[df["quality_status"].isin(selected_quality)]

# --------------------------------------------------
# EXPLANATION PANEL
# --------------------------------------------------
st.markdown("""
### Quality meaning
- 🟢 **USABLE** → Safe for reporting and analysis  
- 🟡 **RISKY** → Passed validation but requires analyst review  
- 🔴 **DENIED** → Should not be used (bad or unreliable data)
""")

# --------------------------------------------------
# DATA TABLE
# --------------------------------------------------
st.subheader("📊 Cleaned data with quality decision")

st.dataframe(
    filtered_df,
    use_container_width=True
)

# --------------------------------------------------
# SIMPLE SUMMARY COUNTS (VERY IMPRESSIVE)
# --------------------------------------------------
st.subheader("📈 Data quality summary")

summary = (
    df.groupby("quality_status")
    .size()
    .reset_index(name="record_count")
)

st.bar_chart(summary.set_index("quality_status"))
