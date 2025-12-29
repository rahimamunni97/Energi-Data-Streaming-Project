import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2

load_dotenv()

# --- Database connection ---
def get_data():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "55432"),
        database=os.getenv("DB_NAME", "energi_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )
    query = "SELECT * FROM energi_records ORDER BY timestamp DESC LIMIT 50;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- Streamlit app ---
st.set_page_config(page_title="Energi Dashboard", layout="wide")
st.title("Real-Time Energi Data Dashboard")

st.markdown("This dashboard shows the latest energy production and price area data coming from Kafka → PostgreSQL.")

if st.button("Refresh Data"):
    st.rerun()

# Load data
df = get_data()

if df.empty:
    st.warning("No data yet — make sure Kafka Producer & Consumer are running.")
else:
    # Show summary
    st.subheader("Latest Records")
    st.dataframe(df)

    # Show grouped summary
    st.subheader("CO2 per kWh by Price Area")
    co2_summary = df.groupby("price_area")["co2_per_kwh"].mean().reset_index()
    st.bar_chart(co2_summary.set_index("price_area"))


df = df.dropna()

# Remove outliers (example: above 99th percentile)
upper = df['SpotPriceEUR'].quantile(0.99)
df = df[df['SpotPriceEUR'] < upper]

# Convert timestamp
df["HourUTC"] = pd.to_datetime(df["HourUTC"])

# Create new column: Day
df["Day"] = df["HourUTC"].dt.date