# streamlit/app.py
import streamlit as st
import duckdb
import os
from pathlib import Path

st.set_page_config(
    page_title="Crypto Analytics Platform",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Crypto Analytics Platform")
st.markdown("""
**Data Pipeline**: CoinGecko API → MinIO (Bronze/Silver) → DuckDB (Gold) → Dashboard

Navigate using the sidebar 👈
""")


# ── Quick KPIs from Gold layer ──────────────────────────────────────────────
import duckdb
import os

BASE_DIR = Path(__file__).resolve().parent.parent  # crypto-pipeline/
DB_PATH  = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"

st.caption(f"🗄️ DB Path: `{DB_PATH}`")
st.caption(f"📁 Exists: `{DB_PATH.exists()}`")

@st.cache_resource
def get_connection():
    # สร้าง folder ถ้าไม่มี
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))

try:
    con = get_connection()
    st.success("✅ Connected to DuckDB")
except Exception as e:
    st.error(f"❌ Database Error: {e}")
    st.stop()

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_records = con.execute(
        "SELECT COUNT(*) FROM gold.fact_price_ohlcv"
    ).fetchone()
    st.metric("Total Price Records", f"{total_records:,}")

with col2:
    total_coins = con.execute(
        "SELECT COUNT(*) FROM gold.dim_coin"
    ).fetchone()
    st.metric("Coins Tracked", total_coins)

with col3:
    latest_date = con.execute(
        "SELECT MAX(date) FROM gold.dim_date d "
        "JOIN gold.fact_price_ohlcv f ON d.date_id = f.date_id"
    ).fetchone()
    st.metric("Latest Data Date", str(latest_date))

with col4:
    avg_sentiment = con.execute(
        "SELECT ROUND(AVG(fear_greed_value), 1) FROM gold.fact_price_ohlcv "
        "WHERE fear_greed_value IS NOT NULL"
    ).fetchone()
    st.metric("Avg Fear & Greed", avg_sentiment)