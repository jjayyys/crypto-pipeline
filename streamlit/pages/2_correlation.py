# streamlit/pages/2_correlation.py
import streamlit as st
import duckdb
import plotly.express as px
import os

st.title("🔗 Correlation Analysis")

DB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse/crypto.duckdb")

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()

# ── Pivot: daily return per coin ──────────────────────────────────────────────
df = con.execute("""
    SELECT
        d.date,
        dc.symbol,
        f.daily_return_pct
    FROM gold.fact_price_ohlcv f
    JOIN gold.dim_coin  dc ON f.coin_sk = dc.coin_sk
    JOIN gold.dim_date  d  ON f.date_id = d.date_id
    WHERE f.daily_return_pct IS NOT NULL
    ORDER BY d.date
""").df()

pivot = df.pivot(index="date", columns="symbol", values="daily_return_pct")
corr  = pivot.corr()

st.subheader("Return Correlation Heatmap")
fig = px.imshow(
    corr,
    text_auto=".2f",
    color_continuous_scale="RdBu_r",
    aspect="auto",
    template="plotly_dark",
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("""
> **Interpretation**: ค่าใกล้ **+1** = เคลื่อนไหวไปทิศทางเดียวกัน,
> ค่าใกล้ **-1** = เคลื่อนไหวสวนทางกัน,
> ค่าใกล้ **0** = ไม่มีความสัมพันธ์
""")