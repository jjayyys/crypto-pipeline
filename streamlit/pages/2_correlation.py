# streamlit/pages/2_correlation.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st
import plotly.express as px
from connection import get_connection, get_gold_schema

st.title("🔗 Correlation Analysis")

con         = get_connection()
gold_schema = get_gold_schema(con)

if not gold_schema:
    st.warning("⚠️ ยังไม่มีข้อมูล กรุณารัน load_local_data.py และ dbt run")
    st.stop()

df = con.execute(f"""
    SELECT
        d.date,
        dc.symbol,
        f.daily_return_pct
    FROM {gold_schema}.fact_price_ohlcv f
    JOIN {gold_schema}.dim_coin dc ON f.coin_sk = dc.coin_sk
    JOIN {gold_schema}.dim_date d  ON f.date_id = d.date_id
    WHERE f.daily_return_pct IS NOT NULL
    ORDER BY d.date
""").df()

if df.empty:
    st.warning("No data available")
    st.stop()

pivot = df.pivot(
    index="date",
    columns="symbol",
    values="daily_return_pct",
)
corr = pivot.corr()

st.subheader("Return Correlation Heatmap")
fig = px.imshow(
    corr,
    text_auto=".2f",
    color_continuous_scale="RdBu_r",
    template="plotly_dark",
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("""
> **+1** = เคลื่อนไหวทิศทางเดียวกัน |
> **-1** = สวนทาง |
> **0** = ไม่สัมพันธ์กัน
""")

# ── Rolling Correlation ───────────────────────────────────────
st.subheader("30-Day Rolling Correlation vs BTC")

if "BTC" in pivot.columns:
    rolling = pivot.rolling(30).corr()["BTC"].reset_index()
    rolling = rolling[rolling["symbol"] != "BTC"]

    fig2 = px.line(
        rolling, x="date", y="BTC",
        color="symbol",
        template="plotly_dark",
        labels={"BTC": "Correlation with BTC"},
    )
    fig2.add_hline(y=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)