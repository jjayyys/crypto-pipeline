# streamlit/pages/1_price_analysis.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from connection import get_connection, get_gold_schema

st.title("💹 Price Analysis")

con         = get_connection()
gold_schema = get_gold_schema(con)

if not gold_schema:
    st.warning("⚠️ ยังไม่มีข้อมูล กรุณารัน load_local_data.py และ dbt run")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────
coins = con.execute(f"""
    SELECT coin_id, symbol, name
    FROM {gold_schema}.dim_coin
    ORDER BY name
""").df()

selected_coin = st.sidebar.selectbox(
    "Select Coin",
    options=coins["coin_id"].tolist(),
    format_func=lambda x: coins.loc[
        coins["coin_id"] == x, "name"
    ].values,
)
show_ma = st.sidebar.checkbox("Show Moving Averages", value=True)

# ── Load Data ─────────────────────────────────────────────────
df = con.execute(f"""
    SELECT
        d.date,
        f.open_price,
        f.high_price,
        f.low_price,
        f.close_price,
        f.daily_return_pct,
        f.ma_7d,
        f.ma_30d
    FROM {gold_schema}.fact_price_ohlcv f
    JOIN {gold_schema}.dim_coin dc ON f.coin_sk  = dc.coin_sk
    JOIN {gold_schema}.dim_date d  ON f.date_id  = d.date_id
    WHERE dc.coin_id = ?
    ORDER BY d.date
""", [selected_coin]).df()

if df.empty:
    st.warning("No data for selected coin")
    st.stop()

# ── Candlestick ───────────────────────────────────────────────
st.subheader(f"OHLCV — {selected_coin.upper()}")
fig = go.Figure()
fig.add_trace(go.Candlestick(
    x=df["date"],
    open=df["open_price"],
    high=df["high_price"],
    low=df["low_price"],
    close=df["close_price"],
    name="OHLCV",
))
if show_ma:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["ma_7d"],
        name="MA 7D",
        line=dict(color="orange", width=1.5),
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["ma_30d"],
        name="MA 30D",
        line=dict(color="royalblue", width=1.5),
    ))
fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=500,
    template="plotly_dark",
)
st.plotly_chart(fig, use_container_width=True)

# ── Return Distribution ───────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.subheader("Daily Return Distribution")
    fig2 = px.histogram(
        df, x="daily_return_pct",
        nbins=50, template="plotly_dark",
        color_discrete_sequence=["#636EFA"],
        labels={"daily_return_pct": "Daily Return (%)"},
    )
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("Price Statistics")
    stats = df[["open_price","high_price",
                "low_price","close_price",
                "daily_return_pct"]].describe()
    st.dataframe(stats.round(4))