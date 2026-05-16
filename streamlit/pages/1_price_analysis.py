# streamlit/pages/1_price_analysis.py
import streamlit as st
import duckdb
import plotly.graph_objects as go
import plotly.express as px
import os
from pathlib import Path

st.title("💹 Price Analysis")

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # crypto-pipeline/
DB_PATH  = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()

# ── Sidebar Controls ─────────────────────────────────────────────────────────
coins = con.execute(
    "SELECT coin_id, symbol, name FROM gold.dim_coin ORDER BY name"
).df()

selected_coin = st.sidebar.selectbox(
    "Select Coin",
    options=coins["coin_id"].tolist(),
    format_func=lambda x: coins.loc[coins["coin_id"] == x, "name"].values,
)

date_range = st.sidebar.date_input(
    "Date Range",
    value=[],
    help="Leave empty for full history",
)

show_ma = st.sidebar.checkbox("Show Moving Averages", value=True)

# ── Load Data ─────────────────────────────────────────────────────────────────
query = """
    SELECT
        d.date,
        f.open_price,
        f.high_price,
        f.low_price,
        f.close_price,
        f.daily_return_pct,
        f.ma_7d,
        f.ma_30d,
        f.fear_greed_value,
        f.daily_range
    FROM gold.fact_price_ohlcv f
    JOIN gold.dim_coin      dc ON f.coin_sk  = dc.coin_sk
    JOIN gold.dim_date      d  ON f.date_id  = d.date_id
    WHERE dc.coin_id = ?
    ORDER BY d.date
"""
df = con.execute(query, [selected_coin]).df()

if df.empty:
    st.warning("No data available for selected coin")
    st.stop()

# ── Candlestick Chart ─────────────────────────────────────────────────────────
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
        name="MA 7D", line=dict(color="orange", width=1.5),
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["ma_30d"],
        name="MA 30D", line=dict(color="blue", width=1.5),
    ))

fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=500,
    template="plotly_dark",
)
st.plotly_chart(fig, use_container_width=True)

# ── Daily Return Distribution ─────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Return Distribution")
    fig2 = px.histogram(
        df, x="daily_return_pct",
        nbins=50, template="plotly_dark",
        color_discrete_sequence=["#636EFA"],
    )
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("Fear & Greed vs Price")
    fig3 = px.scatter(
        df, x="fear_greed_value", y="daily_return_pct",
        color="fear_greed_value",
        color_continuous_scale="RdYlGn",
        template="plotly_dark",
        labels={
            "fear_greed_value": "Fear & Greed Index",
            "daily_return_pct": "Daily Return (%)",
        },
    )
    st.plotly_chart(fig3, use_container_width=True)