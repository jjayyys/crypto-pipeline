# streamlit/pages/3_sentiment.py
import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import os

st.title("😨 Fear & Greed Sentiment")

DB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse/crypto.duckdb")

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()

df = con.execute("""
    SELECT
        d.date,
        s.fear_greed_value,
        s.sentiment_bucket,
        s.classification
    FROM gold.dim_sentiment s
    JOIN gold.dim_date d ON s.sentiment_date = d.date
    ORDER BY d.date
""").df()

# ── Sentiment Timeline ────────────────────────────────────────────────────────
st.subheader("Fear & Greed Index Over Time")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["fear_greed_value"],
    fill="tozeroy",
    line=dict(color="#EF553B"),
    name="Fear & Greed",
))
fig.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Neutral")
fig.update_layout(template="plotly_dark", height=400)
st.plotly_chart(fig, use_container_width=True)

# ── Distribution by Bucket ────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    bucket_counts = df["sentiment_bucket"].value_counts().reset_index()
    bucket_counts.columns = ["bucket", "count"]
    fig2 = px.pie(
        bucket_counts, names="bucket", values="count",
        template="plotly_dark",
        color_discrete_sequence=px.colors.sequential.RdBu,
    )
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("Current Sentiment")
    latest = df.iloc[-1]
    st.metric(
        label=latest["sentiment_bucket"],
        value=int(latest["fear_greed_value"]),
        delta=int(latest["fear_greed_value"] - df.iloc[-2]["fear_greed_value"]),
    )
    st.progress(int(latest["fear_greed_value"]) / 100)