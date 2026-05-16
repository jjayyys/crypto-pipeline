# streamlit/app.py
import streamlit as st
import duckdb
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent))
from connection import get_connection, get_gold_schema, table_exists

st.set_page_config(
    page_title="Crypto Analytics",
    page_icon="📈",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"

st.title("📈 Crypto Analytics Platform")
st.caption("BTC · ETH · SOL — Historical Price Analysis")

try:
    con = get_connection()
except Exception as e:
    st.error(f"❌ Cannot connect: {e}")
    st.stop()

# ── หา schema จริงที่ dbt สร้าง ──────────────────────────────
gold_schema = get_gold_schema(con)

if not gold_schema:
    st.warning("""
        ⚠️ ยังไม่มีข้อมูล กรุณารัน:
        ```bash
        python scripts/load_local_data.py
        cd dbt && dbt run
        ```
    """)
    with st.expander("🔍 Debug Info"):
        try:
            schemas = con.execute("""
                SELECT DISTINCT table_schema, table_name
                FROM information_schema.tables
                ORDER BY table_schema, table_name
            """).df()
            st.dataframe(schemas)
        except Exception as e:
            st.error(str(e))
    st.stop()

st.caption(f"📂 Schema: `{gold_schema}`")

# ── KPI Cards ─────────────────────────────────────────────────
try:
    col1, col2, col3 = st.columns(3)

    with col1:
        n = con.execute(
            f"SELECT COUNT(*) FROM {gold_schema}.fact_price_ohlcv"
        ).fetchone()[0]
        st.metric("Total Records", f"{n:,}")

    with col2:
        coins = con.execute(f"""
            SELECT COUNT(DISTINCT coin_sk)
            FROM {gold_schema}.fact_price_ohlcv
        """).fetchone()[0]
        st.metric("Coins Tracked", coins)

    with col3:
        latest = con.execute(f"""
            SELECT MAX(d.date)
            FROM {gold_schema}.dim_date d
            JOIN {gold_schema}.fact_price_ohlcv f
              ON d.date_id = f.date_id
        """).fetchone()[0]
        st.metric("Latest Date", str(latest))

except Exception as e:
    st.error(f"❌ Query Error: {e}")