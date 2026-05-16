# streamlit/connection.py
import duckdb
import streamlit as st
from pathlib import Path

# Streamlit รัน Local → อ่าน DuckDB จาก named volume ผ่าน helper script
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"

@st.cache_resource
def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        return duckdb.connect(str(DB_PATH))
    except duckdb.IOException:
        con = duckdb.connect(":memory:")
        try:
            con.execute(
                f"ATTACH '{DB_PATH}' AS crypto (READ_ONLY)"
            )
        except Exception:
            pass
        return con


def get_gold_schema(con) -> str:
    """หา schema ที่มี fact_price_ohlcv"""
    try:
        result = con.execute("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = 'fact_price_ohlcv'
            LIMIT 1
        """).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def table_exists(con, table: str) -> bool:
    try:
        n = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = '{table}'
        """).fetchone()
        return n > 0
    except Exception:
        return False