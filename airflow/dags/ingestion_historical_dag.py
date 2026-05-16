# airflow/dags/ingestion_historical_dag.py
"""
DAG: Historical Ingestion (Run Once / Backfill)
- ดึง 1 ปีย้อนหลังของ OHLCV, Market Index, Fear&Greed
- เขียนลง MinIO Bronze Layer
- Transform → Silver (DuckDB)
- Load → Gold / Star Schema (DuckDB)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task functions ─────────────────────────────────────────────────────────────

def extract_ohlcv_historical(**context):
    """Extract 1-year OHLCV from CoinGecko → Bronze (MinIO)"""
    from ingestion.coingecko import fetch_historical_ohlcv, COINS
    from ingestion.minio_client import upload_json_gz
    import time

    uploaded = []
    for coin in COINS:
        data = fetch_historical_ohlcv(coin_id=coin["id"], days=365)
        if data:
            key = upload_json_gz(
                data=data,
                layer="bronze",
                source="coingecko",
                entity="ohlcv",
            )
            uploaded.append(key)
            context["ti"].log.info(f"✅ {coin['id']} → {key}")
        time.sleep(2)   # rate limit

    # Push keys to XCom for downstream tasks
    context["ti"].xcom_push(key="ohlcv_keys", value=uploaded)


def extract_coin_metadata(**context):
    """Extract coin metadata → Bronze (MinIO)"""
    from ingestion.coingecko import fetch_coin_metadata, COINS
    from ingestion.minio_client import upload_json_gz
    import time

    uploaded = []
    for coin in COINS:
        data = fetch_coin_metadata(coin_id=coin["id"])
        if data:
            key = upload_json_gz(
                data=data,
                layer="bronze",
                source="coingecko",
                entity="metadata",
            )
            uploaded.append(key)
        time.sleep(2)

    context["ti"].xcom_push(key="metadata_keys", value=uploaded)


def extract_fear_greed_historical(**context):
    """Extract 365 days of Fear & Greed → Bronze (MinIO)"""
    from ingestion.fear_greed import fetch_fear_greed_index
    from ingestion.minio_client import upload_json_gz

    data = fetch_fear_greed_index(limit=365)
    if data:
        key = upload_json_gz(
            data=data,
            layer="bronze",
            source="fear_greed",
            entity="sentiment",
        )
        context["ti"].xcom_push(key="sentiment_key", value=key)


def extract_market_indices(**context):
    """Extract 1-year Market Index from Yahoo Finance → Bronze (MinIO)"""
    from ingestion.yfinance_connector import fetch_index_history, INDICES
    from ingestion.minio_client import upload_json_gz

    uploaded = []
    for ticker in INDICES:
        data = fetch_index_history(ticker=ticker, period="1y")
        if data:
            key = upload_json_gz(
                data=data,
                layer="bronze",
                source="yfinance",
                entity="market_index",
            )
            uploaded.append(key)

    context["ti"].xcom_push(key="index_keys", value=uploaded)


def transform_bronze_to_silver(**context):
    """
    Read from Bronze (MinIO JSON.gz) → Clean & Normalize → Write Silver (DuckDB)
    """
    import duckdb
    import json
    import gzip
    import os
    from ingestion.minio_client import get_s3_client, list_objects, BUCKET

    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse/crypto.duckdb")
    s3 = get_s3_client()
    con = duckdb.connect(db_path)

    # ── Install & Load httpfs for S3 access ──────────────────────
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    # ── Create Silver Schema ──────────────────────────────────────
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # ── Transform OHLCV ──────────────────────────────────────────
    ohlcv_keys = context["ti"].xcom_pull(
        task_ids="extract_ohlcv_historical",
        key="ohlcv_keys"
    ) or []

    ohlcv_rows = []
    for key in ohlcv_keys:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        raw = json.loads(gzip.decompress(resp["Body"].read()))

        coin_id     = raw["coin_id"]
        extracted   = raw["extracted_at"]

        for row in raw["data"]:  # [timestamp_ms, open, high, low, close]
            ts_ms = row
            date  = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
            ohlcv_rows.append({
                "coin_id":       coin_id,
                "date":          date,
                "open":          row,
                "high":          row,
                "low":           row,
                "close":         row,
                "source":        "coingecko",
                "extracted_at":  extracted,
            })

    if ohlcv_rows:
        import pandas as pd
        df = pd.DataFrame(ohlcv_rows).drop_duplicates(subset=["coin_id", "date"])
        con.execute("""
            CREATE OR REPLACE TABLE silver.ohlcv AS
            SELECT * FROM df
        """)
        context["ti"].log.info(f"✅ Silver OHLCV: {len(df):,} rows")

    con.close()


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="historical_ingestion",
    description="Full historical load: CoinGecko + Yahoo Finance + Fear&Greed",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,     # Manual trigger only (Backfill)
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "historical", "bronze"],
) as dag:

    # ── Extract Tasks (Parallel) ──────────────────────────────────
    t_ohlcv = PythonOperator(
        task_id="extract_ohlcv_historical",
        python_callable=extract_ohlcv_historical,
    )

    t_metadata = PythonOperator(
        task_id="extract_coin_metadata",
        python_callable=extract_coin_metadata,
    )

    t_sentiment = PythonOperator(
        task_id="extract_fear_greed_historical",
        python_callable=extract_fear_greed_historical,
    )

    t_indices = PythonOperator(
        task_id="extract_market_indices",
        python_callable=extract_market_indices,
    )

    # ── Transform Bronze → Silver ─────────────────────────────────
    t_silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
    )

    # ── dbt Run (Silver → Gold / Star Schema) ─────────────────────
    t_dbt_run = BashOperator(
        task_id="dbt_run_all_models",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    # ── dbt Test ──────────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test_all_models",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    # ── Dependencies ──────────────────────────────────────────────
    [t_ohlcv, t_metadata, t_sentiment, t_indices] >> t_silver >> t_dbt_run >> t_dbt_test