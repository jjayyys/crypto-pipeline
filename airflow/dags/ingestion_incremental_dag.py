# airflow/dags/ingestion_incremental_dag.py
"""
DAG: Incremental Ingestion (Hourly)
- ดึงข้อมูลล่าสุดทุกชั่วโมง
- Append ลง Bronze → Re-transform Silver → Refresh Gold
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def extract_latest_market_data(**context):
    """ดึง current market snapshot ทุกชั่วโมง → Bronze"""
    from ingestion.coingecko import fetch_market_data, COINS
    from ingestion.minio_client import upload_json_gz
    from datetime import datetime, timezone

    coin_ids = [c["id"] for c in COINS]
    data = fetch_market_data(coin_ids=coin_ids)

    if not data:
        raise ValueError("Failed to fetch market data from CoinGecko")

    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = upload_json_gz(
        data=data,
        layer="bronze",
        source="coingecko",
        entity="market_snapshot",
        partition_date=partition_date,
    )
    context["ti"].xcom_push(key="snapshot_key", value=key)


def extract_latest_sentiment(**context):
    """ดึง Fear & Greed ล่าสุด → Bronze"""
    from ingestion.fear_greed import fetch_fear_greed_index
    from ingestion.minio_client import upload_json_gz
    from datetime import datetime, timezone

    data = fetch_fear_greed_index(limit=1)
    if not data:
        raise ValueError("Failed to fetch Fear & Greed")

    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = upload_json_gz(
        data=data,
        layer="bronze",
        source="fear_greed",
        entity="sentiment",
        partition_date=partition_date,
    )
    context["ti"].xcom_push(key="sentiment_key", value=key)


def upsert_silver_market_snapshot(**context):
    """
    Bronze snapshot → Silver (upsert ด้วย coin_id + snapshot_hour)
    """
    import duckdb
    import json
    import gzip
    import os
    from ingestion.minio_client import get_s3_client, BUCKET

    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse/crypto.duckdb")
    key = context["ti"].xcom_pull(
        task_ids="extract_latest_market_data",
        key="snapshot_key"
    )

    s3 = get_s3_client()
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    raw = json.loads(gzip.decompress(resp["Body"].read()))

    rows = []
    extracted_at = raw["extracted_at"]
    for item in raw["data"]:
        rows.append({
            "coin_id":               item["id"],
            "snapshot_at":           extracted_at,
            "current_price":         item.get("current_price"),
            "market_cap":            item.get("market_cap"),
            "total_volume":          item.get("total_volume"),
            "price_change_pct_1h":   item.get("price_change_percentage_1h_in_currency"),
            "price_change_pct_24h":  item.get("price_change_percentage_24h_in_currency"),
            "price_change_pct_7d":   item.get("price_change_percentage_7d_in_currency"),
        })

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    import pandas as pd
    df = pd.DataFrame(rows)

    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.market_snapshot (
            coin_id              VARCHAR,
            snapshot_at          TIMESTAMP,
            current_price        DOUBLE,
            market_cap           DOUBLE,
            total_volume         DOUBLE,
            price_change_pct_1h  DOUBLE,
            price_change_pct_24h DOUBLE,
            price_change_pct_7d  DOUBLE
        );
    """)
    # Insert-only (append) — downstream dbt handles dedup
    con.execute("INSERT INTO silver.market_snapshot SELECT * FROM df")
    con.close()


with DAG(
    dag_id="incremental_ingestion",
    description="Hourly incremental load: market snapshot + sentiment",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "incremental", "bronze"],
) as dag:

    t_market = PythonOperator(
        task_id="extract_latest_market_data",
        python_callable=extract_latest_market_data,
    )

    t_sentiment = PythonOperator(
        task_id="extract_latest_sentiment",
        python_callable=extract_latest_sentiment,
    )

    t_silver_snapshot = PythonOperator(
        task_id="upsert_silver_market_snapshot",
        python_callable=upsert_silver_market_snapshot,
    )

    t_dbt_refresh = BashOperator(
        task_id="dbt_refresh_mart",
        bash_command="cd /opt/airflow/dbt && dbt run --select mart --profiles-dir .",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    [t_market, t_sentiment] >> t_silver_snapshot >> t_dbt_refresh