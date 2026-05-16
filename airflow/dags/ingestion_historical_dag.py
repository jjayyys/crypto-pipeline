# airflow/dags/ingestion_historical_dag.py
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

def count_rows(con, table: str) -> int:
    """Helper: นับ rows และ return int เสมอ"""
    return con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

def load_ohlcv_to_silver(**context):
    import duckdb
    import pandas as pd
    import os
    from pathlib import Path
    from datetime import datetime, timezone

    db_path = os.getenv(
        "DUCKDB_PATH",
        "/opt/airflow/data/warehouse/crypto.duckdb"
    )
    raw_dir = Path("/opt/airflow/data/raw")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    COIN_FILES = {
        "bitcoin":  "bitcoin.csv",
        "ethereum": "ethereum.csv",
        "solana":   "solana.csv",
    }

    all_ohlcv = []
    for coin_id, filename in COIN_FILES.items():
        filepath = raw_dir / filename
        if not filepath.exists():
            context["ti"].log.warning(f"Missing: {filepath}")
            continue

        df = pd.read_csv(str(filepath))
        df.columns = df.columns.str.strip()
        context["ti"].log.info(
            f"{filename} columns: {list(df.columns)}"
        )

        # หา columns อัตโนมัติ
        col_map = {}
        for col in df.columns:
            cl = col.lower()
            if "date" in cl:
                col_map[col] = "date"
            elif cl == "open":
                col_map[col] = "open"
            elif cl == "high":
                col_map[col] = "high"
            elif cl == "low":
                col_map[col] = "low"
            elif cl == "close" and "adj" not in cl:
                col_map[col] = "close"

        df = df.rename(columns=col_map)

        required = ["date", "open", "high", "low", "close"]
        missing  = [c for c in required if c not in df.columns]
        if missing:
            context["ti"].log.error(
                f"{filename}: missing columns {missing}"
            )
            continue

        df["date"] = pd.to_datetime(
            df["date"]
        ).dt.strftime("%Y-%m-%d")

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=required)
        df = df[required].copy()
        df["coin_id"]      = coin_id
        df["source"]       = "local_csv"
        df["extracted_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        df = df.sort_values("date")
        all_ohlcv.append(df)

        context["ti"].log.info(
            f"✅ {coin_id}: {len(df):,} rows "
            f"({df['date'].min()} → {df['date'].max()})"
        )

    if not all_ohlcv:
        raise ValueError(
            "No OHLCV data! ตรวจสอบ CSV ใน /opt/airflow/data/raw/"
        )

    df_all = pd.concat(all_ohlcv, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["coin_id", "date"])

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.ohlcv AS
        SELECT * FROM df_all
    """)

    row_count = count_rows(con, "silver.ohlcv")
    context["ti"].log.info(f"✅ silver.ohlcv: {row_count:,} rows")


def load_metadata_to_silver(**context):
    import duckdb
    import pandas as pd
    import os
    from datetime import datetime, timezone

    db_path = os.getenv(
        "DUCKDB_PATH",
        "/opt/airflow/data/warehouse/crypto.duckdb"
    )

    COIN_META = [
        {
            "coin_id":           "bitcoin",
            "symbol":            "BTC",
            "name":              "Bitcoin",
            "categories":        "['Cryptocurrency', 'Layer 1']",
            "genesis_date":      "2009-01-03",
            "hashing_algorithm": "SHA-256",
        },
        {
            "coin_id":           "ethereum",
            "symbol":            "ETH",
            "name":              "Ethereum",
            "categories":        "['Cryptocurrency', 'Smart Contract']",
            "genesis_date":      "2015-07-30",
            "hashing_algorithm": "Ethash",
        },
        {
            "coin_id":           "solana",
            "symbol":            "SOL",
            "name":              "Solana",
            "categories":        "['Cryptocurrency', 'Layer 1']",
            "genesis_date":      "2020-03-16",
            "hashing_algorithm": "PoH",
        },
    ]

    df = pd.DataFrame(COIN_META)
    df["extracted_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    # ── แก้ Bug 2: connect หลังจาก load_ohlcv ปิด con แล้ว ───
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.coin_metadata AS
        SELECT * FROM df
    """)
    context["ti"].log.info(
        f"✅ silver.coin_metadata: {len(df)} rows"
    )
    con.close()


def verify_silver(**context):
    import duckdb
    import os

    db_path = os.getenv(
        "DUCKDB_PATH",
        "/opt/airflow/data/warehouse/crypto.duckdb"
    )
    con = duckdb.connect(db_path)

    checks = {
        "silver.ohlcv":         100,
        "silver.coin_metadata": 1,
    }

    for table, min_rows in checks.items():
        try:
            n = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            if n < min_rows:
                raise ValueError(
                    f"{table}: {n} rows (ต้องการ >= {min_rows})"
                )
            context["ti"].log.info(f"✅ {table}: {n:,} rows")
        except Exception as e:
            con.close()
            raise ValueError(f"❌ {e}")

    con.close()
    context["ti"].log.info("✅ Silver verification passed!")


with DAG(
    dag_id="historical_ingestion",
    description="Load CSV -> Silver -> dbt Gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "historical", "csv"],
) as dag:

    t_ohlcv = PythonOperator(
        task_id="load_ohlcv_to_silver",
        python_callable=load_ohlcv_to_silver,
    )

    t_metadata = PythonOperator(
        task_id="load_metadata_to_silver",
        python_callable=load_metadata_to_silver,
    )

    t_verify = PythonOperator(
        task_id="verify_silver",
        python_callable=verify_silver,
    )

    t_dbt_run = BashOperator(
        task_id="dbt_run_all_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PYTHONUTF8=1 dbt run --profiles-dir ."
        ),
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test_all_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PYTHONUTF8=1 dbt test --profiles-dir ."
        ),
    )

    # ── Sequential เพื่อหลีกเลี่ยง DuckDB lock conflict ────────
    t_ohlcv >> t_metadata >> t_verify >> t_dbt_run >> t_dbt_test