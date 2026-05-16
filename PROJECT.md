# Crypto Analytics Data Pipeline — Project Context

## Project Overview
An end-to-end Data Pipeline for analyzing historical cryptocurrency prices (BTC, ETH, SOL) using Medallion Architecture (Bronze → Silver → Gold). Built as a university project for a Data Architecture course.

## Goal
Load historical OHLCV data from local CSV files → transform through dbt → serve via Streamlit dashboard.

## Architecture
data/raw/*.csv (Local)
↓ mount into Docker
Apache Airflow DAG (historical_ingestion)
├── load_ohlcv_to_silver → reads CSV, writes to DuckDB silver.ohlcv
├── load_metadata_to_silver → hardcoded coin metadata → silver.coin_metadata
├── verify_silver → row count check
├── dbt_run_all_models → silver → gold (Star Schema)
└── dbt_test_all_models → data quality tests
↓
DuckDB (named Docker volume: data_volume)
├── silver.ohlcv
├── silver.coin_metadata
├── main_gold.fact_price_ohlcv
├── main_gold.dim_coin
└── main_gold.dim_date
↓
scripts/export_db.py → docker cp DuckDB → data/warehouse/crypto.duckdb
↓
Streamlit (Local)
├── app.py → Home + KPI Cards
├── pages/1_price_analysis.py → Candlestick, MA7, MA30, Return Distribution
└── pages/2_correlation.py → Correlation Heatmap + Rolling Correlation


## Tech Stack
| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.9 (Docker) |
| Data Lake | MinIO (Docker) — not actively used after pivot to CSV |
| Data Warehouse | DuckDB 1.1.3 (named Docker volume) |
| Transformation | dbt-duckdb 1.8.1 (runs inside Airflow container) |
| Visualization | Streamlit 1.35 + Plotly 5.22 (Local) |
| Containerization | Docker Compose |
| Language | Python 3.11 |

## Deployment
- **Docker (named volume)**: Airflow, MinIO, PostgreSQL (Airflow metadata)
- **Local**: Streamlit, dbt CLI (for local dev), Python scripts
- **DuckDB**: Lives inside Docker named volume `data_volume` mounted at `/opt/airflow/data/warehouse/crypto.duckdb`
- **Streamlit** reads DuckDB from `data/warehouse/crypto.duckdb` (local path after export)

## Data Sources
- **CSV files** in `data/raw/`: `bitcoin.csv`, `ethereum.csv`, `solana.csv`
- Format: `SNo, Name, Symbol, Date, High, Low, Open, Close, Volume, Marketcap`
- Source: Kaggle — "Cryptocurrency Historical Prices" by sudalairajkumar
- Date range: BTC 2013-2021, ETH 2015-2021, SOL 2020-2021

## Data Model (Star Schema — Gold Layer)
dim_coin (coin_sk, coin_id, symbol, name, categories, genesis_date, hashing_algorithm)
dim_date (date_id, date, year, month, quarter, day_name, is_weekend, season)
fact_price_ohlcv (price_sk, coin_sk→FK, date_id→FK, open_price, high_price,
low_price, close_price, daily_range, daily_return_pct,
price_change, ma_7d, ma_30d, source_system, extracted_at)


## dbt Models
staging/
stg_coin_prices.sql → reads silver.ohlcv, type cast, quality flags
intermediate/
int_price_enriched.sql → window functions: daily_return_pct, ma_7d, ma_30d
mart/
dim_coin.sql → from silver.coin_metadata, md5 surrogate key
dim_date.sql → generated date spine 2013-01-01 to 2030-12-31
fact_price_ohlcv.sql → joins enriched + dim_coin + dim_date


## dbt profiles.yml
```yaml
crypto_pipeline:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /opt/airflow/data/warehouse/crypto.duckdb
      schema: main
      threads: 4
```
Note: dbt creates schemas as main_silver and main_gold (not silver/gold)

## Airflow DAG: historical_ingestion
- Schedule: None (manual trigger only)
- Tasks (sequential to avoid DuckDB lock conflicts):
load_ohlcv_to_silver >> load_metadata_to_silver >> verify_silver >> dbt_run_all_models >> dbt_test_all_models
- CSV path inside container: /opt/airflow/data/raw/
- DuckDB path inside container: /opt/airflow/data/warehouse/crypto.duckdb
- dbt run command: cd /opt/airflow/dbt && PYTHONUTF8=1 dbt run --profiles-dir .

## Key Issues Encountered & Resolved
1. yfinance/CoinGecko API failed inside Docker → pivoted to local CSV
2. DuckDB lock conflict when parallel Airflow tasks → changed to sequential
3. fetchone() returns tuple not int → must use .fetchone()
4. dbt schema prefix: dbt adds main_ prefix → gold tables are main_gold.* not gold.*
5. Streamlit uses get_gold_schema() to dynamically find correct schema name
6. PYTHONUTF8=1 required for dbt on Windows to avoid charmap encoding error
7. DuckDB named volume not directly accessible from Local → use scripts/export_db.py to copy out

## Streamlit connection.py Key Function
```python
    def get_gold_schema(con) -> str:
        result = con.execute("""
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'fact_price_ohlcv' LIMIT 1
        """).fetchone()
        return result if result else None
```

## Current Status
- Airflow DAG: load_ohlcv_to_silver still failing
- Root cause: count_rows() function in DAG returns tuple instead of int because .fetchone() is missing ``
- Fix needed: ensure count_rows() uses .fetchone()
- Container DAG file may not be syncing from local mount correctly
- Workaround: use docker cp to copy DAG file directly into container

## File Structure
crypto-pipeline/
├── docker-compose.yml
├── Dockerfile.airflow
├── .env
├── requirements.txt
├── airflow/dags/
│   ├── ingestion_historical_dag.py   ← CURRENT ISSUE HERE
│   └── ingestion_incremental_dag.py
├── ingestion/
│   ├── coingecko.py
│   ├── fear_greed.py
│   ├── minio_client.py
│   └── yfinance_connector.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/stg_coin_prices.sql
│       ├── intermediate/int_price_enriched.sql
│       └── mart/ (dim_coin, dim_date, fact_price_ohlcv, schema.yml)
├── streamlit/
│   ├── connection.py
│   ├── app.py
│   └── pages/ (1_price_analysis.py, 2_correlation.py)
├── scripts/
│   ├── load_local_data.py   ← alternative local pipeline (bypass Airflow)
│   ├── export_db.py         ← docker cp DuckDB to local
│   └── check_db.py          ← verify tables exist
└── data/
    ├── raw/ (bitcoin.csv, ethereum.csv, solana.csv)
    └── warehouse/ (crypto.duckdb — local copy after export)

## Environment Variables (.env)
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=crypto-lake
DUCKDB_PATH=/opt/airflow/data/warehouse/crypto.duckdb

## Docker Compose Volumes
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./airflow/logs:/opt/airflow/logs
  - ./ingestion:/opt/airflow/ingestion
  - ./dbt:/opt/airflow/dbt
  - ./data/raw:/opt/airflow/data/raw        # CSV input
  - data_volume:/opt/airflow/data/warehouse  # DuckDB output (named volume)
