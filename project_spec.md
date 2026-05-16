# Project Specification: Crypto Analytics Data Pipeline

## 1. Project Overview

| Field       | Detail                                          |
|-------------|-------------------------------------------------|
| Project     | Crypto & Financial Market Analytics Platform    |
| Course      | Data Architecture                               |
| Objective   | End-to-end Data Pipeline สำหรับวิเคราะห์ตลาด Crypto |
| Pattern     | ELT + Medallion Architecture (Bronze/Silver/Gold) |
| Duration    | 16 hours                                        |

---

## 2. Problem Statement

ตลาด Cryptocurrency มีความผันผวนสูงและมีข้อมูลจากหลายแหล่ง
โปรเจคนี้ต้องการสร้าง Data Platform ที่:

1. รวบรวมข้อมูลจากหลาย API อัตโนมัติ
2. จัดเก็บในรูปแบบที่เหมาะสมกับแต่ละ Layer
3. Transform ให้พร้อมสำหรับการวิเคราะห์
4. แสดงผลผ่าน Dashboard ที่เข้าใจง่าย

---

## 3. Data Sources

### 3.1 CoinGecko API (Primary)
- **URL**: https://api.coingecko.com/api/v3
- **Auth**: ไม่ต้องใช้ API Key (Free tier)
- **Rate Limit**: 30 calls/minute
- **Endpoints ที่ใช้**:
  | Endpoint | Data | Schedule |
  |----------|------|----------|
  | `/coins/{id}/ohlc` | Historical OHLCV | Once (Backfill) |
  | `/coins/markets` | Market snapshot | Hourly |
  | `/coins/{id}` | Coin metadata | Once |

- **Coins ที่ติดตาม**: bitcoin (BTC), ethereum (ETH), binancecoin (BNB), solana (SOL), cardano (ADA)

### 3.2 Yahoo Finance (yfinance)
- **Auth**: ไม่ต้องใช้ API Key
- **Data**: Historical OHLCV ของ Market Indices
- **Tickers**: ^GSPC (S&P500), ^IXIC (NASDAQ), ^DJI (Dow Jones), GC=F (Gold Futures)
- **Schedule**: Once (Backfill), Daily

### 3.3 Fear & Greed Index (alternative.me)
- **URL**: https://api.alternative.me/fng/
- **Auth**: ไม่ต้องใช้ API Key
- **Data**: Daily sentiment score (0-100)
- **Schedule**: Daily

---

## 4. Architecture

### 4.1 Overall Architecture
┌─────────────────────────────────────────────────────────────┐
│ DATA SOURCES                                                │
│ CoinGecko API │ Yahoo Finance │ Fear & Greed API            │
└──────────┬────────┴─────────┬─────────┴──────────┬──────────┘
│ │ │
▼ ▼ ▼
┌─────────────────────────────────────────────────────────────┐
│ ORCHESTRATION (Apache Airflow)                              │
│ DAG: historical_ingestion DAG: incremental_ingestion        │
│ Schedule: Manual (Once) Schedule: @hourly                   │
└──────────────────────────┬──────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ DATA LAKE (MinIO)                                           │
│                                                             │
│ Bronze Layer Silver Layer Gold Layer                        │
│ /bronze/ → DuckDB → DuckDB                                  │
│ raw JSON.gz silver schema gold schema                       │
│ immutable cleaned star schema                               │
└──────────────────────────┬──────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ TRANSFORMATION (dbt Core + DuckDB)                          │
│                                                             │
│ Staging Models → Intermediate Models → Mart Models          │
│ (Views) (Tables) (Star Schema)                              │
└──────────────────────────┬──────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ SERVING LAYER (Streamlit)                                   │
│ Price Analysis │ Correlation Analysis │ Sentiment Analysis  │
└─────────────────────────────────────────────────────────────┘


### 4.2 Medallion Architecture

| Layer | Location | Format | Purpose |
|-------|----------|--------|---------|
| **Bronze** | MinIO `/bronze/` | JSON.gz | Raw, immutable, as-is from API |
| **Silver** | DuckDB `silver.*` | Table | Cleaned, normalized, typed |
| **Gold** | DuckDB `gold.*` | Table | Star Schema, ready for BI |

### 4.3 Deployment Architecture

| Service | Deployment | Port | หมายเหตุ |
|---------|-----------|------|---------|
| Apache Airflow | Docker | 8080 | Orchestration |
| MinIO | Docker | 9000, 9001 | Data Lake |
| PostgreSQL | Docker | 5432 | Airflow Metadata |
| DuckDB | Local File | - | Data Warehouse |
| Streamlit | Local | 8501 | Dashboard |
| dbt | Local CLI | - | Transformation |
| Python Scripts | Local | - | Ingestion Logic |

---

## 5. Data Model

### 5.1 Star Schema (Gold Layer)
                ┌──────────────┐
                │   dim_coin   │
                │──────────────│
                │ coin_sk (PK) │
                │ coin_id      │
                │ symbol       │
                │ name         │
                │ categories   │
                │ genesis_date │
                │ hashing_algo │
                └──────┬───────┘
                       │
┌─────────────┐ ┌───────▼──────────────┐ ┌────────────────┐
│ dim_date    │ │ fact_price_ohlcv     │ │ dim_sentiment  │
│─────────────│ │──────────────────────│ │────────────────│
│ date_id(PK) │◄───│ price_sk (PK) │───►│sentiment_sk(PK) │
│ date        │ │ coin_sk (FK)         │ │sentiment_date  │
│ year        │ │ date_id (FK)         │ │fear_greed_value│
│ month       │ │ sentiment_sk (FK)    │ │classification  │
│ day         │ │ open_price           │ │sentiment_bucket│
│ quarter     │ │ high_price           │ └────────────────┘
│ week        │ │ low_price            │
│ day_name    │ │ close_price          │
│ is_weekend  │ │ daily_range          │
│ season      │ │ daily_return_pct     │
└─────────────┘ │ price_change         │
                │ ma_7d                │
                │ ma_30d               │
                │ fear_greed_value     │
                │ source_system        │
                │ extracted_at         │
                └──────────────────────┘         


### 5.2 Dimension Details

#### dim_coin
| Column | Type | Description |
|--------|------|-------------|
| coin_sk | VARCHAR | Surrogate Key (MD5 hash) |
| coin_id | VARCHAR | Natural Key (e.g., "bitcoin") |
| symbol | VARCHAR | Ticker (e.g., "BTC") |
| name | VARCHAR | Full name |
| categories | VARCHAR[] | e.g., ["Cryptocurrency", "Layer 1"] |
| genesis_date | DATE | Launch date |
| hashing_algorithm | VARCHAR | e.g., "SHA-256" |
| dbt_updated_at | TIMESTAMP | Last updated by dbt |

#### dim_date
| Column | Type | Description |
|--------|------|-------------|
| date_id | INTEGER | Surrogate Key (YYYYMMDD) |
| date | DATE | Full date |
| year | INTEGER | Year |
| month | INTEGER | Month (1-12) |
| quarter | INTEGER | Quarter (1-4) |
| day_name | VARCHAR | e.g., "Monday" |
| is_weekend | BOOLEAN | True if Sat/Sun |
| season | VARCHAR | Winter/Spring/Summer/Fall |

#### dim_sentiment
| Column | Type | Description |
|--------|------|-------------|
| sentiment_sk | VARCHAR | Surrogate Key |
| sentiment_date | DATE | Date of reading |
| fear_greed_value | INTEGER | 0-100 |
| classification | VARCHAR | Original API label |
| sentiment_bucket | VARCHAR | Extreme Fear/Fear/Neutral/Greed/Extreme Greed |

#### fact_price_ohlcv
| Column | Type | Description |
|--------|------|-------------|
| price_sk | VARCHAR | Surrogate Key |
| coin_sk | VARCHAR | FK → dim_coin |
| date_id | INTEGER | FK → dim_date |
| sentiment_sk | VARCHAR | FK → dim_sentiment |
| open_price | DOUBLE | Opening price (USD) |
| high_price | DOUBLE | Daily high (USD) |
| low_price | DOUBLE | Daily low (USD) |
| close_price | DOUBLE | Closing price (USD) |
| daily_range | DOUBLE | high - low |
| daily_return_pct | DOUBLE | (close-open)/open * 100 |
| price_change | DOUBLE | close - prev_close |
| ma_7d | DOUBLE | 7-day moving average |
| ma_30d | DOUBLE | 30-day moving average |
| fear_greed_value | INTEGER | Denormalized sentiment |
| source_system | VARCHAR | "coingecko" |
| extracted_at | TIMESTAMP | Ingestion timestamp |

---

## 6. Data Pipeline Design

### 6.1 DAG: historical_ingestion
Schedule: None (Manual trigger)
Purpose: Full historical backfill (365 days)

[extract_ohlcv_historical] ─┐
[extract_coin_metadata] ├─→ [transform_bronze_to_silver]
[extract_fear_greed_historical]│ → [dbt_run_all_models]
[extract_market_indices] ─┘ → [dbt_test_all_models]

Parallelism: Extract tasks รันพร้อมกัน 4 tasks


### 6.2 DAG: incremental_ingestion
Schedule: @hourly
Purpose: ดึงข้อมูลล่าสุดทุกชั่วโมง

[extract_latest_market_data] ─┐
├─→ [upsert_silver_market_snapshot]
[extract_latest_sentiment] ─┘ → [dbt_refresh_mart]

Parallelism: Extract tasks รันพร้อมกัน 2 tasks


### 6.3 File Path Convention (MinIO)
Pattern:
{layer}/{source}/{entity}/date={YYYY-MM-DD}/{YYYYMMDD_HHMMSS}.json.gz

Examples:
bronze/coingecko/ohlcv/date=2024-01-15/20240115_083000.json.gz
bronze/fear_greed/sentiment/date=2024-01-15/20240115_083001.json.gz
bronze/yfinance/market_index/date=2024-01-15/20240115_083002.json.gz


---

## 7. Data Quality

### 7.1 dbt Tests (Automated)
| Test | Model | Column | Rule |
|------|-------|--------|------|
| unique | fact_price_ohlcv | price_sk | ต้องไม่ซ้ำ |
| not_null | fact_price_ohlcv | price_sk, coin_sk, date_id | ต้องไม่ null |
| relationships | fact_price_ohlcv | coin_sk | ต้องมีใน dim_coin |
| relationships | fact_price_ohlcv | date_id | ต้องมีใน dim_date |
| expression | fact_price_ohlcv | close_price | >= 0 |
| expression | fact_price_ohlcv | daily_return_pct | between -100 and 10000 |
| expression | dim_sentiment | fear_greed_value | between 0 and 100 |
| unique | dim_coin | coin_sk | ต้องไม่ซ้ำ |
| unique | dim_date | date_id | ต้องไม่ซ้ำ |

### 7.2 Staging-level Checks (SQL)
| Check | Location | Action |
|-------|----------|--------|
| null price | stg_coin_prices | flag `has_null_price = true` |
| invalid OHLC (high < low) | stg_coin_prices | flag `is_invalid_ohlc = true` |
| filter nulls | int_price_enriched | WHERE not has_null_price |

---

## 8. Dashboard Pages

### Page 1: Home (app.py)
- KPI Cards: Total Records, Coins Tracked, Latest Date, Avg Fear & Greed
- Pipeline status overview

### Page 2: Price Analysis (1_price_analysis.py)
- Interactive Candlestick Chart (OHLCV)
- Moving Average Overlay (MA7, MA30)
- Daily Return Distribution (Histogram)
- Fear & Greed vs Daily Return (Scatter)

### Page 3: Correlation Analysis (2_correlation.py)
- Cross-coin Return Correlation Heatmap
- Interpretation guide

### Page 4: Sentiment Analysis (3_sentiment.py)
- Fear & Greed Timeline
- Sentiment Distribution (Pie Chart)
- Current Sentiment Gauge

---

## 9. Project Structure
crypto-pipeline/
├── docker-compose.yml # Docker services definition
├── .env # Environment variables
├── .gitignore
├── requirements.txt # Python dependencies
├── README.md
│
├── airflow/
│ ├── dags/
│ │ ├── ingestion_historical_dag.py
│ │ └── ingestion_incremental_dag.py
│ ├── plugins/
│ └── logs/
│
├── ingestion/
│ ├── init.py
│ ├── coingecko.py # CoinGecko API connector
│ ├── yfinance_connector.py # Yahoo Finance connector
│ ├── fear_greed.py # Fear & Greed API connector
│ └── minio_client.py # MinIO S3 client wrapper
│
├── dbt/
│ ├── dbt_project.yml
│ ├── profiles.yml
│ ├── packages.yml # dbt-utils dependency
│ └── models/
│ ├── staging/ # Views, normalize raw data
│ ├── intermediate/ # Tables, enrichment & derivation
│ └── mart/ # Tables, Star Schema (Gold)
│
├── streamlit/
│ ├── app.py # Home + KPIs
│ └── pages/
│ ├── 1_price_analysis.py
│ ├── 2_correlation.py
│ └── 3_sentiment.py
│
└── data/
├── warehouse/ # DuckDB file (auto-created)
└── temp/ # Temporary files


---

## 10. Constraints & Assumptions

| Item | Detail |
|------|--------|
| CoinGecko Rate Limit | 30 req/min → ใช้ time.sleep(2) ระหว่าง calls |
| DuckDB Concurrency | ไม่รองรับ multiple writers → Airflow ใช้ LocalExecutor |
| Data Freshness | Market data อัปเดตทุก 1 ชั่วโมง |
| History Depth | 365 วันย้อนหลัง |
| Currency | USD ทั้งหมด |
| OS | Tested on Windows 11 |
| Python Version | 3.11 (recommended) |