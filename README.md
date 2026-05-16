# 🪙 Crypto Analytics Data Pipeline

A production-grade data engineering project demonstrating modern best practices for building scalable, maintainable analytics pipelines. Built with **Apache Airflow**, **dbt**, **DuckDB**, and **Streamlit**.

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Technology Stack](#technology-stack)
4. [System Architecture](#system-architecture)
5. [Repository Structure](#repository-structure)
6. [Prerequisites](#prerequisites)
7. [Environment Setup](#environment-setup)
8. [Installation & Setup](#installation--setup)
9. [Usage](#usage)
10. [Pipeline Details](#pipeline-details)
11. [Data Model](#data-model)
12. [Dashboard Features](#dashboard-features)
13. [Development](#development)
14. [Troubleshooting](#troubleshooting)
15. [Project Statistics](#project-statistics)
16. [Lessons Learned](#lessons-learned)
17. [Future Enhancements](#future-enhancements)
18. [Contributing](#contributing)
19. [License](#license)

---

## 🎯 Project Overview

**Crypto Analytics Data Pipeline** is an end-to-end data engineering solution for cryptocurrency market analysis. It demonstrates a modern, professional approach to:

- **Data Ingestion**: Load historical cryptocurrency price data from CSV sources
- **Data Transformation**: Clean, validate, and transform using dbt with SQL
- **Data Warehousing**: Store in DuckDB for efficient OLAP analytics
- **Orchestration**: Automate daily pipeline runs with Apache Airflow
- **Analytics**: Deliver insights through an interactive Streamlit dashboard

### 📊 What It Does

1. **Ingests** historical OHLCV (Open, High, Low, Close, Volume) data for 3 cryptocurrencies
2. **Transforms** raw data through a 3-layer Medallion Architecture
3. **Creates** a dimensional star schema optimized for analytics
4. **Generates** technical indicators (moving averages, daily returns)
5. **Serves** insights via interactive dashboard with price analysis and correlation

### 🎓 Why This Project

Built as a **Data Architecture course project** to demonstrate:
- Real-world data engineering patterns
- Modern data stack best practices
- Production-ready code quality
- Automated testing and monitoring
- Cloud-ready, scalable architecture

---

## ✨ Key Features

### ✅ Architecture
- **Medallion Architecture**: Bronze → Silver → Gold layer separation
- **Star Schema**: Optimized dimensional design for analytics
- **Automated Orchestration**: Daily scheduled pipeline execution
- **Version Control**: All SQL and Python code in Git

### ✅ Data Quality
- **Automated Tests**: dbt assertions on data integrity
- **Validation Layer**: Python validation functions in Airflow
- **Null Checks**: Ensures critical columns have values
- **Duplicate Detection**: Removes duplicate records

### ✅ Analytics
- **Interactive Dashboard**: Built with Streamlit
- **Real-time KPIs**: Total records, coins tracked, latest date
- **Price Analysis**: Candlestick charts with moving averages
- **Correlation Analysis**: Cryptocurrency price correlations

### ✅ DevOps
- **Containerization**: Docker Compose for reproducible environment
- **CI/CD Ready**: Version controlled code, automated tests
- **Monitoring**: Airflow UI with execution logs
- **Scalability**: Cloud-ready architecture (cloud DW migration path)

---

## 🛠 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.9 | DAG scheduling and execution |
| **Data Warehouse** | DuckDB | 1.1.3 | Columnar OLAP database |
| **Transformation** | dbt-duckdb | 1.8.1 | SQL-based ELT logic |
| **Visualization** | Streamlit | 1.35 | Interactive web dashboard |
| **Charting** | Plotly | 5.22 | Interactive visualizations |
| **Data Processing** | Pandas | 2.2.2 | Data manipulation |
| **Database Adapter** | boto3 | 1.34 | S3/MinIO compatibility |
| **Containerization** | Docker | latest | Infrastructure-as-code |
| **Language** | Python | 3.11 | Core programming language |
| **Version Control** | Git | latest | Code versioning |
| **Metadata Store** | PostgreSQL | 15 | Airflow metadata |

---

## 🏗 System Architecture

### 📐 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                            │
│                  CSV Files (Local Storage)                        │
│         bitcoin.csv, ethereum.csv, solana.csv                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Airflow DAG │
                    │   (Daily)    │
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   Python Load     Python Load         Quality
   OHLCV Data      Metadata            Checks
   (5,603 rows)    (3 coins)
        │                  │                 │
        └──────────────────┼─────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned)                        │
│           DuckDB - Type Cast, Quality Flags                      │
│         silver.ohlcv | silver.coin_metadata                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                    dbt Models (5 files)
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
    Staging         Intermediate          Mart
  stg_coin_prices  int_price_enriched   (3 models)
  (validate)       (add indicators)    dim_coin
                   (MA7, MA30, return)  dim_date
                                        fact_price_ohlcv
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Business-Ready)                   │
│                  Star Schema Design                              │
│                                                                  │
│         ┌──────────────────┐     ┌──────────────────┐           │
│         │   dim_coin       │     │    dim_date      │           │
│         │  (3 rows)        │     │  (6,574 rows)    │           │
│         ├──────────────────┤     ├──────────────────┤           │
│         │ coin_sk (PK)  ◄──┼─────┤► date_id (PK)    │           │
│         │ coin_id          │  │  │ date             │           │
│         │ symbol           │  │  │ year, month, etc │           │
│         │ name             │  │  └──────────────────┘           │
│         │ genesis_date     │  │                                 │
│         │ hashing_algo     │  │                                 │
│         └──────────────────┘  │                                 │
│                                │                                │
│                         ┌──────┴──────────┐                     │
│                         ▼                 ▼                     │
│              ┌────────────────────────────────────┐             │
│              │  fact_price_ohlcv (Fact)          │             │
│              │  (5,603 rows)                      │             │
│              ├────────────────────────────────────┤             │
│              │ price_sk (PK)                      │             │
│              │ coin_sk (FK) → dim_coin            │             │
│              │ date_id (FK) → dim_date            │             │
│              │ open_price, high, low, close       │             │
│              │ daily_return, daily_range          │             │
│              │ ma_7d, ma_30d (technical)          │             │
│              │ extracted_at                       │             │
│              └────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                   ┌────────────────┐
                   │  DuckDB Export │
                   │ crypto.duckdb  │
                   └────────┬───────┘
                            │
                            ▼
            ┌───────────────────────────────┐
            │   Streamlit Dashboard         │
            ├───────────────────────────────┤
            │ 🏠 Home: KPI Cards            │
            │ 📊 Price Analysis: Charts     │
            │ 🔗 Correlation: Heatmaps      │
            └───────────────────────────────┘
```

### 🔄 Medallion Architecture Pattern

```
BRONZE LAYER (Raw Data)
├─ No transformation
├─ Direct from source
└─ Immutable historical record

        ↓↓↓

SILVER LAYER (Cleaned & Validated)
├─ Type casting
├─ Deduplication
├─ Null handling
├─ Business rule validation
└─ Ready for transformation

        ↓↓↓

GOLD LAYER (Business-Ready Analytics)
├─ Dimensional modeling
├─ Aggregated metrics
├─ Technical indicators
├─ Optimized for analytics
└─ Serves dashboards & reports
```

---

## 📁 Repository Structure

```
crypto-pipeline/
│
├── 📄 README.md                          # This file
├── 📄 PROJECT.md                         # Detailed technical documentation
├── 📄 DATABASE_PRESENTATION.html         # Interactive schema visualization
├── 📄 PRESENTATION_GUIDE.md              # Professor presentation guide
├── 📄 PROFESSOR_FACT_SHEET.txt           # Quick reference sheet
├── 📄 README_PRESENTATION.md             # Presentation package index
│
├── 🐳 docker-compose.yml                 # Docker infrastructure
├── 🐳 Dockerfile.airflow                 # Airflow container image
├── .env                                  # Environment variables
├── requirements.txt                      # Python dependencies
│
├── 📂 airflow/                           # Apache Airflow
│   ├── dags/
│   │   └── ingestion_historical_dag.py  # ⭐ Main pipeline DAG
│   ├── logs/                            # Execution logs
│   └── plugins/                         # Custom operators
│
├── 📂 dbt/                              # Data transformation with dbt
│   ├── dbt_project.yml                  # dbt configuration
│   ├── profiles.yml                     # DuckDB connection
│   ├── packages.yml                     # dbt packages
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_coin_prices.sql     # Clean raw data
│   │   ├── intermediate/
│   │   │   └── int_price_enriched.sql  # Add technical indicators
│   │   └── mart/
│   │       ├── dim_coin.sql            # Cryptocurrency dimension
│   │       ├── dim_date.sql            # Date dimension (spine)
│   │       ├── fact_price_ohlcv.sql    # Central fact table
│   │       └── schema.yml              # dbt tests & docs
│   ├── logs/
│   ├── target/                         # Compiled dbt artifacts
│   └── dbt_packages/
│
├── 📂 streamlit/                        # Analytics dashboard
│   ├── app.py                          # Home page (KPIs)
│   ├── connection.py                   # Database connection
│   └── pages/
│       ├── 1_price_analysis.py        # Price charts & indicators
│       └── 2_correlation.py           # Correlation analysis
│
├── 📂 data/                            # Data storage
│   ├── raw/
│   │   ├── bitcoin.csv                # Historical prices
│   │   ├── ethereum.csv
│   │   └── solana.csv
│   └── warehouse/
│       └── crypto.duckdb              # DuckDB database
│
├── 📂 scripts/                         # Utility scripts
│   └── export_db.py                   # Export DuckDB from Docker
│
├── 📂 sample_data/                    # CSV exports for reference
│   ├── dim_coin.csv
│   ├── dim_date_sample.csv
│   ├── fact_price_ohlcv_sample.csv
│   └── bitcoin_2021.csv
│
├── 📄 show_database_proof.py          # Interactive database verification
└── 📄 .gitignore                       # Git ignore rules
```

---

## 📋 Prerequisites

### System Requirements
- **OS**: Windows, macOS, or Linux
- **RAM**: 4GB minimum (8GB recommended for Docker)
- **Disk**: 2GB free space
- **Network**: Internet connection for Docker image pulls

### Required Software
- **Python** 3.11+
- **Docker** 20.10+
- **Docker Compose** 2.0+
- **Git** 2.30+

### Verify Prerequisites
```bash
# Check Python version
python --version          # Should be 3.11 or higher

# Check Docker
docker --version
docker run hello-world

# Check Docker Compose
docker-compose --version

# Check Git
git --version
```

---

## 🚀 Environment Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/crypto-pipeline.git
cd crypto-pipeline
```

### 2. Create Python Virtual Environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS/Linux
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Create `.env` File

```bash
# Copy template
cp .env.example .env

# Or create manually with these settings:
cat > .env << 'EOF'
# Airflow Configuration
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=crypto-lake

# Database
DUCKDB_PATH=/opt/airflow/data/warehouse/crypto.duckdb
EOF
```

### 4. Install Dependencies

```bash
# Install Python packages
pip install -r requirements.txt

# Install dbt-duckdb locally (optional, for local testing)
pip install dbt-duckdb==1.8.1
```

---

## 📥 Installation & Setup

### Step 1: Start Docker Containers

```bash
# Build and start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs
docker-compose logs airflow-scheduler
```

**Expected Output:**
```
CONTAINER ID   IMAGE              STATUS
xxx            airflow-scheduler   Up 2 minutes
xxx            airflow-webserver   Up 2 minutes
xxx            postgres            Up 2 minutes
xxx            minio               Up 2 minutes
```

### Step 2: Access Airflow UI

Open in browser: **http://localhost:8080**
- Username: `admin`
- Password: `admin`

Verify DAG is loaded: Look for `historical_ingestion` in the DAG list

### Step 3: Trigger the Pipeline

```bash
# Option 1: Via Airflow UI
# Click DAG → Trigger DAG → Execute

# Option 2: Via CLI
docker exec crypto-pipeline-airflow-scheduler-1 \
  airflow dags trigger historical_ingestion

# Wait 2-3 minutes for execution
```

### Step 4: Start Streamlit Dashboard

```bash
# In a new terminal
streamlit run streamlit/app.py

# Opens at http://localhost:8501
```

### Step 5: Verify Data

```bash
# Run database verification script
python show_database_proof.py

# Expected output shows:
# ✅ 5,603 price records
# ✅ 3 cryptocurrencies
# ✅ 0 NULL values
# ✅ 0 duplicates
```

---

## 💡 Usage

### Running the Full Pipeline

**Daily Automatic Run:**
```
Pipeline automatically runs daily at 00:00 UTC
Monitor progress in Airflow UI: http://localhost:8080
```

**Manual Trigger:**
```bash
# Method 1: Airflow CLI
docker exec crypto-pipeline-airflow-scheduler-1 \
  airflow dags trigger historical_ingestion

# Method 2: Python script
python << 'EOF'
from airflow.utils import db
from airflow import DAG, settings
from airflow.models import DagModel

# Get DAG and trigger run
session = settings.Session()
dag = session.query(DagModel).filter(DagModel.dag_id == 'historical_ingestion').first()
dag.set_is_paused(False)
session.commit()
EOF
```

### Querying the Database

**Method 1: Python (Recommended)**
```bash
python show_database_proof.py
```

**Method 2: Python REPL**
```python
import duckdb

con = duckdb.connect('data/warehouse/crypto.duckdb')

# Query samples
coins = con.execute("SELECT * FROM main_gold.dim_coin").fetchall()
prices = con.execute("SELECT * FROM main_gold.fact_price_ohlcv LIMIT 10").df()

con.close()
```

**Method 3: dbt CLI**
```bash
cd dbt

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Accessing the Dashboard

```bash
streamlit run streamlit/app.py
```

Then navigate to: **http://localhost:8501**

**Pages:**
- 🏠 **app.py**: Home page with KPI cards
- 📊 **1_price_analysis.py**: Price charts and indicators
- 🔗 **2_correlation.py**: Correlation analysis

---

## 🔄 Pipeline Details

### Daily Execution Flow

```
00:00 UTC: Scheduler triggers historical_ingestion
    │
    ├─► Task 1: load_ohlcv_to_silver (30s)
    │   ├─ Read CSV files from data/raw/
    │   ├─ Validate columns and data types
    │   ├─ Load 5,603 rows to silver.ohlcv
    │   └─ Log: ✓ Bitcoin: 2,991 rows | Ethereum: 2,160 | Solana: 452
    │
    ├─► Task 2: load_metadata_to_silver (10s)
    │   ├─ Create coin metadata (hardcoded)
    │   └─ Load to silver.coin_metadata
    │
    ├─► Task 3: verify_silver (5s)
    │   ├─ Count rows: silver.ohlcv >= 100
    │   ├─ Count rows: silver.coin_metadata >= 1
    │   └─ Quality checks pass ✓
    │
    ├─► Task 4: dbt_run_all_models (45s)
    │   ├─ stg_coin_prices (staging)
    │   ├─ int_price_enriched (intermediate)
    │   ├─ dim_coin (dimension)
    │   ├─ dim_date (dimension)
    │   └─ fact_price_ohlcv (fact table)
    │
    └─► Task 5: dbt_test_all_models (20s)
        ├─ NOT NULL checks
        ├─ Uniqueness constraints
        ├─ Referential integrity
        └─ All tests pass ✓

Total Duration: ~2 minutes
Pipeline Status: ✅ SUCCESS
```

### Task Configuration

| Task | Type | Duration | Retries | Timeout |
|------|------|----------|---------|---------|
| load_ohlcv_to_silver | PythonOperator | 30s | 1 | 5m |
| load_metadata_to_silver | PythonOperator | 10s | 1 | 5m |
| verify_silver | PythonOperator | 5s | 1 | 5m |
| dbt_run_all_models | BashOperator | 45s | 0 | 10m |
| dbt_test_all_models | BashOperator | 20s | 0 | 10m |

---

## 📊 Data Model

### Star Schema (Gold Layer)

#### **fact_price_ohlcv** (Central Fact Table)
```sql
CREATE TABLE main_gold.fact_price_ohlcv (
    price_sk BIGINT PRIMARY KEY,              -- Surrogate key
    coin_sk BIGINT REFERENCES dim_coin,       -- Foreign key
    date_id INTEGER REFERENCES dim_date,      -- Foreign key
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    daily_range DOUBLE,
    daily_return_pct DOUBLE,
    price_change DOUBLE,
    ma_7d DOUBLE,                            -- 7-day moving average
    ma_30d DOUBLE,                           -- 30-day moving average
    source_system VARCHAR,
    extracted_at TIMESTAMP
);
-- Rows: 5,603
-- Indexed: coin_sk, date_id
```

#### **dim_coin** (Cryptocurrency Dimension)
```sql
CREATE TABLE main_gold.dim_coin (
    coin_sk BIGINT PRIMARY KEY,              -- MD5 hash surrogate key
    coin_id VARCHAR UNIQUE,
    symbol VARCHAR,
    name VARCHAR,
    categories VARCHAR,                      -- JSON array
    genesis_date DATE,
    hashing_algorithm VARCHAR
);
-- Rows: 3
-- Coins: Bitcoin, Ethereum, Solana
```

#### **dim_date** (Date Dimension Spine)
```sql
CREATE TABLE main_gold.dim_date (
    date_id INTEGER PRIMARY KEY,             -- YYYYMMDD format
    date DATE,
    year INTEGER,
    month INTEGER,                           -- 1-12
    quarter INTEGER,                         -- 1-4
    day_of_month INTEGER,                    -- 1-31
    day_name VARCHAR,                        -- Monday, Tuesday, etc
    is_weekend BOOLEAN,
    season VARCHAR                           -- Winter, Spring, etc
);
-- Rows: 6,574 (2013-01-01 to 2030-12-31)
```

### Query Examples

```sql
-- KPI: Total records
SELECT COUNT(*) as total_records
FROM main_gold.fact_price_ohlcv;
-- Result: 5,603

-- Price for Bitcoin in July 2021
SELECT 
    d.date,
    c.symbol,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.ma_7d,
    f.ma_30d
FROM main_gold.fact_price_ohlcv f
JOIN main_gold.dim_coin c ON f.coin_sk = c.coin_sk
JOIN main_gold.dim_date d ON f.date_id = d.date_id
WHERE c.coin_id = 'bitcoin' 
  AND EXTRACT(YEAR FROM d.date) = 2021
  AND EXTRACT(MONTH FROM d.date) = 7
ORDER BY d.date;

-- Correlation of daily returns
SELECT 
    c1.symbol,
    c2.symbol,
    CORR(f1.daily_return_pct, f2.daily_return_pct) as correlation
FROM main_gold.fact_price_ohlcv f1
JOIN main_gold.fact_price_ohlcv f2
    ON f1.date_id = f2.date_id
    AND f1.coin_sk < f2.coin_sk
JOIN main_gold.dim_coin c1 ON f1.coin_sk = c1.coin_sk
JOIN main_gold.dim_coin c2 ON f2.coin_sk = c2.coin_sk
GROUP BY c1.symbol, c2.symbol;
```

---

## 📈 Dashboard Features

### 🏠 Home Page (`app.py`)

**KPI Cards:**
- Total Records: 5,603
- Coins Tracked: 3 (BTC, ETH, SOL)
- Latest Date: 2021-07-06

**Data Source:** main_gold.fact_price_ohlcv

### 📊 Price Analysis (`pages/1_price_analysis.py`)

**Features:**
- Interactive candlestick chart
- Coin selector (dropdown)
- 7-day moving average (MA7)
- 30-day moving average (MA30)
- Daily return percentage distribution
- Price statistics (min, max, mean, std)

**Technologies:** Plotly, Pandas

### 🔗 Correlation Analysis (`pages/2_correlation.py`)

**Features:**
- Correlation heatmap (BTC vs ETH vs SOL)
- 30-day rolling correlation
- Period selector
- Real-time computation from database

**Technologies:** Plotly, NumPy, Pandas

---

## 🛠 Development

### Local Development Setup

```bash
# Activate virtual environment
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate      # Windows

# Install dev dependencies
pip install -r requirements.txt
pip install dbt-duckdb

# Create local DuckDB instance
duckdb data/warehouse/crypto.duckdb

# Run dbt models locally
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Code Organization

**Python Files:**
- `airflow/dags/ingestion_historical_dag.py` - Airflow DAG definition
- `streamlit/app.py` - Dashboard home page
- `streamlit/connection.py` - Database connection logic
- `show_database_proof.py` - Data verification script

**SQL Files (dbt models):**
- `dbt/models/staging/stg_coin_prices.sql` - Data cleaning
- `dbt/models/intermediate/int_price_enriched.sql` - Feature engineering
- `dbt/models/mart/dim_coin.sql` - Coin dimension
- `dbt/models/mart/dim_date.sql` - Date dimension
- `dbt/models/mart/fact_price_ohlcv.sql` - Fact table

### Testing

```bash
# Run dbt tests
cd dbt
dbt test

# Run Python tests (if added)
pytest

# Manual data verification
python show_database_proof.py
```

### Code Style

- **Python**: PEP 8 compliance
- **SQL**: Uppercase keywords, snake_case identifiers
- **dbt**: Modular models, comprehensive tests

---

## 🐛 Troubleshooting

### Docker Issues

**Problem: Containers won't start**
```bash
# Check Docker status
docker status
docker-compose logs airflow-scheduler

# Solution: Rebuild containers
docker-compose down -v
docker-compose up --build
```

**Problem: Port already in use**
```bash
# Find process using port
lsof -i :8080          # Airflow
lsof -i :8501          # Streamlit
lsof -i :5432          # Postgres

# Kill process
kill -9 <PID>
```

### Database Issues

**Problem: DuckDB is locked**
```bash
# Close all connections
pkill duckdb
pkill python

# Restart Airflow
docker-compose restart airflow-scheduler
```

**Problem: No data in database**
```bash
# Re-export from Docker
docker cp crypto-pipeline-airflow-scheduler-1:/opt/airflow/data/warehouse/crypto.duckdb \
  data/warehouse/crypto.duckdb

# Or trigger DAG manually
docker exec crypto-pipeline-airflow-scheduler-1 \
  airflow dags trigger historical_ingestion
```

### Pipeline Issues

**Problem: DAG not visible in Airflow UI**
```bash
# Check DAG syntax
python -m py_compile airflow/dags/ingestion_historical_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

**Problem: Tasks failing**
```bash
# Check logs
docker exec crypto-pipeline-airflow-scheduler-1 \
  tail -f /opt/airflow/logs/dag_id=*/run_id=*/task_id=*/attempt=1.log

# Or via Airflow UI: DAG → Task Instance → Log
```

### Streamlit Issues

**Problem: Dashboard won't load**
```bash
# Check if port is available
lsof -i :8501

# Restart Streamlit
pkill streamlit
streamlit run streamlit/app.py
```

**Problem: Database connection error**
```bash
# Verify database path
ls -la data/warehouse/crypto.duckdb

# Test connection
python << 'EOF'
import duckdb
con = duckdb.connect('data/warehouse/crypto.duckdb')
print(con.execute("SELECT COUNT(*) FROM main_gold.fact_price_ohlcv").fetchone())
EOF
```

---

## 📊 Project Statistics

### Data Volume
| Metric | Value |
|--------|-------|
| **Total OHLCV Records** | 5,603 |
| **Cryptocurrencies** | 3 (BTC, ETH, SOL) |
| **Date Dimensions** | 6,574 |
| **Data Span** | ~8 years (2013-2021) |
| **Database Size** | 3.9 MB |

### Data Distribution
| Coin | Records | Date Range | Duration |
|------|---------|-----------|----------|
| Bitcoin | 2,991 | 2013-04-29 → 2021-07-06 | ~8 years |
| Ethereum | 2,160 | 2015-08-08 → 2021-07-06 | ~6 years |
| Solana | 452 | 2020-04-11 → 2021-07-06 | ~1 year |

### Data Quality
| Metric | Status |
|--------|--------|
| **NULL Values** | 0 ✅ |
| **Duplicates** | 0 ✅ |
| **Validation Tests** | All pass ✅ |
| **Data Integrity** | Excellent ✅ |

### Pipeline Performance
| Component | Duration | Status |
|-----------|----------|--------|
| Load OHLCV | 30s | ✅ |
| Load Metadata | 10s | ✅ |
| Verify Silver | 5s | ✅ |
| dbt Run Models | 45s | ✅ |
| dbt Run Tests | 20s | ✅ |
| **Total** | **~2 min** | **✅** |

---

## 📚 Lessons Learned

### ✅ What Went Well

1. **Medallion Architecture** provides clear separation of concerns
2. **dbt** makes SQL transformations version-controlled and testable
3. **DuckDB** is fast and efficient for OLAP without cloud costs
4. **Sequential task execution** prevents database lock conflicts
5. **Docker containers** ensure reproducible environments
6. **Streamlit** enables rapid dashboard iteration

### ⚠️ Challenges Overcome

1. **API Rate Limits**: Pivoted from live APIs to CSV ingestion
2. **DuckDB Locking**: Changed from parallel to sequential tasks
3. **Schema Discovery**: Implemented dynamic schema detection
4. **Data Type Handling**: Added comprehensive pandas validation
5. **Moving Averages**: Used window functions for efficient calculation
6. **Charmap Encoding**: Set PYTHONUTF8=1 environment variable

### 🔍 Key Insights

1. **Incremental Loading** can be added via `dbt_utils.date_spine`
2. **Cloud Migration** path: Replace DuckDB with Snowflake/BigQuery
3. **Real-time Updates** would need streaming architecture (Kafka)
4. **Scalability** is limited by CSV ingestion; APIs would enable growth
5. **Testing Framework** dbt provides comprehensive data quality checks

---

## 🚀 Future Enhancements

### Short-term (1-2 months)

- [ ] Add incremental load capability (append-only, not full refresh)
- [ ] Implement email alerts on DAG failure
- [ ] Add data profiling dashboard (data quality metrics)
- [ ] Create dbt documentation site
- [ ] Add unit tests for Python functions

### Medium-term (3-6 months)

- [ ] Migrate DuckDB to cloud DW (Snowflake)
- [ ] Add real-time streaming (Kafka → Spark)
- [ ] Implement machine learning models (price prediction)
- [ ] Add more cryptocurrencies (expand beyond 3)
- [ ] Create mobile-friendly dashboard version

### Long-term (6-12 months)

- [ ] Cloud deployment (AWS, GCP, Azure)
- [ ] Advanced analytics (factor models, backtesting)
- [ ] APIs for external consumers
- [ ] Governance & compliance (data lineage, audit logs)
- [ ] Advanced monitoring & alerting (Monte Carlo simulations)

---

## 📖 Documentation

- **[PROJECT.md](PROJECT.md)** - Detailed technical documentation
- **[PRESENTATION_GUIDE.md](PRESENTATION_GUIDE.md)** - Presentation walkthrough
- **[PROFESSOR_FACT_SHEET.txt](PROFESSOR_FACT_SHEET.txt)** - Quick reference
- **[DATABASE_PRESENTATION.html](DATABASE_PRESENTATION.html)** - Visual schema guide
- **dbt Docs** - `cd dbt && dbt docs generate && dbt docs serve`

---

## 🤝 Contributing

### How to Contribute

1. Fork the repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Make changes and commit: `git commit -am 'Add feature'`
4. Push to branch: `git push origin feature/your-feature`
5. Submit pull request

### Code Standards

- Follow PEP 8 for Python
- SQL: UPPERCASE keywords, snake_case names
- dbt: Follow conventions, add tests
- Include documentation for new features

### Reporting Issues

Please use GitHub Issues to report bugs. Include:
- Description of the issue
- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment details

---

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

---

## 👨‍💼 Author

Built as a **Data Architecture course project** in May 2026.

**Key Features Implemented:**
- Production-grade data pipeline
- Modern data stack (Airflow, dbt, DuckDB)
- Analytics dashboard with Streamlit
- Star schema data modeling
- Comprehensive documentation

---

## 📞 Support

For questions or issues:
1. Check [TROUBLESHOOTING](#troubleshooting) section
2. Review [PROJECT.md](PROJECT.md) for technical details
3. Check Airflow logs: `docker-compose logs airflow-scheduler`
4. Run verification script: `python show_database_proof.py`

---

## 🎓 Learning Resources

### Data Engineering
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

### Databases
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)
- [OLAP vs OLTP](https://en.wikipedia.org/wiki/Online_analytical_processing)

### Python & Visualization
- [Streamlit Docs](https://docs.streamlit.io/)
- [Plotly Reference](https://plotly.com/python/)
- [Pandas Guide](https://pandas.pydata.org/docs/)

---

**Last Updated:** May 16, 2026

**Status:** ✅ Production Ready

**Questions?** Check the documentation or run `python show_database_proof.py` to verify everything is working!