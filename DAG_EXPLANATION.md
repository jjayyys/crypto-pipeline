# Airflow DAG Detailed Explanation: `historical_ingestion`

## 📋 Table of Contents
1. [DAG Overview](#dag-overview)
2. [DAG Configuration](#dag-configuration)
3. [Default Arguments](#default-arguments)
4. [Helper Functions](#helper-functions)
5. [Task Functions](#task-functions)
6. [Task Objects](#task-objects)
7. [Task Dependencies](#task-dependencies)
8. [Execution Flow](#execution-flow)
9. [Data Transformations](#data-transformations)
10. [Error Handling](#error-handling)

---

## DAG Overview

### What is a DAG?
A **DAG** (Directed Acyclic Graph) is the core concept in Apache Airflow. It represents:
- A workflow with multiple interconnected tasks
- Each task is a unit of work (Python function, Bash command, etc.)
- Tasks have dependencies - some must run before others
- No cycles allowed (you can't have Task A → Task B → Task A)

### Our DAG: `historical_ingestion`
**Purpose:** Load cryptocurrency OHLCV price data from CSV files → Validate in Silver layer → Transform with dbt → Create Gold analytics tables

**Workflow Type:** Historical data ingestion with validation and transformation

**Execution Pattern:** Daily automated run (configured with `@daily` schedule)

**Total Pipeline Duration:** ~2 minutes

---

## DAG Configuration

```python
with DAG(
    dag_id="historical_ingestion",
    description="Load CSV -> Silver -> dbt Gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "historical", "csv"],
) as dag:
```

### Configuration Parameters Explained

| Parameter | Value | Purpose |
|-----------|-------|---------|
| **dag_id** | `historical_ingestion` | Unique identifier for the DAG in Airflow UI |
| **description** | `Load CSV -> Silver -> dbt Gold` | Human-readable summary shown in UI |
| **start_date** | `2024-01-01` | First execution date; Airflow uses this to calculate schedule |
| **schedule_interval** | `@daily` | Run this DAG every day at midnight UTC |
| **default_args** | See below | Default settings applied to all tasks |
| **catchup** | `False` | Don't run missed executions if DAG is paused |
| **tags** | `["ingestion", "historical", "csv"]` | Categorization labels in Airflow UI |

---

## Default Arguments

```python
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}
```

### What Each Default Arg Does

| Argument | Value | Explanation |
|----------|-------|-------------|
| **owner** | `data-engineer` | Designates who is responsible for this DAG |
| **depends_on_past** | `False` | Each task run is independent; doesn't wait for previous runs |
| **retries** | `1` | If a task fails, retry it 1 additional time |
| **retry_delay** | `2 minutes` | Wait 2 minutes between retry attempts |
| **email_on_failure** | `False` | Don't send email notifications on failure |

### Applied To
These settings apply to ALL tasks in the DAG unless overridden at the task level.

---

## Helper Functions

### 1. `count_rows(con, table: str) -> int`

#### Function Signature
```python
def count_rows(con, table: str) -> int:
    """Helper: นับ rows และ return int เสมอ"""
    return con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]
```

#### Purpose
Execute a SQL COUNT query on a DuckDB table and return the result as an integer.

#### Parameters
| Parameter | Type | Description |
|-----------|------|-------------|
| **con** | DuckDB Connection | Active database connection to execute SQL |
| **table** | str | Table name (schema-qualified, e.g., `silver.ohlcv`) |

#### Return Value
- **Type:** `int`
- **Value:** Number of rows in the table
- **Example:** `5603`

#### How It Works

```
Step 1: con.execute(f"SELECT COUNT(*) FROM {table}")
        ↓
        Executes SQL: SELECT COUNT(*) FROM silver.ohlcv
        ↓
        Returns DuckDB result object

Step 2: .fetchone()
        ↓
        Gets first (and only) row: (5603,)
        ↓
        Returns as tuple

Step 3: [0]
        ↓
        Extracts first element from tuple
        ↓
        Returns: 5603
```

#### Why The `[0]` Subscript?
- DuckDB's `.fetchone()` returns a **tuple**, not a scalar
- `.fetchone()` on `SELECT COUNT(*)` returns `(5603,)`
- We need just the integer `5603`, not the tuple
- `[0]` extracts the first (and only) element

#### Used In
- `load_ohlcv_to_silver()` - Count rows after loading
- `verify_silver()` - Validate row counts meet minimum thresholds

---

## Task Functions

### 2. `load_ohlcv_to_silver(**context)`

#### Purpose
Load cryptocurrency OHLCV (Open, High, Low, Close, Volume) price data from CSV files into DuckDB's `silver.ohlcv` table.

#### Input Files
```
/opt/airflow/data/raw/
├── bitcoin.csv
├── ethereum.csv
└── solana.csv
```

#### Process Flow Diagram
```
┌─────────────────────────────────────────────────────────────┐
│                   load_ohlcv_to_silver()                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 1: Initialize Paths & Variables            │
    ├──────────────────────────────────────────────────┤
    │ • Get DuckDB path from env (default:             │
    │   /opt/airflow/data/warehouse/crypto.duckdb)     │
    │ • Define COIN_FILES mapping (bitcoin,            │
    │   ethereum, solana → filename)                   │
    │ • Initialize empty list for OHLCV data           │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 2: For Each Coin (bitcoin, ethereum, sol)  │
    ├──────────────────────────────────────────────────┤
    │  2.1 Read CSV File                               │
    │      └─ pandas.read_csv(filepath)                │
    │                                                   │
    │  2.2 Clean Column Names                          │
    │      └─ Strip whitespace: df.columns.str.strip() │
    │                                                   │
    │  2.3 Auto-Map Columns to Standard Names          │
    │      └─ Detect: date, open, high, low, close    │
    │      └─ Handle variations (Date vs date, etc)    │
    │                                                   │
    │  2.4 Validate Required Columns Exist             │
    │      └─ Must have: date, open, high, low, close │
    │      └─ If missing, skip this coin & log error   │
    │                                                   │
    │  2.5 Data Type Conversions                       │
    │      └─ date: String to datetime to YYYY-MM-DD   │
    │      └─ OHLC: Numeric (handle missing values)    │
    │                                                   │
    │  2.6 Data Cleaning                               │
    │      └─ Drop rows with NULL in required columns  │
    │      └─ Select only: date, open, high, low, close│
    │                                                   │
    │  2.7 Add Metadata Columns                        │
    │      └─ coin_id: bitcoin, ethereum, solana       │
    │      └─ source: "local_csv"                      │
    │      └─ extracted_at: Current UTC timestamp      │
    │                                                   │
    │  2.8 Sort by Date                                │
    │      └─ Chronological order (oldest first)       │
    │                                                   │
    │  2.9 Append to List                              │
    │      └─ Add processed coin data to all_ohlcv     │
    │                                                   │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 3: Validate Data Loaded                    │
    ├──────────────────────────────────────────────────┤
    │ • Check if any coin data was loaded              │
    │ • If all_ohlcv is empty, raise ValueError        │
    │   (no data found = pipeline error)               │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 4: Combine & Deduplicate                   │
    ├──────────────────────────────────────────────────┤
    │ • Concat all coin dataframes into one            │
    │ • Remove duplicates on (coin_id, date)           │
    │   (same coin, same date = keep only one)         │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 5: Connect to DuckDB                       │
    ├──────────────────────────────────────────────────┤
    │ • Connect to crypto.duckdb file                  │
    │ • Create silver schema if doesn't exist          │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 6: Load to DuckDB Table                    │
    ├──────────────────────────────────────────────────┤
    │ • Execute: CREATE OR REPLACE TABLE               │
    │   silver.ohlcv AS SELECT * FROM df_all           │
    │ • This overwrites the table on each run          │
    └──────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────────────┐
    │  STEP 7: Verify & Log                            │
    ├──────────────────────────────────────────────────┤
    │ • Count rows in silver.ohlcv                     │
    │ • Log success message with row count             │
    │ • Example: ✅ silver.ohlcv: 5,603 rows           │
    └──────────────────────────────────────────────────┘
```

#### Code Breakdown

##### Initialization
```python
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
```
- **`**context`**: Airflow context object with logging, task instance, etc.
- **imports**: Load required libraries
- **db_path**: Get DuckDB file path from environment variable, or use default

##### Define Coin Files
```python
    COIN_FILES = {
        "bitcoin":  "bitcoin.csv",
        "ethereum": "ethereum.csv",
        "solana":   "solana.csv",
    }
```
- Maps coin IDs to their CSV filenames
- This dictionary controls which coins are processed

##### Process Each Coin
```python
    all_ohlcv = []
    for coin_id, filename in COIN_FILES.items():
        filepath = raw_dir / filename
        if not filepath.exists():
            context["ti"].log.warning(f"Missing: {filepath}")
            continue
```
- Loop through each cryptocurrency
- Check if CSV file exists; skip if not (with warning log)

##### Read and Inspect
```python
        df = pd.read_csv(str(filepath))
        df.columns = df.columns.str.strip()
        context["ti"].log.info(
            f"{filename} columns: {list(df.columns)}"
        )
```
- Read CSV using Pandas
- Remove leading/trailing spaces from column names
- Log column names for debugging

##### Auto-Detect Columns
```python
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
```
- Maps CSV columns to standard OHLCV names
- Handles variations (Date, DATE, date)
- Filters out "Adj Close" if present
- Renames columns in dataframe

##### Validate Required Columns
```python
        required = ["date", "open", "high", "low", "close"]
        missing  = [c for c in required if c not in df.columns]
        if missing:
            context["ti"].log.error(
                f"{filename}: missing columns {missing}"
            )
            continue
```
- Check if all required columns exist after mapping
- If any missing, log error and skip this coin

##### Data Type Conversions
```python
        df["date"] = pd.to_datetime(
            df["date"]
        ).dt.strftime("%Y-%m-%d")

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
```
- **date**: Convert to datetime, then format as string "YYYY-MM-DD"
- **OHLC**: Convert to numeric type, coerce non-numeric to NaN

##### Clean Missing Values
```python
        df = df.dropna(subset=required)
        df = df[required].copy()
```
- Remove rows where any required column is NULL/NaN
- Keep only the 5 required columns

##### Add Metadata
```python
        df["coin_id"]      = coin_id
        df["source"]       = "local_csv"
        df["extracted_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        df = df.sort_values("date")
        all_ohlcv.append(df)
```
- Add coin_id (bitcoin, ethereum, solana)
- Mark source as "local_csv"
- Add extraction timestamp (ISO 8601 UTC format)
- Sort by date chronologically
- Append to list of all coin data

##### Logging
```python
        context["ti"].log.info(
            f"✅ {coin_id}: {len(df):,} rows "
            f"({df['date'].min()} → {df['date'].max()})"
        )
```
- Log success with formatted row count and date range

##### Validate Data Was Loaded
```python
    if not all_ohlcv:
        raise ValueError(
            "No OHLCV data! ตรวจสอบ CSV ใน /opt/airflow/data/raw/"
        )
```
- If no coin data was loaded, raise error
- Pipeline will fail (intended behavior - no data = error)

##### Combine Data
```python
    df_all = pd.concat(all_ohlcv, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["coin_id", "date"])
```
- Combine all coin dataframes into one
- Drop duplicate rows (same coin_id + date)

##### Create DuckDB Table
```python
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.ohlcv AS
        SELECT * FROM df_all
    """)
```
- Connect to DuckDB (creates file if doesn't exist)
- Create `silver` schema if missing
- Create/replace `silver.ohlcv` table with combined data

##### Verify & Log
```python
    row_count = count_rows(con, "silver.ohlcv")
    context["ti"].log.info(f"✅ silver.ohlcv: {row_count:,} rows")
```
- Count rows in the newly created table
- Log success with row count

#### Output Data
**Table:** `silver.ohlcv`

**Schema:**
```
┌─────────────┬──────────┐
│ Column      │ Type     │
├─────────────┼──────────┤
│ date        │ VARCHAR  │  YYYY-MM-DD format
│ open        │ DOUBLE   │  Price in USD
│ high        │ DOUBLE   │  Price in USD
│ low         │ DOUBLE   │  Price in USD
│ close       │ DOUBLE   │  Price in USD
│ coin_id     │ VARCHAR  │  bitcoin, ethereum, solana
│ source      │ VARCHAR  │  "local_csv"
│ extracted_at│ VARCHAR  │  ISO 8601 UTC timestamp
└─────────────┴──────────┘
```

**Example Data:**
```
date       | open  | high  | low   | close | coin_id  | source    | extracted_at
-----------|-------|-------|-------|-------|----------|-----------|------------------------
2013-01-01 | 13.50 | 13.66 | 13.51 | 13.51 | bitcoin  | local_csv | 2026-05-16T05:37:58Z
2013-01-02 | 13.56 | 13.95 | 13.50 | 13.92 | bitcoin  | local_csv | 2026-05-16T05:37:58Z
...
2021-07-06 | 34290 | 34873 | 32909 | 34201 | bitcoin  | local_csv | 2026-05-16T05:37:58Z
```

---

### 3. `load_metadata_to_silver(**context)`

#### Purpose
Load cryptocurrency metadata (name, symbol, genesis date, etc.) into DuckDB's `silver.coin_metadata` table.

#### Process Flow

```
┌──────────────────────────────────┐
│  load_metadata_to_silver()       │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 1: Define Metadata          │
│ 3 hardcoded dicts:               │
│ • Bitcoin: BTC, Layer 1, SHA-256 │
│ • Ethereum: ETH, Smart Contract, │
│ • Solana: SOL, Layer 1, PoH      │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 2: Create DataFrame         │
│ Convert list of dicts to         │
│ Pandas DataFrame                 │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 3: Add Extraction Timestamp │
│ extracted_at = now (UTC)         │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 4: Connect to DuckDB        │
│ Create silver schema if missing   │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 5: Load to Table            │
│ CREATE OR REPLACE silver.        │
│ coin_metadata                    │
└──────────────────────────────────┘
              ↓
┌──────────────────────────────────┐
│ STEP 6: Log Success              │
│ ✅ silver.coin_metadata: 3 rows  │
└──────────────────────────────────┘
```

#### Code Breakdown

##### Define Metadata
```python
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
```
- Hardcoded metadata for 3 cryptocurrencies
- Each dict contains metadata attributes
- Note: categories stored as string (not parsed)

##### Create DataFrame & Add Timestamp
```python
    df = pd.DataFrame(COIN_META)
    df["extracted_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
```
- Convert list of dicts to Pandas DataFrame
- Add extraction timestamp column (ISO 8601)

##### Load to DuckDB
```python
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
```
- Connect to DuckDB
- Create silver schema
- Load dataframe to table
- Log success
- Close connection

#### Output Data
**Table:** `silver.coin_metadata`

**Schema:**
```
┌────────────────────┬──────────┐
│ Column             │ Type     │
├────────────────────┼──────────┤
│ coin_id            │ VARCHAR  │
│ symbol             │ VARCHAR  │
│ name               │ VARCHAR  │
│ categories         │ VARCHAR  │
│ genesis_date       │ VARCHAR  │
│ hashing_algorithm  │ VARCHAR  │
│ extracted_at       │ VARCHAR  │
└────────────────────┴──────────┘
```

**Example Data:**
```
coin_id  | symbol | name      | categories                      | genesis_date | hashing_algorithm | extracted_at
---------|--------|-----------|--------------------------------|--------------|-------------------|------------------------
bitcoin  | BTC    | Bitcoin   | ['Cryptocurrency', 'Layer 1']  | 2009-01-03   | SHA-256          | 2026-05-16T05:37:58Z
ethereum | ETH    | Ethereum  | ['Cryptocurrency', 'Smart...] | 2015-07-30   | Ethash           | 2026-05-16T05:37:58Z
solana   | SOL    | Solana    | ['Cryptocurrency', 'Layer 1']  | 2020-03-16   | PoH              | 2026-05-16T05:37:58Z
```

---

### 4. `verify_silver(**context)`

#### Purpose
Validate that data was loaded correctly to the `silver` schema by checking row counts meet minimum thresholds.

#### Data Quality Checks
```
silver.ohlcv         >= 100 rows  (minimum)
silver.coin_metadata >= 1 row     (minimum)
```

#### Process Flow

```
┌────────────────────────────────┐
│    verify_silver()             │
└────────────────────────────────┘
              ↓
┌────────────────────────────────┐
│ STEP 1: Connect to DuckDB      │
└────────────────────────────────┘
              ↓
┌────────────────────────────────┐
│ STEP 2: For Each Table:        │
│ • silver.ohlcv (≥100 rows)     │
│ • silver.coin_metadata (≥1)    │
├────────────────────────────────┤
│  2.1 Execute: SELECT COUNT(*)  │
│      from table                │
│                                │
│  2.2 Extract row count         │
│      using .fetchone()[0]      │
│                                │
│  2.3 Check against minimum     │
│      if count < minimum:       │
│        raise ValueError()      │
│                                │
│  2.4 Log result                │
│      ✅ table: count rows      │
└────────────────────────────────┘
              ↓
┌────────────────────────────────┐
│ STEP 3: On Error               │
│ • Close connection             │
│ • Raise error (task fails)     │
└────────────────────────────────┘
              ↓
┌────────────────────────────────┐
│ STEP 4: On Success             │
│ • Close connection             │
│ • Log final success message    │
└────────────────────────────────┘
```

#### Code Breakdown

##### Define Checks
```python
    checks = {
        "silver.ohlcv":         100,
        "silver.coin_metadata": 1,
    }
```
- Dictionary mapping table names to minimum row counts
- `silver.ohlcv`: Must have at least 100 rows
- `silver.coin_metadata`: Must have at least 1 row

##### Execute Checks
```python
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
```
- Loop through each table check
- Count rows using SQL
- Compare against minimum
- If below minimum, raise error (task fails)
- If successful, log row count

##### Final Validation
```python
    con.close()
    context["ti"].log.info("✅ Silver verification passed!")
```
- Close database connection
- Log final success message

#### Purpose of Verification
1. **Data Quality:** Ensure data was loaded correctly
2. **Early Detection:** Fail fast if something went wrong
3. **Dependencies:** Next tasks know silver layer is valid
4. **Monitoring:** Log row counts for pipeline health

---

## Task Objects

Tasks are objects that define work units in the DAG. Here are the 5 task objects:

### Task 1: `t_ohlcv` - Load OHLCV Data
```python
t_ohlcv = PythonOperator(
    task_id="load_ohlcv_to_silver",
    python_callable=load_ohlcv_to_silver,
)
```

**What is PythonOperator?**
- Airflow operator that runs Python functions
- Calls the function at execution time
- Manages logging, retry, error handling

**Task Properties:**
- **task_id:** Unique identifier within DAG
- **python_callable:** Function to execute (no parentheses)

**Execution Behavior:**
- Runs: `load_ohlcv_to_silver(**context)`
- Duration: ~30 seconds
- Returns: None
- Logs: Row counts, column mapping, date ranges

---

### Task 2: `t_metadata` - Load Metadata
```python
t_metadata = PythonOperator(
    task_id="load_metadata_to_silver",
    python_callable=load_metadata_to_silver,
)
```

**Task Properties:**
- **task_id:** `load_metadata_to_silver`
- **python_callable:** `load_metadata_to_silver` function

**Execution Behavior:**
- Runs: `load_metadata_to_silver(**context)`
- Duration: ~10 seconds
- Returns: None
- Logs: Metadata row count (3 rows)

---

### Task 3: `t_verify` - Verify Silver Layer
```python
t_verify = PythonOperator(
    task_id="verify_silver",
    python_callable=verify_silver,
)
```

**Task Properties:**
- **task_id:** `verify_silver`
- **python_callable:** `verify_silver` function

**Execution Behavior:**
- Runs: `verify_silver(**context)`
- Duration: ~5 seconds
- Returns: None (or raises error on failed checks)
- Logs: Row counts for each table

---

### Task 4: `t_dbt_run` - Run dbt Models
```python
t_dbt_run = BashOperator(
    task_id="dbt_run_all_models",
    bash_command=(
        "cd /opt/airflow/dbt && "
        "PYTHONUTF8=1 dbt run --profiles-dir ."
    ),
)
```

**What is BashOperator?**
- Airflow operator that runs shell commands
- Executes bash scripts or commands

**Task Properties:**
- **task_id:** `dbt_run_all_models`
- **bash_command:** Shell command to execute

**The Command:**
```bash
cd /opt/airflow/dbt && PYTHONUTF8=1 dbt run --profiles-dir .
```

Breaking it down:
- `cd /opt/airflow/dbt` → Change to dbt directory
- `&&` → Only proceed if cd succeeds
- `PYTHONUTF8=1` → Set environment variable for UTF-8 encoding
- `dbt run` → Execute dbt transformation models
- `--profiles-dir .` → Use profiles.yml in current directory

**Execution Behavior:**
- Runs 5 dbt models: stg_coin_prices, int_price_enriched, dim_coin, dim_date, fact_price_ohlcv
- Duration: ~45 seconds
- Output: Logs in dbt/logs/
- Creates/updates tables in gold schema

**dbt Models Executed:**
1. `stg_coin_prices` - Clean raw data
2. `int_price_enriched` - Add calculations
3. `dim_coin` - Create coin dimension
4. `dim_date` - Create date dimension
5. `fact_price_ohlcv` - Create main fact table

---

### Task 5: `t_dbt_test` - Test dbt Models
```python
t_dbt_test = BashOperator(
    task_id="dbt_test_all_models",
    bash_command=(
        "cd /opt/airflow/dbt && "
        "PYTHONUTF8=1 dbt test --profiles-dir ."
    ),
)
```

**Task Properties:**
- **task_id:** `dbt_test_all_models`
- **bash_command:** dbt test command

**The Command:**
```bash
cd /opt/airflow/dbt && PYTHONUTF8=1 dbt test --profiles-dir .
```

**Execution Behavior:**
- Runs data quality tests on all models
- Duration: ~20 seconds
- Output: Test results in logs
- Fails if any test doesn't pass

**Tests Defined In:**
- `dbt/models/mart/schema.yml` (see fact_price_ohlcv tests)

**Example Tests:**
- Check for NULL values
- Verify foreign key relationships
- Validate data types
- Check row counts

---

## Task Dependencies

### Execution Order

The task dependencies are defined by:
```python
t_ohlcv >> t_metadata >> t_verify >> t_dbt_run >> t_dbt_test
```

### Dependency Graph

```
t_ohlcv
   ↓
t_metadata
   ↓
t_verify
   ↓
t_dbt_run
   ↓
t_dbt_test
```

### Sequential vs Parallel

**Why Sequential?**

The `>>` operator creates a **sequential dependency** (one after another).

Instead of parallel execution like this (❌ WRONG):
```
t_ohlcv ─┐
         ├─ parallel → t_dbt_run
t_metadata ┘
```

We use sequential (✅ CORRECT):
```
t_ohlcv → t_metadata → t_verify → t_dbt_run → t_dbt_test
```

**Reason: DuckDB Lock Conflicts**

DuckDB uses file-based locking. If multiple processes write simultaneously:
- Process 1: `silver.ohlcv` write
- Process 2: `silver.coin_metadata` write
- Result: Lock conflicts, race conditions, data corruption

**Sequential execution prevents this:**
- Only one process writes at a time
- Prevents file lock conflicts
- Ensures data consistency

---

## Execution Flow

### Step-by-Step Execution Timeline

#### 1. Task: `load_ohlcv_to_silver` (t_ohlcv)
```
Time: 00:00-00:30 (30 seconds)

Actions:
  ├─ Read bitcoin.csv (2,991 rows)
  ├─ Read ethereum.csv (2,160 rows)
  ├─ Read solana.csv (452 rows)
  ├─ Clean & validate data
  ├─ Remove duplicates
  └─ Create silver.ohlcv table (5,603 rows)

Logs:
  ✅ bitcoin: 2,991 rows (2013-01-01 → 2021-07-04)
  ✅ ethereum: 2,160 rows (2015-07-30 → 2021-07-04)
  ✅ solana: 452 rows (2020-01-01 → 2021-07-04)
  ✅ silver.ohlcv: 5,603 rows

Database State:
  silver.ohlcv → 5,603 rows
```

#### 2. Task: `load_metadata_to_silver` (t_metadata)
```
Time: 00:30-00:40 (10 seconds)

Actions:
  ├─ Create DataFrame with 3 coin metadata
  ├─ Add extraction timestamp
  └─ Create silver.coin_metadata table

Logs:
  ✅ silver.coin_metadata: 3 rows

Database State:
  silver.ohlcv → 5,603 rows
  silver.coin_metadata → 3 rows
```

#### 3. Task: `verify_silver` (t_verify)
```
Time: 00:40-00:45 (5 seconds)

Actions:
  ├─ Check silver.ohlcv >= 100 rows
  │   └─ Count: 5,603 rows ✅ PASS
  ├─ Check silver.coin_metadata >= 1 row
  │   └─ Count: 3 rows ✅ PASS
  └─ Log final success

Logs:
  ✅ silver.ohlcv: 5,603 rows
  ✅ silver.coin_metadata: 3 rows
  ✅ Silver verification passed!

If Fails:
  ❌ Task fails with error
  Pipeline stops (no dbt execution)
```

#### 4. Task: `dbt_run_all_models` (t_dbt_run)
```
Time: 00:45-01:30 (45 seconds)

Actions:
  ├─ Parse dbt models
  ├─ Build dependency graph
  ├─ Run stg_coin_prices
  │   └─ Creates temporary staging table
  ├─ Run int_price_enriched
  │   └─ Creates intermediate enriched data
  ├─ Run dim_coin
  │   └─ Creates gold.dim_coin (3 rows)
  ├─ Run dim_date
  │   └─ Creates gold.dim_date (6,574 rows)
  └─ Run fact_price_ohlcv
      └─ Creates gold.fact_price_ohlcv (5,603 rows)

Logs:
  21 created model stg_coin_prices
  22 created model int_price_enriched
  23 created model dim_coin
  24 created model dim_date
  25 created model fact_price_ohlcv

Database State:
  gold.dim_coin → 3 rows
  gold.dim_date → 6,574 rows
  gold.fact_price_ohlcv → 5,603 rows
  (staging/intermediate tables created as temporary)
```

#### 5. Task: `dbt_test_all_models` (t_dbt_test)
```
Time: 01:30-01:50 (20 seconds)

Actions:
  ├─ Run data quality tests
  ├─ Test: fact_price_ohlcv has no NULLs
  ├─ Test: Foreign key relationships valid
  ├─ Test: Row counts reasonable
  └─ Verify all tests pass

Logs:
  1 pass fact_price_ohlcv.not_null...
  2 pass fact_price_ohlcv.relationships...
  ... (more tests)

Final Status:
  ✅ All tests passed
  Pipeline complete!
```

### Total Duration
```
t_ohlcv (30s) + t_metadata (10s) + t_verify (5s) + 
t_dbt_run (45s) + t_dbt_test (20s) = ~110 seconds (~2 minutes)
```

---

## Data Transformations

### Data Flow Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     RAW DATA LAYER                          │
├─────────────────────────────────────────────────────────────┤
│  /opt/airflow/data/raw/                                     │
│  ├─ bitcoin.csv    (2,991 rows)                             │
│  ├─ ethereum.csv   (2,160 rows)                             │
│  └─ solana.csv     (452 rows)                               │
└─────────────────────────────────────────────────────────────┘
              ↓ (load_ohlcv_to_silver)
┌─────────────────────────────────────────────────────────────┐
│                    SILVER DATA LAYER                        │
│              (Bronze → Silver: Cleaned Data)                │
├─────────────────────────────────────────────────────────────┤
│  DuckDB silver schema:                                      │
│  ├─ silver.ohlcv (5,603 rows)                              │
│  │   Columns: date, open, high, low, close, coin_id,       │
│  │            source, extracted_at                         │
│  │   Purpose: Raw prices cleaned & standardized            │
│  │                                                          │
│  └─ silver.coin_metadata (3 rows)                          │
│      Columns: coin_id, symbol, name, categories,           │
│               genesis_date, hashing_algorithm              │
│      Purpose: Master data for cryptocurrencies             │
└─────────────────────────────────────────────────────────────┘
              ↓ (dbt run)
┌─────────────────────────────────────────────────────────────┐
│                     GOLD DATA LAYER                         │
│         (Silver → Gold: Transformed Analytics Data)         │
├─────────────────────────────────────────────────────────────┤
│  DuckDB gold schema:                                        │
│                                                              │
│  Dimensions:                                                │
│  ├─ gold.dim_coin (3 rows)                                 │
│  │   Surrogate keys, coin metadata                         │
│  │                                                          │
│  └─ gold.dim_date (6,574 rows)                             │
│      Date spine, calendar attributes                       │
│                                                              │
│  Facts:                                                     │
│  └─ gold.fact_price_ohlcv (5,603 rows)                     │
│      ├─ Foreign keys to dimensions                         │
│      ├─ Calculated fields (MA7, MA30, daily_return_pct)    │
│      └─ Ready for analytics & dashboards                  │
└─────────────────────────────────────────────────────────────┘
              ↓ (Streamlit Dashboard)
┌─────────────────────────────────────────────────────────────┐
│                ANALYTICS & VISUALIZATION                    │
├─────────────────────────────────────────────────────────────┤
│  ├─ Home: KPI cards (5,603 records, 3 coins, latest date)  │
│  ├─ Price Analysis: Candlestick charts, moving averages    │
│  └─ Correlation: Heatmaps, rolling correlation             │
└─────────────────────────────────────────────────────────────┘
```

### Schema Evolution

#### Raw Data (from CSVs)
```
bitcoin.csv:
  Date, Open, High, Low, Close, ...
  2013-01-01, 13.50, 13.66, 13.51, 13.51

ethereum.csv:
  Date, Open, High, Low, Close, ...
  2015-07-30, 1.00, 1.00, 0.50, 0.53

solana.csv:
  Date, Open, High, Low, Close, ...
  2020-01-01, 0.0394, 0.0430, 0.0394, 0.0405
```

#### Silver Layer (Cleaned & Standardized)
```
silver.ohlcv:
  date       | open  | high  | low   | close | coin_id  | source    | extracted_at
  -----------|-------|-------|-------|-------|----------|-----------|---
  2013-01-01 | 13.50 | 13.66 | 13.51 | 13.51 | bitcoin  | local_csv | 2026-05-16T...
  2015-07-30 | 1.00  | 1.00  | 0.50  | 0.53  | ethereum | local_csv | 2026-05-16T...
  2020-01-01 | 0.039 | 0.043 | 0.039 | 0.040 | solana   | local_csv | 2026-05-16T...

silver.coin_metadata:
  coin_id  | symbol | name      | genesis_date | hashing_algorithm | ...
  ---------|--------|-----------|--------------|-------------------|---
  bitcoin  | BTC    | Bitcoin   | 2009-01-03   | SHA-256          | ...
  ethereum | ETH    | Ethereum  | 2015-07-30   | Ethash           | ...
  solana   | SOL    | Solana    | 2020-03-16   | PoH              | ...
```

#### Gold Layer (Transformed Analytics)

**dim_coin:**
```
coin_key | coin_id  | symbol | name      | genesis_date
---------|----------|--------|-----------|----------
  1      | bitcoin  | BTC    | Bitcoin   | 2009-01-03
  2      | ethereum | ETH    | Ethereum  | 2015-07-30
  3      | solana   | SOL    | Solana    | 2020-03-16
```

**dim_date:**
```
date_key | date       | year | month | day | day_of_week | week_of_year | ...
---------|------------|------|-------|-----|-------------|--------------|----
  1      | 2013-01-01 | 2013 | 1     | 1   | 2 (Tues)   | 1            | ...
  2      | 2013-01-02 | 2013 | 1     | 2   | 3 (Wed)    | 1            | ...
  ...
  6574   | 2030-12-31 | 2030 | 12    | 31  | 5 (Fri)    | 52           | ...
```

**fact_price_ohlcv:**
```
price_id | date_key | coin_key | open   | high   | low    | close  | ma7  | ma30 | daily_return_pct
---------|----------|----------|--------|--------|--------|--------|------|------|------------------
  1      | 1        | 1        | 13.50  | 13.66  | 13.51  | 13.51  | NULL | NULL | NULL
  2      | 2        | 1        | 13.56  | 13.95  | 13.50  | 13.92  | NULL | NULL | 0.030
  ...
  5603   | 6574     | 1        | 34290  | 34873  | 32909  | 34201  | 33412| 32106| 1.254
```

### Transformation Rules

#### Column Mapping (Raw → Silver)
```
Raw CSV Columns          Silver Columns    Transformation
─────────────────────────────────────────────────────────
Date/date/DATE         → date             Standardize to YYYY-MM-DD
Open/OPEN              → open             Convert to numeric
High/HIGH              → high             Convert to numeric
Low/LOW                → low              Convert to numeric
Close/CLOSE            → close            Exclude "Adj Close"
(new)                  → coin_id          Map from filename
(new)                  → source           Set to "local_csv"
(new)                  → extracted_at     Current UTC timestamp
```

#### Data Type Conversions
```
date:   String (various formats) → String (YYYY-MM-DD)
OHLC:   Mixed (string, numeric)  → Numeric (Double precision)
coin_id:Derived from filename    → String
source: Hardcoded                → String
extracted_at: Timestamp          → String (ISO 8601)
```

#### dbt Transformations (Silver → Gold)

**stg_coin_prices** (Staging):
```sql
-- Selects required columns, validates data types
SELECT
  date,
  CAST(open AS DOUBLE) as open,
  CAST(high AS DOUBLE) as high,
  CAST(low AS DOUBLE) as low,
  CAST(close AS DOUBLE) as close,
  coin_id,
  extracted_at,
  CASE WHEN open > 0 THEN 'valid' ELSE 'invalid' END as data_quality_flag
FROM silver.ohlcv
```

**int_price_enriched** (Intermediate - Calculations):
```sql
-- Add moving averages and returns
SELECT
  date,
  open, high, low, close,
  coin_id,
  ROUND(AVG(close) OVER (
    PARTITION BY coin_id 
    ORDER BY date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) as ma7,
  ROUND(AVG(close) OVER (
    PARTITION BY coin_id 
    ORDER BY date 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 2) as ma30,
  ROUND(
    ((close - LAG(close) OVER (PARTITION BY coin_id ORDER BY date)) 
     / LAG(close) OVER (PARTITION BY coin_id ORDER BY date)) * 100,
    2
  ) as daily_return_pct
FROM stg_coin_prices
```

**dim_coin** (Dimension):
```sql
-- Create coin dimension with surrogate keys
SELECT
  ROW_NUMBER() OVER (ORDER BY coin_id) as coin_key,
  coin_id,
  symbol,
  name,
  genesis_date,
  hashing_algorithm,
  extracted_at
FROM silver.coin_metadata
```

**dim_date** (Dimension):
```sql
-- Create date dimension spine (2013-2030)
SELECT
  ROW_NUMBER() OVER (ORDER BY calendar_date) as date_key,
  calendar_date as date,
  YEAR(calendar_date) as year,
  MONTH(calendar_date) as month,
  DAY(calendar_date) as day,
  DAYOFWEEK(calendar_date) as day_of_week,
  WEEKOFYEAR(calendar_date) as week_of_year,
  ...
FROM dbt_utils.generate_series(
  '2013-01-01'::DATE,
  '2030-12-31'::DATE,
  INTERVAL 1 DAY
)
```

**fact_price_ohlcv** (Fact Table):
```sql
-- Central fact table joining dimensions
SELECT
  ROW_NUMBER() OVER (ORDER BY date, coin_id) as price_id,
  dd.date_key,
  dc.coin_key,
  f.open,
  f.high,
  f.low,
  f.close,
  f.ma7,
  f.ma30,
  f.daily_return_pct
FROM int_price_enriched f
LEFT JOIN dim_date dd ON f.date = dd.date
LEFT JOIN dim_coin dc ON f.coin_id = dc.coin_id
WHERE f.date BETWEEN '2013-01-01' AND '2030-12-31'
```

---

## Error Handling

### Error Types & Recovery

#### 1. Missing CSV File
```python
if not filepath.exists():
    context["ti"].log.warning(f"Missing: {filepath}")
    continue
```
- **Severity:** Warning (not fatal)
- **Recovery:** Skip that coin, continue with others
- **Example:** bitcoin.csv missing → load ethereum & solana only

#### 2. Invalid Column Names
```python
missing = [c for c in required if c not in df.columns]
if missing:
    context["ti"].log.error(
        f"{filename}: missing columns {missing}"
    )
    continue
```
- **Severity:** Error (coin skipped)
- **Recovery:** Skip coin, process others
- **Example:** CSV has "Prices" instead of "close" → skip

#### 3. No Data Loaded
```python
if not all_ohlcv:
    raise ValueError(
        "No OHLCV data! ตรวจสอบ CSV ใน /opt/airflow/data/raw/"
    )
```
- **Severity:** Fatal (task fails)
- **Recovery:** DAG stops, all tasks fail
- **Example:** All 3 CSVs missing → pipeline error

#### 4. Data Quality Check Failure
```python
if n < min_rows:
    raise ValueError(
        f"{table}: {n} rows (ต้องการ >= {min_rows})"
    )
```
- **Severity:** Fatal (verify_silver task fails)
- **Recovery:** DAG stops, dbt doesn't run
- **Example:** silver.ohlcv only has 50 rows (needs ≥100)

#### 5. dbt Model Failure
```bash
dbt run --profiles-dir .
```
- **Severity:** Fatal (task fails)
- **Recovery:** DAG stops, tests don't run
- **Example:** dbt SQL syntax error → pipeline error

#### 6. dbt Test Failure
```bash
dbt test --profiles-dir .
```
- **Severity:** Fatal (task fails)
- **Recovery:** DAG stops
- **Example:** NULL values found in fact table → pipeline error

### Retry Behavior

From `default_args`:
```python
"retries": 1,
"retry_delay": timedelta(minutes=2),
```

**Retry Rules:**
- Each task gets 1 retry on failure
- Wait 2 minutes between retry and re-execution
- If 2nd attempt also fails, task is marked failed
- Dependent tasks don't execute

**Example Timeline (on failure):**
```
00:00 - t_ohlcv starts
00:05 - t_ohlcv fails (error in load)
↓
        Airflow detects failure
↓
        Waits 2 minutes
↓
00:07 - t_ohlcv retries
00:12 - t_ohlcv fails again
↓
        Mark as failed
        t_metadata doesn't run (dependency not met)
        t_verify doesn't run
        t_dbt_run doesn't run
        t_dbt_test doesn't run
```

### Logging & Monitoring

**Key Logs:**
```
context["ti"].log.info()    → Blue logs (information)
context["ti"].log.warning() → Yellow logs (non-fatal issues)
context["ti"].log.error()   → Red logs (errors, still processes)
raise ValueError()          → Red alert (fatal, task fails)
```

**Log Locations:**
```
Airflow UI:
  Dags → historical_ingestion → [run_id] → Task → Logs tab

Filesystem:
  airflow/logs/dag_id=historical_ingestion/
    run_id=manual__2026-05-16.../
      task_id=load_ohlcv_to_silver/
        attempt=1.log
```

---

## Summary

This DAG implements a **3-layer data pipeline:**

```
┌─────────────────────────────────────────────┐
│ RAW: Load from CSV files                    │ ← Human/External
├─────────────────────────────────────────────┤
│ SILVER: Validate & Standardize              │ ← Quality Check
├─────────────────────────────────────────────┤
│ GOLD: Transform for Analytics               │ ← dbt Automation
├─────────────────────────────────────────────┤
│ TESTS: Verify Data Quality                  │ ← Quality Assurance
└─────────────────────────────────────────────┘
```

**Each component serves a specific purpose:**
- **load_ohlcv_to_silver**: Raw data acquisition & cleaning
- **load_metadata_to_silver**: Master data management
- **verify_silver**: Quality gates before transformation
- **dbt_run_all_models**: Business logic & analytics
- **dbt_test_all_models**: Data validation & monitoring

**Execution guarantees:**
- ✅ Sequential to prevent conflicts
- ✅ Automatic daily runs
- ✅ Retries on failure
- ✅ Comprehensive logging
- ✅ Data quality validation
