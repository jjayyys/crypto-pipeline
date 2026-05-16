# dbt (Data Build Tool) Explanation

## 📚 Table of Contents
1. [What is dbt?](#what-is-dbt)
2. [dbt in This Project](#dbt-in-this-project)
3. [How dbt Works](#how-dbt-works)
4. [Project Configuration](#project-configuration)
5. [The 5 dbt Models](#the-5-dbt-models)
6. [Medallion Architecture](#medallion-architecture)
7. [Data Quality Tests](#data-quality-tests)
8. [Execution & Outputs](#execution--outputs)
9. [dbt vs Traditional ETL](#dbt-vs-traditional-etl)

---

## What is dbt?

### Definition
**dbt** (data build tool) is a Python-based framework that transforms data in a data warehouse using **SQL SELECT statements**.

### Key Principle
> "All transformations are just SQL SELECT statements"

Instead of writing complex ETL code, dbt lets you:
- Write transformation logic in SQL
- Manage dependencies between transformations
- Test data quality automatically
- Version control your data transformations
- Generate documentation

### dbt's Core Philosophy
```
Raw Data → SELECT ... FROM → Transformed Data
          (using SQL)

No Python/Pandas/Spark needed for transformations!
Just SQL.
```

### What dbt Does NOT Do
- ❌ Extract data (that's Airflow's job)
- ❌ Move data between systems (that's Airflow's job)
- ❌ Schedule tasks (that's Airflow's job)

### What dbt DOES Do
- ✅ Transform data using SQL
- ✅ Create tables/views from SELECT statements
- ✅ Manage dependencies between models
- ✅ Test data quality
- ✅ Generate documentation
- ✅ Run transformations incrementally

---

## dbt in This Project

### dbt's Role in Pipeline

```
┌─────────────────────────────────────────────────────┐
│  AIRFLOW (load & ingest)                            │
├─────────────────────────────────────────────────────┤
│  Raw CSVs → Silver Schema (cleaned data)            │
│  (Airflow handles extraction & loading)             │
└─────────────────────────────────────────────────────┘
                        ↓
        (Airflow calls dbt to transform)
                        ↓
┌─────────────────────────────────────────────────────┐
│  DBT (transform & validate)                         │
├─────────────────────────────────────────────────────┤
│  Silver Schema → Gold Schema (analytics-ready)      │
│  (dbt handles transformation & testing)             │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  STREAMLIT (visualize)                              │
├─────────────────────────────────────────────────────┤
│  Gold Schema → Interactive Dashboard                │
│  (Streamlit reads from transformed data)            │
└─────────────────────────────────────────────────────┘
```

### Responsibilities Split

| Component | Responsibility | Technology |
|-----------|---|---|
| **Airflow** | Extract, Load, Schedule | Python, Bash |
| **dbt** | Transform, Test, Document | SQL |
| **Streamlit** | Visualize, Analyze | Python |

---

## How dbt Works

### dbt Workflow

```
Step 1: Parse
  ├─ Read all .sql files in models/
  ├─ Read profiles.yml for database connection
  ├─ Read dbt_project.yml for configuration
  └─ Build dependency graph

Step 2: Plan
  ├─ Determine execution order
  ├─ Check for circular dependencies
  ├─ Calculate what needs to run
  └─ Generate execution DAG

Step 3: Run
  ├─ Execute each model (run SELECT statement)
  ├─ Create table/view in database
  ├─ Execute in dependency order
  ├─ Log progress & results
  └─ Handle errors with retries

Step 4: Test
  ├─ Run data quality tests
  ├─ Check constraints (unique, not_null, etc)
  ├─ Verify relationships (foreign keys)
  ├─ Report test results
  └─ Fail if critical tests don't pass

Step 5: Document
  ├─ Generate HTML documentation site
  ├─ Include model descriptions
  ├─ Include column documentation
  ├─ Show data lineage (what depends on what)
  └─ Create interactive schema viewer
```

### dbt Model Execution

When you run `dbt run`, dbt:

1. **Parses** all .sql files
2. **Compiles** them (substitutes `{{ ref() }}` with actual table names)
3. **Executes** SELECT statement to build table/view
4. **Persists** results to database (table or view based on config)

#### Example: How `stg_coin_prices` Gets Built

```yaml
# Configuration (in dbt_project.yml)
models:
  crypto_pipeline:
    staging:
      +schema: silver
      +materialized: view
```

```sql
-- models/staging/stg_coin_prices.sql
with source as (
    select * from silver.ohlcv
),
cleaned as (
    select
        coin_id,
        cast(date as date) as price_date,
        ...
    from source
)
select * from cleaned
```

**dbt executes:**
```sql
-- Compiled and executed SQL
CREATE OR REPLACE VIEW silver.stg_coin_prices AS
SELECT
    coin_id,
    CAST(date AS DATE) AS price_date,
    ...
FROM silver.ohlcv
```

**Result:**
- ✅ View created: `silver.stg_coin_prices`
- ✅ Can be queried like a table
- ✅ No duplicate data storage
- ✅ Always uses latest silver.ohlcv data

---

## Project Configuration

### dbt_project.yml

```yaml
name: 'crypto_pipeline'          # Project name
version: '1.0.0'                 # Version
config-version: 2                # dbt version

profile: 'crypto_pipeline'       # Which profile to use (in profiles.yml)

model-paths: ["models"]          # Where dbt models are located
test-paths: ["tests"]            # Where tests are defined
seed-paths: ["seeds"]            # Where seed data files are

target-path: "target"            # Where dbt outputs go (artifacts)
clean-targets: ["target", "dbt_packages"]  # Folders to clean

models:
  crypto_pipeline:               # Project models
    staging:
      +schema: silver            # All staging models → silver schema
      +materialized: view        # Create as views (not tables)
    
    intermediate:
      +schema: silver            # All intermediate models → silver schema
      +materialized: table       # Create as tables (persistent storage)
    
    mart:
      +schema: gold              # All mart models → gold schema
      +materialized: table       # Create as tables (analytics-ready)
```

### profiles.yml (Database Connection)

```yaml
crypto_pipeline:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /opt/airflow/data/warehouse/crypto.duckdb
      schema: main
```

- **type**: Database type (duckdb, postgres, snowflake, etc)
- **path**: Location of DuckDB file
- **schema**: Default schema for operations

---

## The 5 dbt Models

dbt Models are organized into 3 layers (following the Medallion Architecture):

```
Layer 1: STAGING (dbt)
  └─ stg_coin_prices ─ Views that clean raw data

Layer 2: INTERMEDIATE (dbt)
  └─ int_price_enriched ─ Add calculations & metrics

Layer 3: MART (dbt)
  ├─ dim_coin ─ Dimension table (coins)
  ├─ dim_date ─ Dimension table (dates)
  └─ fact_price_ohlcv ─ Fact table (prices)
```

### Model 1: `stg_coin_prices` (Staging)

**Purpose:** Clean and standardize raw price data

**Input:** `silver.ohlcv` (from Airflow)

**Output:** `silver.stg_coin_prices` (view)

**What It Does:**

```sql
with source as (
    select * from silver.ohlcv  -- Raw data from Airflow
),

cleaned as (
    select
        coin_id,
        cast(date as date)              as price_date,
        round(cast(open  as double), 8) as open_price,
        round(cast(high  as double), 8) as high_price,
        round(cast(low   as double), 8) as low_price,
        round(cast(close as double), 8) as close_price,
        source,
        cast(extracted_at as timestamp) as extracted_at,

        -- Flag invalid data
        case
            when open is null or close is null
            then true else false
        end as has_null_price,

        case
            when high < low
            then true else false
        end as is_invalid_ohlc

    from source
    where coin_id is not null
      and date is not null
)

select * from cleaned
```

**Transformations:**
- ✅ Rename columns (date → price_date, open → open_price)
- ✅ Cast to proper types (string → date, → double)
- ✅ Round numbers to 8 decimals
- ✅ Add quality flags (has_null_price, is_invalid_ohlc)
- ✅ Remove obviously bad rows (null coin_id, date)

**Output Sample:**
```
coin_id  | price_date | open_price | high_price | ... | has_null_price | is_invalid_ohlc
---------|------------|------------|------------|-----|----------------|----------------
bitcoin  | 2013-01-01 | 13.50      | 13.66      | ... | false          | false
ethereum | 2015-07-30 | 1.00       | 1.00       | ... | false          | false
```

**Type:** VIEW (not stored, computed on-the-fly)

---

### Model 2: `int_price_enriched` (Intermediate)

**Purpose:** Add calculated metrics (moving averages, returns, etc.)

**Input:** `silver.stg_coin_prices` (from staging model)

**Output:** `silver.int_price_enriched` (table)

**What It Does:**

```sql
with prices as (
    select * from {{ ref('stg_coin_prices') }}
    where not has_null_price
      and not is_invalid_ohlc  -- Filter out bad data
),

final as (
    select
        coin_id,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,

        -- Daily price range
        round(high_price - low_price, 8) as daily_range,

        -- Daily return percentage
        round(
            (close_price - open_price) / open_price * 100, 4
        ) as daily_return_pct,

        -- Day-over-day price change
        round(close_price - lag(close_price) over (
            partition by coin_id order by price_date
        ), 8) as price_change,

        -- 7-day moving average
        round(avg(close_price) over (
            partition by coin_id
            order by price_date
            rows between 6 preceding and current row
        ), 8) as ma_7d,

        -- 30-day moving average
        round(avg(close_price) over (
            partition by coin_id
            order by price_date
            rows between 29 preceding and current row
        ), 8) as ma_30d,

        source,
        extracted_at

    from prices
)

select * from final
```

**Calculations:**

1. **Daily Range:**
   ```
   daily_range = high_price - low_price
   Example: 13.66 - 13.51 = 0.15
   ```

2. **Daily Return Percentage:**
   ```
   daily_return_pct = (close_price - open_price) / open_price * 100
   Example: (13.92 - 13.50) / 13.50 * 100 = 3.11%
   ```

3. **Price Change (Day-over-Day):**
   ```
   price_change = close_price - previous_close_price
   Example: 13.92 - 13.51 = 0.41
   Uses LAG() window function to get previous day's close
   ```

4. **7-Day Moving Average:**
   ```
   ma_7d = AVG(close_price) over last 7 days
   Window: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
   Example: Average of last 7 closing prices
   ```

5. **30-Day Moving Average:**
   ```
   ma_30d = AVG(close_price) over last 30 days
   Window: ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
   Example: Average of last 30 closing prices
   ```

**Output Sample:**
```
coin_id | price_date | close_price | daily_range | daily_return_pct | ma_7d   | ma_30d
--------|------------|-------------|-------------|------------------|---------|--------
bitcoin | 2013-01-01 | 13.51       | 0.15        | 0.00             | NULL    | NULL
bitcoin | 2013-01-02 | 13.92       | 0.45        | 3.11             | 13.72   | NULL
bitcoin | 2013-01-03 | 14.50       | 0.99        | 4.17             | 14.04   | NULL
```

**Type:** TABLE (persisted to database)

---

### Model 3: `dim_coin` (Mart - Dimension)

**Purpose:** Create a coin dimension table with surrogate keys

**Input:** `silver.coin_metadata` (from Airflow)

**Output:** `gold.dim_coin` (table)

**What It Does:**

```sql
with source as (
    select * from silver.coin_metadata  -- Metadata from Airflow
),

final as (
    select
        md5(coin_id)        as coin_sk,  -- Surrogate key (hashed ID)
        coin_id,
        symbol,
        name,
        categories,
        genesis_date,
        hashing_algorithm,
        current_timestamp   as dbt_updated_at

    from source
    qualify row_number() over (
        partition by coin_id
        order by extracted_at desc
    ) = 1  -- Keep only latest record per coin
)

select * from final
```

**Key Concepts:**

1. **Surrogate Key (coin_sk):**
   - MD5 hash of coin_id
   - Example: md5('bitcoin') → 'c1d3a...'
   - Purpose: Join to fact table efficiently
   - Protects against business key changes

2. **QUALIFY Clause:**
   - Keeps only the latest record per coin
   - Deduplicates if same coin appears multiple times
   - `ROW_NUMBER() OVER (... ORDER BY extracted_at DESC) = 1`

3. **dbt_updated_at:**
   - Timestamp when dbt ran
   - Tracks when dimension was last updated
   - Useful for auditing

**Output Sample:**
```
coin_sk              | coin_id  | symbol | name      | genesis_date | ...
---------------------|----------|--------|-----------|--------------|----
c1d3a9e6... (MD5)   | bitcoin  | BTC    | Bitcoin   | 2009-01-03   | ...
5fad2f... (MD5)     | ethereum | ETH    | Ethereum  | 2015-07-30   | ...
8e9c4b... (MD5)     | solana   | SOL    | Solana    | 2020-03-16   | ...
```

**Type:** TABLE (3 rows total)

---

### Model 4: `dim_date` (Mart - Dimension)

**Purpose:** Create a date dimension table for time-based analysis

**Input:** None (generates dates programmatically)

**Output:** `gold.dim_date` (table)

**What It Does:**

```sql
with date_spine as (
    -- Generate all dates from 2013-01-01 to 2030-12-31
    select unnest(
        generate_series(
            date '2013-01-01',
            date '2030-12-31',
            interval '1 day'
        )
    ) as full_date
),

final as (
    select
        cast(strftime(full_date, '%Y%m%d') as integer) as date_id,
        full_date                                        as date,
        extract('year'    from full_date)               as year,
        extract('month'   from full_date)               as month,
        extract('day'     from full_date)               as day,
        extract('quarter' from full_date)               as quarter,
        extract('week'    from full_date)               as week_of_year,
        strftime(full_date, '%A')                       as day_name,
        strftime(full_date, '%B')                       as month_name,
        case
            when extract('dow' from full_date) in (0, 6)
            then true else false
        end                                             as is_weekend,
        case
            when extract('month' from full_date) in (12, 1, 2) then 'Winter'
            when extract('month' from full_date) in (3, 4, 5)  then 'Spring'
            when extract('month' from full_date) in (6, 7, 8)  then 'Summer'
            else 'Fall'
        end                                             as season
    from date_spine
)

select * from final
```

**Date Spine Generation:**
- `generate_series()`: Create sequence from 2013-01-01 to 2030-12-31, 1 day at a time
- Creates 6,574 rows (18 years × 365 days + leap days)

**Columns Generated:**

| Column | Source | Example |
|--------|--------|---------|
| **date_id** | YYYYMMDD format as integer | 20130101 |
| **date** | Original full date | 2013-01-01 |
| **year** | Extract from date | 2013 |
| **month** | Extract from date | 1 (January) |
| **day** | Extract from date | 1 |
| **quarter** | Calculated (month/3 + 1) | 1 |
| **week_of_year** | Extract from date | 1 |
| **day_name** | Day of week name | 'Tuesday' |
| **month_name** | Month name | 'January' |
| **is_weekend** | Boolean (Sat/Sun) | false |
| **season** | Derived from month | 'Winter' |

**Output Sample:**
```
date_id | date       | year | month | day | day_name | is_weekend | season
--------|------------|------|-------|-----|----------|------------|-------
20130101| 2013-01-01 | 2013 | 1     | 1   | Tuesday  | false      | Winter
20130102| 2013-01-02 | 2013 | 1     | 2   | Wednesday| false      | Winter
20130105| 2013-01-05 | 2013 | 1     | 5   | Saturday | true       | Winter
```

**Type:** TABLE (6,574 rows total)

**Purpose of Date Dimensions:**
- ✅ Enables time-based filtering (give me Q1 2021 data)
- ✅ Holiday/weekend tracking
- ✅ Season-based analysis
- ✅ Performance optimization (join on integer instead of date)

---

### Model 5: `fact_price_ohlcv` (Mart - Fact Table)

**Purpose:** Central fact table joining enriched prices with dimensions

**Input:**
- `int_price_enriched` (enriched prices with calculations)
- `dim_coin` (coin metadata)
- `dim_date` (date attributes)

**Output:** `gold.fact_price_ohlcv` (table)

**What It Does:**

```sql
with enriched as (
    select * from {{ ref('int_price_enriched') }}
),

dim_coin as (
    select coin_sk, coin_id from {{ ref('dim_coin') }}
),

dim_date as (
    select date_id, date from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key (unique row identifier)
        md5(concat(e.coin_id, '|', cast(e.price_date as varchar)))
                            as price_sk,

        -- Foreign keys to dimensions
        dc.coin_sk,
        dd.date_id,

        -- Fact data (measurements)
        e.source            as source_system,
        e.open_price,
        e.high_price,
        e.low_price,
        e.close_price,
        e.daily_range,
        e.daily_return_pct,
        e.price_change,
        e.ma_7d,
        e.ma_30d,
        e.extracted_at

    from enriched e
    left join dim_coin dc on e.coin_id    = dc.coin_id
    left join dim_date dd on e.price_date = dd.date
)

select * from final
```

**Join Logic:**

```
enriched (prices + metrics)
    ↓ LEFT JOIN
    ├─ dim_coin: coin_id = coin_id → get coin_sk
    └─ dim_date: price_date = date → get date_id

Result: fact_price_ohlcv with surrogate keys + dimensions
```

**Star Schema Structure:**

```
                    ┌──────────────────┐
                    │   DIM_DATE       │
                    ├──────────────────┤
                    │ date_id (PK)     │
                    │ date             │
                    │ year, month, day │
                    │ ...              │
                    └──────────────────┘
                            ↑
                            │ (date_id FK)
                            │
        ┌───────────────────┴───────────────────┐
        │   FACT_PRICE_OHLCV                    │
        ├───────────────────┬───────────────────┤
        │ price_sk (PK)     │ coin_sk (FK)      │
        │ date_id (FK)      │ open_price        │
        │ source_system     │ high_price        │
        │ close_price       │ daily_range       │
        │ ma_7d, ma_30d     │ daily_return_pct  │
        └───────────────────┬───────────────────┘
                            │
                            │ (coin_sk FK)
                            ↓
                    ┌──────────────────┐
                    │   DIM_COIN       │
                    ├──────────────────┤
                    │ coin_sk (PK)     │
                    │ coin_id          │
                    │ symbol, name     │
                    │ genesis_date     │
                    └──────────────────┘
```

**Output Sample:**
```
price_sk            | coin_sk           | date_id | open_price | close_price | ma_7d | ma_30d
---------------------|-------------------|---------|------------|-------------|-------|-------
[MD5 HASH]          | [MD5 HASH]        | 20130101| 13.50      | 13.51       | NULL  | NULL
[MD5 HASH]          | [MD5 HASH]        | 20130102| 13.56      | 13.92       | 13.72 | NULL
[MD5 HASH]          | [MD5 HASH]        | 20150730| 1.00       | 0.53        | NULL  | NULL
```

**Type:** TABLE (5,603 rows total)

**Why This Design?**
- ✅ **Normalization:** Dimensions don't repeat (only SKs in fact)
- ✅ **Performance:** Small fact table, quick joins
- ✅ **Flexibility:** Add new dimensions without changing fact table
- ✅ **Analytics:** Easy to filter/group by dimension attributes

---

## Medallion Architecture

This project uses the **Medallion Architecture** pattern:

```
┌──────────────────────────────────────┐
│        MEDALLION ARCHITECTURE        │
├──────────────────────────────────────┤

LAYER 1: BRONZE (Raw)
  └─ CSV files (bitcoin.csv, ethereum.csv, solana.csv)
     Raw data as-is, no transformations

LAYER 2: SILVER (Cleaned)
  ├─ silver.ohlcv
  │  └─ Cleaned, deduplicated price data
  └─ silver.coin_metadata
     └─ Validated metadata

LAYER 3: GOLD (Refined/Analytics)
  ├─ Dimensions:
  │  ├─ gold.dim_coin (3 rows)
  │  └─ gold.dim_date (6,574 rows)
  └─ Facts:
     └─ gold.fact_price_ohlcv (5,603 rows)

└─ Ready for analytics, dashboards, reports
```

### Why This Pattern?

| Layer | Purpose | Users | Quality |
|-------|---------|-------|---------|
| **Bronze** | Source data | Data engineers | ⭐ Raw |
| **Silver** | Cleaned, deduplicated | Data engineers | ⭐⭐⭐ Good |
| **Gold** | Analytics-ready | Analysts, BI tools | ⭐⭐⭐⭐⭐ Excellent |

---

## Data Quality Tests

dbt includes data quality testing via `schema.yml`:

```yaml
version: 2

models:
  - name: fact_price_ohlcv
    description: "Daily OHLCV fact table — BTC, ETH, SOL"
    columns:
      - name: price_sk
        data_tests: [unique, not_null]  # ← Fact PK must be unique & not null

      - name: coin_sk
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_coin')
              field: coin_sk  # ← FK must reference dim_coin

      - name: date_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_id  # ← FK must reference dim_date

      - name: close_price
        data_tests: [not_null]  # ← Critical column can't be null
```

### Test Types

1. **unique**: Values in column are unique
   ```sql
   SELECT price_sk FROM fact_price_ohlcv
   GROUP BY price_sk HAVING COUNT(*) > 1
   -- Should return 0 rows
   ```

2. **not_null**: No NULL values in column
   ```sql
   SELECT COUNT(*) FROM fact_price_ohlcv
   WHERE price_sk IS NULL
   -- Should return 0
   ```

3. **relationships**: Foreign key constraint
   ```sql
   SELECT COUNT(*) FROM fact_price_ohlcv f
   LEFT JOIN dim_coin dc ON f.coin_sk = dc.coin_sk
   WHERE dc.coin_sk IS NULL AND f.coin_sk IS NOT NULL
   -- Should return 0
   ```

### When Tests Run

```
dbt run  → Execute models → Create tables
    ↓
dbt test → Run data quality tests
    ↓
if test fails:
    ❌ Task fails (dbt_test_all_models)
    DAG stops execution
else:
    ✅ All tests pass
    Pipeline complete
```

---

## Execution & Outputs

### dbt run Command

```bash
cd /opt/airflow/dbt && \
PYTHONUTF8=1 dbt run --profiles-dir .
```

**What Happens:**

1. **Parse Phase** (~5s)
   - Load all .sql files
   - Load profiles.yml
   - Load dbt_project.yml
   - Build dependency graph

2. **Plan Phase** (~5s)
   - Determine execution order
   - Verify references with `{{ ref() }}`
   - Generate compiled SQL

3. **Run Phase** (~35s)
   - Execute stg_coin_prices (select from silver.ohlcv)
   - Execute int_price_enriched (select from stg_coin_prices)
   - Execute dim_coin (select from silver.coin_metadata)
   - Execute dim_date (generate_series 2013-2030)
   - Execute fact_price_ohlcv (join all above)

**Total Duration:** ~45 seconds

**Output:**
```
Running with dbt 1.8.1
Found 5 models in crypto_pipeline

Completed successfully

|                   21 created model stg_coin_prices
|                   22 created model int_price_enriched
|                   23 created model dim_coin
|                   24 created model dim_date
|                   25 created model fact_price_ohlcv

Done!
```

### dbt test Command

```bash
cd /opt/airflow/dbt && \
PYTHONUTF8=1 dbt test --profiles-dir .
```

**Tests Executed:**
```
Running with dbt 1.8.1

1 pass fact_price_ohlcv.unique_price_sk ...
2 pass fact_price_ohlcv.not_null_price_sk ...
3 pass fact_price_ohlcv.not_null_coin_sk ...
4 pass fact_price_ohlcv.relationships_fact_price_ohlcv_coin_sk_fk_dim_coin_coin_sk
5 pass fact_price_ohlcv.not_null_date_id ...
6 pass fact_price_ohlcv.relationships_fact_price_ohlcv_date_id_fk_dim_date_date_id
7 pass fact_price_ohlcv.not_null_close_price ...
8 pass dim_coin.unique_coin_sk ...
9 pass dim_coin.not_null_coin_sk ...
10 pass dim_coin.unique_coin_id ...
11 pass dim_coin.not_null_coin_id ...
12 pass dim_date.unique_date_id ...
13 pass dim_date.not_null_date_id ...

Completed successfully

Done! [13 passed in 20.12s]
```

**Duration:** ~20 seconds

---

## dbt vs Traditional ETL

### Traditional ETL Approach

```python
# Python/Pandas ETL
def transform_prices():
    df = pd.read_sql("SELECT * FROM silver.ohlcv")
    
    # Manual transformations
    df['price_date'] = pd.to_datetime(df['date'])
    df['open_price'] = df['open'].astype(float).round(8)
    df['daily_return_pct'] = (df['close'] - df['open']) / df['open'] * 100
    df['ma_7d'] = df.groupby('coin_id')['close'].rolling(7).mean()
    df['ma_30d'] = df.groupby('coin_id')['close'].rolling(30).mean()
    
    # Manual testing
    assert df['price_date'].notna().all()
    assert len(df['price_date'].unique()) == len(df)
    
    # Write results
    df.to_sql('fact_price_ohlcv', con=db)

# Run transformation
transform_prices()
```

**Challenges:**
- ❌ Requires Python/Pandas knowledge
- ❌ Difficult to test data quality
- ❌ Hard to track data lineage
- ❌ No built-in documentation
- ❌ Manual dependency management

### dbt Approach

```sql
-- models/intermediate/int_price_enriched.sql
select
    coin_id,
    cast(date as date) as price_date,
    round(cast(open as double), 8) as open_price,
    round(
        (cast(close as double) - cast(open as double))
        / cast(open as double) * 100, 4
    ) as daily_return_pct,
    round(avg(close) over (
        partition by coin_id
        order by date
        rows between 6 preceding and current row
    ), 8) as ma_7d,
    round(avg(close) over (
        partition by coin_id
        order by date
        rows between 29 preceding and current row
    ), 8) as ma_30d
from {{ ref('stg_coin_prices') }}
```

```yaml
# models/mart/schema.yml
data_tests:
  - not_null:
      column: price_date
  - unique:
      column: price_date
```

```bash
# Run with one command
dbt run
dbt test
```

**Advantages:**
- ✅ Pure SQL (familiar to data professionals)
- ✅ Built-in testing framework
- ✅ Automatic lineage tracking
- ✅ Auto-generated documentation
- ✅ Version control friendly (just SQL files)

---

## Summary

### What dbt Does in This Project

```
INPUT (from Airflow):
  silver.ohlcv (5,603 rows)
  silver.coin_metadata (3 rows)

↓↓↓ dbt TRANSFORMATIONS ↓↓↓

OUTPUT (to Gold schema):
  ├─ Dimensions:
  │  ├─ dim_coin (3 rows)
  │  └─ dim_date (6,574 rows)
  └─ Facts:
     └─ fact_price_ohlcv (5,603 rows + calculations)

PLUS:
  ✅ Data quality testing (13 tests)
  ✅ Documentation generation
  ✅ Dependency tracking
  ✅ Error handling & logging
```

### Key Benefits

| Aspect | Benefit |
|--------|---------|
| **SQL-based** | Familiar to data teams, version controllable |
| **Declarative** | Describe *what* you want, not *how* |
| **Testable** | Built-in data quality testing |
| **Documented** | Auto-generate docs from code |
| **Modular** | Reusable components (ref() for dependencies) |
| **Scalable** | Works with any SQL database (DuckDB, Postgres, Snowflake, etc) |
| **Maintainable** | Clear code structure, easy to modify |

### dbt's Role in the Pipeline

```
CSV Files
    ↓ (Airflow extracts, loads)
Silver Layer (cleaned)
    ↓ (dbt transforms)
Gold Layer (analytics-ready)
    ↓ (Streamlit visualizes)
Dashboard
```

**dbt is the "transform" step** in the classic ETL/ELT pipeline! 🚀
