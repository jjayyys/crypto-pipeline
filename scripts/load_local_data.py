# scripts/load_local_data.py
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"
DB_PATH  = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"

RAW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

# ═══════════════════════════════════════════════════════════════
# STEP 1: OHLCV — BTC, ETH, SOL เท่านั้น
# ═══════════════════════════════════════════════════════════════
print("=== Step 1: Loading OHLCV ===")

COIN_FILES = {
    'bitcoin':  'bitcoin.csv',
    'ethereum': 'ethereum.csv',
    'solana':   'solana.csv',
}

all_ohlcv = []
for coin_id, filename in COIN_FILES.items():
    filepath = RAW_DIR / filename
    if not filepath.exists():
        print(f"  ❌ Missing: {filepath}")
        continue

    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()
    print(f"  {filename} columns: {list(df.columns)}")

    # หา columns อัตโนมัติ
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        if 'date' in cl:  col_map[col] = 'date'
        elif cl == 'open':  col_map[col] = 'open'
        elif cl == 'high':  col_map[col] = 'high'
        elif cl == 'low':   col_map[col] = 'low'
        elif cl == 'close' and 'adj' not in cl: col_map[col] = 'close'

    df = df.rename(columns=col_map)

    # ตรวจว่ามี columns ครบ
    required = ['date', 'open', 'high', 'low', 'close']
    missing  = [c for c in required if c not in df.columns]
    if missing:
        print(f"  ❌ {filename}: missing columns {missing}")
        continue

    # Clean
    df['date'] = pd.to_datetime(
        df['date'], infer_datetime_format=True
    ).dt.strftime('%Y-%m-%d')

    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=required)
    df = df[required].copy()
    df['coin_id']      = coin_id
    df['source']       = 'local_csv'
    df['extracted_at'] = datetime.now(timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ'
    )
    df = df.sort_values('date')

    all_ohlcv.append(df)
    print(f"  ✅ {coin_id}: {len(df):,} rows "
          f"({df['date'].min()} → {df['date'].max()})")

if not all_ohlcv:
    print("❌ ไม่มี OHLCV data")
    con.close()
    exit(1)

df_ohlcv = pd.concat(all_ohlcv, ignore_index=True)
df_ohlcv = df_ohlcv.drop_duplicates(subset=['coin_id', 'date'])
con.execute("""
    CREATE OR REPLACE TABLE silver.ohlcv AS
    SELECT * FROM df_ohlcv
""")
print(f"\n✅ silver.ohlcv: {len(df_ohlcv):,} rows total")

# ═══════════════════════════════════════════════════════════════
# STEP 2: Coin Metadata (Hardcoded — ไม่ต้องดึง API)
# ═══════════════════════════════════════════════════════════════
print("\n=== Step 2: Coin Metadata ===")

COIN_META = [
    {
        'coin_id':           'bitcoin',
        'symbol':            'BTC',
        'name':              'Bitcoin',
        'categories':        "['Cryptocurrency', 'Layer 1']",
        'genesis_date':      '2009-01-03',
        'hashing_algorithm': 'SHA-256',
    },
    {
        'coin_id':           'ethereum',
        'symbol':            'ETH',
        'name':              'Ethereum',
        'categories':        "['Cryptocurrency', 'Smart Contract']",
        'genesis_date':      '2015-07-30',
        'hashing_algorithm': 'Ethash',
    },
    {
        'coin_id':           'solana',
        'symbol':            'SOL',
        'name':              'Solana',
        'categories':        "['Cryptocurrency', 'Layer 1']",
        'genesis_date':      '2020-03-16',
        'hashing_algorithm': 'PoH',
    },
]

df_meta = pd.DataFrame(COIN_META)
df_meta['extracted_at'] = datetime.now(timezone.utc).strftime(
    '%Y-%m-%dT%H:%M:%SZ'
)
con.execute("""
    CREATE OR REPLACE TABLE silver.coin_metadata AS
    SELECT * FROM df_meta
""")
print(f"✅ silver.coin_metadata: {len(df_meta)} rows")

# ═══════════════════════════════════════════════════════════════
# FINAL CHECK
# ═══════════════════════════════════════════════════════════════
print("\n=== Silver Tables ===")
for t in ['silver.ohlcv', 'silver.coin_metadata']:
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()  # ← เพิ่ม 
    print(f"  {t}: {n:,} rows")

print("\n=== OHLCV per coin ===")
print(con.execute("""
    SELECT coin_id,
           MIN(date) as from_date,
           MAX(date) as to_date,
           COUNT(*)  as rows
    FROM silver.ohlcv
    GROUP BY coin_id
    ORDER BY coin_id
""").df())

con.close()
print("\n✅ Done! Run dbt next:")
print("   cd dbt && dbt run && dbt test")