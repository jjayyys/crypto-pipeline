# check_tables.py  (สร้างไว้ที่ root ของโปรเจค)
import duckdb

con = duckdb.connect('/opt/airflow/data/warehouse/crypto.duckdb')

print("=== ALL SCHEMAS ===")
schemas = con.execute("""
    SELECT DISTINCT table_schema
    FROM information_schema.tables
    ORDER BY table_schema
""").df()
print(schemas)

print("\n=== SILVER TABLES ===")
silver = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'silver'
    ORDER BY table_name
""").df()
print(silver)

print("\n=== ROW COUNTS ===")
for table in ['silver.ohlcv', 'silver.sentiment',
              'silver.market_index', 'silver.coin_metadata']:
    try:
        count = con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()
        print(f"{table}: {count:,} rows")
    except Exception as e:
        print(f"{table}: ERROR - {e}")

con.close()