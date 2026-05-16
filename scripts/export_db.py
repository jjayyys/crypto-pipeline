# scripts/export_db.py
"""
Copy DuckDB จาก Docker named volume → Local data/warehouse/
รันหลังจาก Airflow DAG success
"""
import subprocess
import sys
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent
LOCAL_DB    = BASE_DIR / "data" / "warehouse" / "crypto.duckdb"
DOCKER_DB   = "/opt/airflow/data/warehouse/crypto.duckdb"

LOCAL_DB.parent.mkdir(parents=True, exist_ok=True)

# หา container name
result = subprocess.run(
    ["docker", "ps", "--filter", "name=airflow-scheduler",
     "--format", "{{.Names}}"],
    capture_output=True, text=True
)
container = result.stdout.strip().split("\n")

if not container:
    print("❌ ไม่เจอ airflow-scheduler container")
    sys.exit(1)

print(f"Container: {container}")
print(f"Copying {DOCKER_DB} → {LOCAL_DB}")

ret = subprocess.run([
    "docker", "cp",
    f"{container}:{DOCKER_DB}",
    str(LOCAL_DB)
])

if ret.returncode == 0:
    print("✅ Export สำเร็จ! รัน Streamlit ได้เลย:")
    print("   streamlit run streamlit/app.py")
else:
    print("❌ Export ล้มเหลว")
    sys.exit(1)