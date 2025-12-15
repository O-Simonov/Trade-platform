from __future__ import annotations
import os
from pathlib import Path
from platform.data.storage.postgres.pool import create_pool
from platform.data.storage.postgres.storage import PostgreSQLStorage

def main():
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")
    ddl_path = Path(__file__).resolve().parents[2] / "data" / "storage" / "postgres" / "ddl.sql"
    ddl_sql = ddl_path.read_text(encoding="utf-8")
    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)
    store.exec_ddl(ddl_sql)
    print("[OK] DB schema applied")

if __name__ == "__main__":
    main()
