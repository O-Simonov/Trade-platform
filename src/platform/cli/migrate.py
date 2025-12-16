# src/platform/cli/migrate.py
from pathlib import Path
import os

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


def main() -> None:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env var is required")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    ddl_path = (
        Path(__file__).resolve().parents[2]
        / "platform"
        / "data"
        / "storage"
        / "postgres"
        / "ddl.sql"
    )

    ddl_sql = ddl_path.read_text(encoding="utf-8")
    store.exec_ddl(ddl_sql)


if __name__ == "__main__":
    main()
