# src/platform/cli/migrate.py
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Iterable, Tuple

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _iter_sql_files(dir_path: Path) -> Iterable[Path]:
    if not dir_path.exists():
        return []
    return sorted([p for p in dir_path.glob("*.sql") if p.is_file()])


def _ensure_schema_migrations(store: PostgreSQLStorage) -> None:
    store.exec_ddl(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )


def _get_applied_map(store: PostgreSQLStorage) -> dict[str, str]:
    rows = store._exec_one_fetchone(
        "SELECT 1"
    )  # quick sanity to ensure connection works; ignore result

    # fetch all migrations
    with store.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, checksum FROM schema_migrations ORDER BY name ASC;")
            out = {}
            for name, checksum in cur.fetchall():
                out[str(name)] = str(checksum)
            return out


def _apply_sql_file(
    store: PostgreSQLStorage,
    *,
    file_path: Path,
    dry_run: bool,
    allow_reapply_changed: bool,
) -> Tuple[bool, str]:
    """
    Returns (applied?, reason).
    """
    name = file_path.name
    sql = _read_sql(file_path).strip()
    if not sql:
        return False, "EMPTY (skip)"

    checksum = _sha256_text(sql)
    applied = _get_applied_map(store)
    if name in applied:
        if applied[name] == checksum:
            return False, "ALREADY APPLIED (same checksum)"
        # file changed after it was applied
        if not allow_reapply_changed:
            raise RuntimeError(
                f"Migration '{name}' was already applied, but checksum changed.\n"
                f"Applied: {applied[name]}\n"
                f"Current: {checksum}\n"
                f"If you REALLY know what you're doing, set MIGRATE_ALLOW_REAPPLY_CHANGED=1 "
                f"and rerun (not recommended)."
            )
        # allow reapply changed: apply SQL again but DO NOT change primary key name;
        # we will update checksum to the new one.
        if dry_run:
            return False, "WOULD REAPPLY (checksum changed, dry-run)"
        store.exec_ddl(sql)
        with store.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE schema_migrations SET checksum=%s, applied_at=now() WHERE name=%s;",
                    (checksum, name),
                )
            conn.commit()
        return True, "REAPPLIED (checksum updated)"
    else:
        if dry_run:
            return False, "WOULD APPLY (dry-run)"
        store.exec_ddl(sql)
        with store.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO schema_migrations (name, checksum) VALUES (%s, %s);",
                    (name, checksum),
                )
            conn.commit()
        return True, "APPLIED"


def main() -> None:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env var is required")

    dry_run = os.getenv("MIGRATE_DRY_RUN", "0").strip() == "1"
    allow_reapply_changed = os.getenv("MIGRATE_ALLOW_REAPPLY_CHANGED", "0").strip() == "1"

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    # paths
    # file is: src/platform/cli/migrate.py
    platform_dir = Path(__file__).resolve().parents[1]          # src/platform
    ddl_path = platform_dir / "data" / "storage" / "postgres" / "ddl.sql"
    migrations_dir = platform_dir / "migrations"

    if not ddl_path.exists():
        raise FileNotFoundError(f"ddl.sql not found: {ddl_path}")

    print(f"[migrate] PG_DSN ok. dry_run={dry_run} allow_reapply_changed={allow_reapply_changed}")
    print(f"[migrate] DDL: {ddl_path}")
    print(f"[migrate] Migrations dir: {migrations_dir}")

    # 1) apply base DDL (idempotent)
    ddl_sql = _read_sql(ddl_path)
    if dry_run:
        print("[migrate] DDL would be executed (dry-run)")
    else:
        store.exec_ddl(ddl_sql)
        print("[migrate] DDL executed")

    # 2) ensure migrations table
    if dry_run:
        print("[migrate] schema_migrations would be ensured (dry-run)")
    else:
        _ensure_schema_migrations(store)
        print("[migrate] schema_migrations ensured")

    # 3) apply migrations
    files = list(_iter_sql_files(migrations_dir))
    if not files:
        print("[migrate] No migrations found (*.sql). Done.")
        return

    print(f"[migrate] Found {len(files)} migration file(s)")
    for p in files:
        applied, reason = _apply_sql_file(
            store,
            file_path=p,
            dry_run=dry_run,
            allow_reapply_changed=allow_reapply_changed,
        )
        mark = "✓" if applied else "-"
        print(f"[migrate] {mark} {p.name}: {reason}")

    print("[migrate] Done.")


if __name__ == "__main__":
    main()
