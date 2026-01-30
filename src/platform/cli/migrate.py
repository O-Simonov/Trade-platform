# src/platform/cli/migrate.py
from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Iterable, Tuple


from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------

def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _iter_sql_files(dir_path: Path) -> Iterable[Path]:
    if not dir_path.exists():
        return []
    return sorted([p for p in dir_path.glob("*.sql") if p.is_file()])


# ------------------------------------------------------------
# SQL splitter (supports comments, quotes, dollar-quoting)
# ------------------------------------------------------------

def _split_sql_statements(sql: str) -> list[str]:
    """
    Разбивает SQL-скрипт на отдельные statements по ';' учитывая:
      - '--' line comments
      - '/* ... */' block comments
      - 'single quotes' + экранирование ''
      - "double quotes"
      - $$ dollar quoting (в т.ч. $tag$ ... $tag$)
    """
    out: list[str] = []
    buf: list[str] = []

    i = 0
    n = len(sql)

    in_squote = False
    in_dquote = False
    in_lcomment = False
    in_bcomment = False
    dollar_tag: str | None = None

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        # -- line comment
        if not in_squote and not in_dquote and dollar_tag is None and not in_bcomment:
            if not in_lcomment and ch == "-" and nxt == "-":
                in_lcomment = True
                i += 2
                continue
        if in_lcomment:
            # keep newline to preserve line structure (optional)
            if ch == "\n":
                in_lcomment = False
                buf.append(ch)
            i += 1
            continue

        # /* block comment */
        if not in_squote and not in_dquote and dollar_tag is None and not in_lcomment:
            if not in_bcomment and ch == "/" and nxt == "*":
                in_bcomment = True
                i += 2
                continue
        if in_bcomment:
            if ch == "*" and nxt == "/":
                in_bcomment = False
                i += 2
            else:
                i += 1
            continue

        # Dollar-quoting start/end
        if not in_squote and not in_dquote and not in_lcomment and not in_bcomment:
            if dollar_tag is None and ch == "$":
                # try to parse $tag$
                j = i + 1
                while j < n and sql[j] != "$" and (sql[j].isalnum() or sql[j] == "_"):
                    j += 1
                if j < n and sql[j] == "$":
                    dollar_tag = sql[i : j + 1]  # $tag$
                    buf.append(dollar_tag)
                    i = j + 1
                    continue
            elif dollar_tag is not None:
                if sql.startswith(dollar_tag, i):
                    buf.append(dollar_tag)
                    i += len(dollar_tag)
                    dollar_tag = None
                    continue

        # quotes
        if dollar_tag is None and not in_lcomment and not in_bcomment:
            if ch == "'" and not in_dquote:
                # handle escaped '' inside single quotes
                if in_squote and nxt == "'":
                    buf.append("''")
                    i += 2
                    continue
                in_squote = not in_squote
            elif ch == '"' and not in_squote:
                in_dquote = not in_dquote

        # statement terminator ;
        if ch == ";" and not in_squote and not in_dquote and dollar_tag is None:
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


# ------------------------------------------------------------
# execution policy: statements that cannot run inside transaction
# ------------------------------------------------------------

_NO_TX_STMT_RE = re.compile(
    r"""
    \bCONCURRENTLY\b
    | \bVACUUM\b
    | \bCLUSTER\b
    | \bREINDEX\b
    | \bCHECKPOINT\b
    | \bREFRESH\s+MATERIALIZED\s+VIEW\s+CONCURRENTLY\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _stmt_requires_autocommit(stmt: str) -> bool:
    return bool(_NO_TX_STMT_RE.search(stmt))


def _exec_sql_script(
    store: PostgreSQLStorage,
    sql: str,
    *,
    dry_run: bool,
    label: str = "",
) -> None:
    """
    Выполняет SQL-скрипт безопасно:
      - разбиваем на statements
      - для "no-tx" statements (VACUUM / CONCURRENTLY / REINDEX / CLUSTER...) переключаем autocommit
      - выполняем строго по порядку
    """
    stmts = _split_sql_statements(sql)
    if not stmts:
        return

    if dry_run:
        # Для dry-run просто печатаем кратко, без выполнения
        print(f"[migrate] DRY-RUN: would execute {len(stmts)} statement(s){(' for ' + label) if label else ''}")
        return

    # Важно: один connection, но с переключением autocommit между stmt.
    # Если нужно выйти из транзакции -> commit + autocommit=True.
    with store.pool.connection() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for stmt in stmts:
                s = stmt.strip()
                if not s:
                    continue

                need_ac = _stmt_requires_autocommit(s)

                if need_ac and not conn.autocommit:
                    conn.commit()
                    conn.autocommit = True
                elif (not need_ac) and conn.autocommit:
                    # возвращаемся в транзакционный режим
                    conn.autocommit = False

                # psycopg3: prepare=False помогает на DDL и multi-скриптах
                cur.execute(s, prepare=False)

        if not conn.autocommit:
            conn.commit()


# ------------------------------------------------------------
# schema_migrations
# ------------------------------------------------------------

def _ensure_schema_migrations(store: PostgreSQLStorage, *, dry_run: bool) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT PRIMARY KEY,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    _exec_sql_script(store, ddl, dry_run=dry_run, label="schema_migrations")


def _get_applied_map(store: PostgreSQLStorage) -> dict[str, str]:
    """
    Возвращает {migration_name: checksum}.
    Если таблицы schema_migrations нет — вернёт пустой dict (и выведет предупреждение).
    """
    try:
        with store.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, checksum FROM schema_migrations ORDER BY name ASC;", prepare=False)
                out: dict[str, str] = {}
                for name, checksum in cur.fetchall():
                    out[str(name)] = str(checksum)
                return out
    except Exception as e:
        print(f"[migrate] WARN: cannot read schema_migrations (will treat as empty). Reason: {e}")
        return {}


def _apply_sql_file(
    store: PostgreSQLStorage,
    *,
    file_path: Path,
    dry_run: bool,
    allow_reapply_changed: bool,
    applied_map: dict[str, str],
) -> Tuple[bool, str]:
    """
    Returns (applied?, reason).
    """
    name = file_path.name
    sql = _read_sql(file_path).strip()
    if not sql:
        return False, "EMPTY (skip)"

    checksum = _sha256_text(sql)

    if name in applied_map:
        if applied_map[name] == checksum:
            return False, "ALREADY APPLIED (same checksum)"

        # file changed after it was applied
        if not allow_reapply_changed:
            raise RuntimeError(
                f"Migration '{name}' was already applied, but checksum changed.\n"
                f"Applied: {applied_map[name]}\n"
                f"Current: {checksum}\n"
                f"If you REALLY know what you're doing, set MIGRATE_ALLOW_REAPPLY_CHANGED=1 "
                f"and rerun (not recommended)."
            )

        if dry_run:
            return False, "WOULD REAPPLY (checksum changed, dry-run)"

        _exec_sql_script(store, sql, dry_run=False, label=name)
        with store.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE schema_migrations SET checksum=%s, applied_at=now() WHERE name=%s;",
                    (checksum, name),
                    prepare=False,
                )
            conn.commit()

        applied_map[name] = checksum
        return True, "REAPPLIED (checksum updated)"

    # new migration
    if dry_run:
        return False, "WOULD APPLY (dry-run)"

    _exec_sql_script(store, sql, dry_run=False, label=name)
    with store.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_migrations (name, checksum) VALUES (%s, %s);",
                (name, checksum),
                prepare=False,
            )
        conn.commit()

    applied_map[name] = checksum
    return True, "APPLIED"


# ------------------------------------------------------------
# main
# ------------------------------------------------------------

def main() -> None:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env var is required")

    dry_run = os.getenv("MIGRATE_DRY_RUN", "0").strip() == "1"
    allow_reapply_changed = os.getenv("MIGRATE_ALLOW_REAPPLY_CHANGED", "0").strip() == "1"

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    # file is: src/platform/cli/migrate.py
    platform_dir = Path(__file__).resolve().parents[1]  # src/platform
    ddl_path = platform_dir / "data" / "storage" / "postgres" / "ddl.sql"
    migrations_dir = platform_dir / "migrations"

    if not ddl_path.exists():
        raise FileNotFoundError(f"ddl.sql not found: {ddl_path}")

    print(f"[migrate] PG_DSN ok. dry_run={dry_run} allow_reapply_changed={allow_reapply_changed}")
    print(f"[migrate] DDL: {ddl_path}")
    print(f"[migrate] Migrations dir: {migrations_dir}")

    # 1) apply base DDL (idempotent)
    ddl_sql = _read_sql(ddl_path).strip()
    if ddl_sql:
        _exec_sql_script(store, ddl_sql, dry_run=dry_run, label="ddl.sql")
        if dry_run:
            print("[migrate] DDL would be executed (dry-run)")
        else:
            print("[migrate] DDL executed")
    else:
        print("[migrate] DDL is empty, skip")

    # 2) ensure migrations table
    if dry_run:
        print("[migrate] schema_migrations would be ensured (dry-run)")
    else:
        _ensure_schema_migrations(store, dry_run=False)
        print("[migrate] schema_migrations ensured")

    # 3) load applied map (even in dry-run, we try to read; if missing -> empty)
    applied_map = _get_applied_map(store)

    # 4) apply migrations
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
            applied_map=applied_map,
        )
        mark = "✓" if applied else "-"
        print(f"[migrate] {mark} {p.name}: {reason}")

    print("[migrate] Done.")


if __name__ == "__main__":
    main()
