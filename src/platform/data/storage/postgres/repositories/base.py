from __future__ import annotations

import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Mapping, List,  Optional

from psycopg.types.json import Jsonb
from psycopg import sql as _sql
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)

def _interval_to_timedelta(interval: str) -> timedelta:
    """
    '1m','5m','15m','1h','4h','1d' -> timedelta
    """
    s = str(interval or "").strip().lower()
    m = re.match(r"^(\d+)\s*([mhd])$", s)
    if not m:
        return timedelta(hours=1)

    n = int(m.group(1))
    unit = m.group(2)

    if unit == "m":
        return timedelta(minutes=n)
    if unit == "h":
        return timedelta(hours=n)
    if unit == "d":
        return timedelta(days=n)

    return timedelta(hours=1)

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()  # Преобразует datetime в строку ISO 8601
    raise TypeError("Type not serializable")

def _utc(dt: Any) -> datetime:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(dt)}")

def _pg_interval(interval: str) -> str:
    """
    '4h' -> '4 hours'
    '15m' -> '15 minutes'
    '1d' -> '1 days'
    """
    s = str(interval).strip().lower()
    try:
        if s.endswith("m"):
            return f"{int(s[:-1])} minutes"
        if s.endswith("h"):
            return f"{int(s[:-1])} hours"
        if s.endswith("d"):
            return f"{int(s[:-1])} days"
    except Exception:
        pass
    return "1 hours"


class BasePostgresRepositoryMixin:
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.logger = logging.getLogger("storage.postgres")
        self._screeners_schema_ready = False

    @staticmethod
    def _safe_int(v: Any, default: Any = None) -> Any:
        try:
            return int(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def _safe_str(v: Any, default: Any = None) -> Any:
        try:
            if v is None:
                return default
            s = str(v)
            return s if s else default
        except Exception:
            return default

    @staticmethod
    def _safe_float(v: Any, default: Any = None) -> Any:
        try:
            return float(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def _to_jsonb(v: Any) -> Jsonb:
        if v is None:
            return Jsonb({})
        if isinstance(v, Jsonb):
            return v
        if isinstance(v, (dict, list)):
            return Jsonb(v)
        if isinstance(v, str):
            try:
                return Jsonb(json.loads(v))
            except Exception:
                return Jsonb({"_raw": v})
        return Jsonb({"_raw": str(v)})

    def fetch_all(self, sql: str, params: Optional[Mapping[str, Any]] = None) -> List[dict]:
        """        Выполняет SELECT и возвращает список строк как list[dict].

        - params: Mapping (для %(name)s) или последовательность (для %s / $1 в зависимости от стиля).
        - Возвращаем dict, чтобы код уровня трейдера мог обращаться по именам колонок.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                if not rows:
                    return []
                cols = [d.name for d in cur.description]
        out: List[dict] = []
        for r in rows:
            out.append({cols[i]: r[i] for i in range(len(cols))})
        return out

    def fetch_one(self, sql: str, params: Optional[Mapping[str, Any]] = None) -> Optional[dict]:
        """
        Возвращает одну строку (dict) или None.
        Удобно для SELECT ... LIMIT 1.
        """
        rows = self.fetch_all(sql, params)
        return rows[0] if rows else None

    def fetch_val(self, sql: str, params: Optional[Mapping[str, Any]] = None, col: str = "") -> Any:
        """
        Возвращает одно значение из первой строки.
        Если col не указан — берёт первое поле.
        """
        row = self.fetch_one(sql, params)
        if not row:
            return None
        if col:
            return row.get(col)
        # первое поле
        return next(iter(row.values()))

    def _exec_many(self, query: str, rows: list[dict], *, chunk_size: int = 500) -> int:
        """
        Execute executemany() safely with rollback+logging.

        Returns:
          affected rows (best-effort). If driver doesn't provide rowcount reliably,
          returns number of attempted rows.
        """
        if not rows:
            return 0

        total_attempted = 0
        total_affected: Optional[int] = 0

        def chunks(xs: list[dict], n: int) -> Iterable[list[dict]]:
            if n <= 0:
                yield xs
                return
            for i in range(0, len(xs), n):
                yield xs[i: i + n]

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    for part in chunks(rows, int(chunk_size)):
                        total_attempted += len(part)
                        cur.executemany(query, part)

                        rc = getattr(cur, "rowcount", None)
                        if isinstance(rc, int) and rc >= 0:
                            total_affected = (total_affected or 0) + rc
                        else:
                            total_affected = None

                conn.commit()

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                sample = rows[0] if rows else None
                sample_keys = sorted(list(sample.keys()))[:60] if isinstance(sample, dict) else None

                log.exception(
                    "[DB][_exec_many] FAILED: rows=%s chunk_size=%s err=%r sample_keys=%s sample_row=%r",
                    len(rows), chunk_size, e, sample_keys, sample
                )
                raise

        return int(total_affected) if total_affected is not None else int(total_attempted)

    def _exec_one_fetchone(self, query: str, params: Any = None):
        """
        For SELECT ... returning 1 row.
        Does NOT commit.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return cur.fetchone()

    def _exec_one_commit(self, query: str, params: Any = None) -> int:
        """
        For INSERT/UPDATE/DELETE single statement.
        Commits and returns rowcount best-effort.
        """
        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    rc = getattr(cur, "rowcount", None)
                conn.commit()
                return int(rc) if isinstance(rc, int) and rc >= 0 else 1
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                log.exception("[DB][_exec_one_commit] FAILED err=%r query=%r params=%r", e, query[:500], params)
                raise

    def _exec_one(self, query: str, params: Any = None):
        return self._exec_one_fetchone(query, params=params)

    def query(self, sql: str, params: Any = None) -> list[tuple]:
        """Выполняет SELECT и возвращает list[tuple]."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return rows

    def query_one(self, sql: str, params: Any = None) -> tuple | None:
        """Выполняет SELECT и возвращает одну строку или None."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        return row

    def query_dict(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        """Выполняет SELECT и возвращает список строк как dict (col -> value).

        В проекте много мест, где удобнее работать с именованными колонками.
        Базовые query/query_all возвращают tuple, поэтому этот хелпер нужен,
        чтобы не плодить zip(description, row) в каждом модуле.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                if not rows:
                    return []
                cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    def query_one_dict(self, sql: str, params: Any = None) -> dict[str, Any] | None:
        """Выполняет SELECT и возвращает одну строку как dict или None."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d.name for d in cur.description]
        return dict(zip(cols, row))

    def execute(self, sql: str, params: Any = None) -> int:
        """Выполняет INSERT/UPDATE/DELETE и возвращает rowcount."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rc = cur.rowcount
            conn.commit()
        return int(rc)

    def execute_raw(self, query: str, *, log_sql: bool = False) -> None:
        """
        Execute raw SQL (DDL / maintenance / cleanup).
        SAFE: uses connection pool.
        """
        if not query:
            return

        if log_sql:
            logger.info("[DB][RAW] %s", query)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_sql.SQL(query))
            conn.commit()

    def exec_ddl(self, sql: str) -> None:
        """
        Execute DDL SQL that may contain multiple statements.

        Supports:
          - multiple statements separated by ';'
          - DO $$ ... $$; and DO $tag$ ... $tag$; blocks
          - CREATE FUNCTION/PROCEDURE with dollar-quoting
          - ignores ';' inside quotes and dollar-quoted blocks
          - ignores '-- line comments' and '/* block comments */'
        """
        if not sql:
            return

        def _split_sql_statements(script: str) -> list[str]:
            s = script
            out: list[str] = []
            buf: list[str] = []

            i = 0
            n = len(s)

            in_squote = False
            in_dquote = False
            in_line_comment = False
            in_block_comment = False
            dollar_tag: str | None = None  # $$ or $tag$

            def startswith_at(prefix: str) -> bool:
                return s.startswith(prefix, i)

            def flush():
                stmt = "".join(buf).strip()
                if stmt:
                    out.append(stmt)
                buf.clear()

            while i < n:
                ch = s[i]

                # line comment
                if in_line_comment:
                    buf.append(ch)
                    if ch == "\n":
                        in_line_comment = False
                    i += 1
                    continue

                # block comment
                if in_block_comment:
                    if startswith_at("*/"):
                        buf.append("*/")
                        i += 2
                        in_block_comment = False
                        continue
                    buf.append(ch)
                    i += 1
                    continue

                # start comment
                if not in_squote and not in_dquote and dollar_tag is None:
                    if startswith_at("--"):
                        buf.append("--")
                        i += 2
                        in_line_comment = True
                        continue
                    if startswith_at("/*"):
                        buf.append("/*")
                        i += 2
                        in_block_comment = True
                        continue

                # dollar block
                if not in_squote and not in_dquote:
                    if dollar_tag is not None:
                        if s.startswith(dollar_tag, i):
                            buf.append(dollar_tag)
                            i += len(dollar_tag)
                            dollar_tag = None
                            continue
                        buf.append(ch)
                        i += 1
                        continue
                    else:
                        if ch == "$":
                            j = i + 1
                            while j < n and s[j] != "$":
                                j += 1
                            if j < n and s[j] == "$":
                                tag = s[i: j + 1]
                                dollar_tag = tag
                                buf.append(tag)
                                i = j + 1
                                continue

                # string quotes
                if dollar_tag is None:
                    if not in_dquote and ch == "'":
                        buf.append(ch)
                        if in_squote:
                            if i + 1 < n and s[i + 1] == "'":
                                buf.append("'")
                                i += 2
                                continue
                            in_squote = False
                        else:
                            in_squote = True
                        i += 1
                        continue

                    if not in_squote and ch == '"':
                        buf.append(ch)
                        if in_dquote:
                            if i + 1 < n and s[i + 1] == '"':
                                buf.append('"')
                                i += 2
                                continue
                            in_dquote = False
                        else:
                            in_dquote = True
                        i += 1
                        continue

                # boundary
                if ch == ";" and not in_squote and not in_dquote and dollar_tag is None:
                    flush()
                    i += 1
                    continue

                buf.append(ch)
                i += 1

            flush()
            return out

        statements = _split_sql_statements(sql)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            conn.commit()



__all__ = [
    "_utcnow",
    "_interval_to_timedelta",
    "datetime_converter",
    "_utc",
    "_pg_interval",
    "BasePostgresRepositoryMixin",
]
