# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import re
import json
import hashlib
import time
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any, Iterable, Sequence, Mapping, List, Tuple, Dict, Optional

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


class PostgreSQLStorage:
    """
    PostgreSQL storage — registry, market state, OMS helpers, retention, exchangeInfo,
    positions, balances snapshots, orders, trades, stats.
    """

    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.logger = logging.getLogger("storage.postgres")
        self._screeners_schema_ready = False

    # ======================================================================
    # SAFE CAST HELPERS
    # ======================================================================

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


    # ======================================================================
    # UID HELPERS
    # ======================================================================

    @staticmethod
    def _mk_fill_uid(
        *,
        exchange_id: int,
        account_id: int,
        symbol_id: int,
        order_id: str | None,
        trade_id: str | None,
        ts_ms: int | None,
        price: Any,
        qty: Any,
    ) -> str:
        """
        Детерминированный UID для fill.
        """
        if trade_id:
            return f"tid:{trade_id}"

        base = {
            "ex": exchange_id,
            "acc": account_id,
            "sym": symbol_id,
            "oid": order_id,
            "ts": ts_ms,
            "price": str(price),
            "qty": str(qty),
        }
        raw = json.dumps(base, sort_keys=True, ensure_ascii=False).encode("utf-8")
        h = hashlib.sha1(raw).hexdigest()
        return f"h:{h}"


    def fetch_liquidations_window(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        start_ts: datetime,
        end_ts: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает серию ликвидаций, агрегированную в интервалы свечей.
        long_usdt = +, short_usdt = +
        (в plotting мы short сделаем отрицательным)
        """
        start_ts = _utc(start_ts)
        end_ts = _utc(end_ts)

        pg_int = _pg_interval(interval)

        q = """
        SELECT
            date_bin(%s::interval, bucket_ts, '1970-01-01 00:00:00+00'::timestamptz) AS ts,
            COALESCE(SUM(long_notional), 0)  AS long_usdt,
            COALESCE(SUM(short_notional), 0) AS short_usdt
        FROM liquidation_1m
        WHERE exchange_id=%s
          AND symbol_id=%s
          AND bucket_ts >= %s
          AND bucket_ts < %s
        GROUP BY 1
        ORDER BY 1 ASC
        """

        out: List[Dict[str, Any]] = []
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (pg_int, int(exchange_id), int(symbol_id), start_ts, end_ts))
                rows = cur.fetchall()

        for r in rows:
            out.append(
                {
                    "ts": _utc(r[0]),
                    "long_usdt": float(r[1] or 0.0),
                    "short_usdt": float(r[2] or 0.0),
                }
            )
        return out



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
    # ======================================================================
    # DB EXEC HELPERS
    # ======================================================================

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

    # ======================================================================
    # IDS / REGISTRY (used by run_balance.py)
    # ======================================================================

    def ensure_exchange_account_symbol(
        self,
        *,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> tuple[int, int, dict[str, int]]:
        r"""
        Ensures exchange/account/symbols exist in DB and returns ids mapping.

        Real schema:
          exchanges(exchange_id, name)
          accounts(account_id, exchange_id, account_name, role, is_active)
          symbols(symbol_id, exchange_id, symbol)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # --- exchange ---
                cur.execute(
                    """
                    INSERT INTO exchanges (name)
                    VALUES (%s)
                    ON CONFLICT (name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING exchange_id
                    """,
                    (exchange,),
                )
                exchange_id = cur.fetchone()[0]

                # --- account ---
                cur.execute(
                    """
                    INSERT INTO accounts (exchange_id, account_name)
                    VALUES (%s, %s)
                    ON CONFLICT (exchange_id, account_name)
                    DO UPDATE SET account_name = EXCLUDED.account_name
                    RETURNING account_id
                    """,
                    (exchange_id, account),
                )
                account_id = cur.fetchone()[0]

                # --- symbols ---
                symbol_ids: dict[str, int] = {}
                for sym in symbols:
                    cur.execute(
                        """
                        INSERT INTO symbols (exchange_id, symbol)
                        VALUES (%s, %s)
                        ON CONFLICT (exchange_id, symbol)
                        DO UPDATE SET symbol = EXCLUDED.symbol
                        RETURNING symbol_id
                        """,
                        (exchange_id, sym),
                    )
                    symbol_ids[sym] = cur.fetchone()[0]

            conn.commit()

        return int(exchange_id), int(account_id), symbol_ids

    def ensure_exchange_account_symbol_map(
        self,
        *,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> dict:
        """
        Backward-compatible wrapper.
        """
        exchange_id, account_id, symbol_ids = self.ensure_exchange_account_symbol(
            exchange=exchange,
            account=account,
            symbols=symbols,
        )
        out = {"_exchange_id": exchange_id, "_account_id": account_id}
        out.update(symbol_ids)
        return out

    def upsert_symbols(
        self,
        *,
        exchange_id: int,
        symbols: list[str],
        deactivate_missing: bool = True,
    ) -> dict[str, int]:
        symbols_norm = sorted({str(s).upper().strip() for s in (symbols or []) if str(s).strip()})
        if not symbols_norm:
            return {}

        sql_upsert = """
            WITH incoming AS (SELECT UNNEST(%(symbols)s::text[]) AS symbol)
            INSERT INTO symbols (exchange_id, symbol, is_active, status, last_seen_at)
            SELECT %(exchange_id)s, symbol, TRUE, 'TRADING', NOW()
            FROM incoming
            ON CONFLICT (exchange_id, symbol)
            DO UPDATE SET
                is_active = TRUE,
                status = EXCLUDED.status,
                last_seen_at = EXCLUDED.last_seen_at
            RETURNING symbol, symbol_id;
        """

        sql_deactivate_missing = """
            UPDATE symbols
            SET is_active    = FALSE,
                status       = 'DELISTED',
                last_seen_at = COALESCE(last_seen_at, NOW())
            WHERE exchange_id = %(exchange_id)s
              AND is_active = TRUE
              AND NOT (symbol = ANY (%(symbols)s::text[]));
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql_upsert,
                    {"exchange_id": int(exchange_id), "symbols": symbols_norm},
                )
                rows = cur.fetchall() or []

                if deactivate_missing:
                    cur.execute(
                        sql_deactivate_missing,
                        {"exchange_id": int(exchange_id), "symbols": symbols_norm},
                    )

            conn.commit()

        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

    def list_symbols(self, *, exchange_id: int) -> dict[str, int]:
        """Return {SYMBOL: symbol_id} for all symbols known to DB for the exchange."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, symbol_id
                    FROM symbols
                    WHERE exchange_id = %s
                    """,
                    (int(exchange_id),),
                )
                rows = cur.fetchall() or []
        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

    # ======================================================================
    # SCREENERS REGISTRY / RUNS / SIGNALS
    # ======================================================================

    def next_signal_seq(
            self,
            *,
            exchange_id: int,
            symbol_id: int,
            screener_id: int,
            signal_day: date,
    ) -> int:
        # Проверка типа signal_day, чтобы убедиться, что это объект типа date
        if not isinstance(signal_day, date):
            raise TypeError(f"signal_day should be of type 'date', got {type(signal_day)}")

        sql = """
              INSERT INTO signals_day_seq (exchange_id, symbol_id, screener_id, signal_day, last_seq)
              VALUES (%s, %s, %s, %s, 1) ON CONFLICT (exchange_id, symbol_id, screener_id, signal_day)
        DO \
              UPDATE SET last_seq = signals_day_seq.last_seq + 1 \
                  RETURNING last_seq \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), int(symbol_id), int(screener_id), signal_day))
                r = cur.fetchone()
            conn.commit()  # ✅ ОБЯЗАТЕЛЬНО
        return int(r[0]) if r else 1

    def insert_signals(self, rows: list[dict]) -> int:
        """
        Идемпотентная вставка.
        Возвращает: сколько реально вставилось (без дублей).
        """
        if not rows:
            return 0

        import json
        from datetime import datetime, date
        from decimal import Decimal
        from psycopg.types.json import Jsonb

        def _json_default(o):
            # ✅ datetime/date -> строка ISO
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            # ✅ Decimal -> float
            if isinstance(o, Decimal):
                return float(o)
            # ✅ остальное -> строка
            return str(o)

        def _json_dumps(obj) -> str:
            return json.dumps(obj, ensure_ascii=False, default=_json_default)

        sql = """
              INSERT INTO signals (exchange_id,
                                   symbol_id,
                                   symbol,
                                   screener_id,
                                   timeframe,
                                   signal_ts,
                                   signal_day,
                                   day_seq,
                                   side,
                                   status,
                                   entry_price,
                                   exit_price,
                                   stop_loss,
                                   take_profit,
                                   confidence,
                                   score,
                                   reason,
                                   context,
                                   source)
              VALUES (%(exchange_id)s,
                      %(symbol_id)s,
                      %(symbol)s,
                      %(screener_id)s,
                      %(timeframe)s,
                      %(signal_ts)s,
                      %(signal_day)s,
                      %(day_seq)s,
                      %(side)s,
                      %(status)s,
                      %(entry_price)s,
                      %(exit_price)s,
                      %(stop_loss)s,
                      %(take_profit)s,
                      %(confidence)s,
                      %(score)s,
                      %(reason)s,
                      %(context)s,
                      %(source)s) ON CONFLICT (exchange_id, symbol_id, screener_id, timeframe, signal_ts)
              DO NOTHING
              RETURNING 1;
              """

        inserted = 0
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    rr = dict(r)

                    # ✅ symbol всегда строка
                    rr["symbol"] = str(rr.get("symbol") or "")

                    # ✅ context -> Jsonb с кастомным dumps (datetime не ломается)
                    ctx = rr.get("context") or {}
                    if isinstance(ctx, (dict, list)):
                        rr["context"] = Jsonb(ctx, dumps=_json_dumps)
                    else:
                        rr["context"] = Jsonb({}, dumps=_json_dumps)

                    cur.execute(sql, rr)
                    if cur.fetchone() is not None:
                        inserted += 1

        return inserted

    def _ensure_screeners_schema(self) -> None:
        """
        Создаёт/чинит таблицы для скринеров (без падений, без потери данных).
        """
        if getattr(self, "_screeners_schema_ready", False):
            return

        ddl = r"""
        -- 1) Базовая таблица screeners (если её нет)
        CREATE TABLE IF NOT EXISTS public.screeners (
            screener_id  BIGSERIAL PRIMARY KEY,
            name         TEXT NOT NULL,
            version      TEXT NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(name, version)
        );

        -- 2) Если таблица уже существовала в другой форме — приводим к виду screener_id PK
        DO $$
        BEGIN
            -- если нет колонки screener_id
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='screeners'
                  AND column_name='screener_id'
            ) THEN

                -- если есть колонка id -> переименуем её в screener_id
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name='screeners'
                      AND column_name='id'
                ) THEN
                    EXECUTE 'ALTER TABLE public.screeners RENAME COLUMN id TO screener_id';
                ELSE
                    -- иначе добавим новую колонку
                    EXECUTE 'ALTER TABLE public.screeners ADD COLUMN screener_id BIGSERIAL';
                END IF;
            END IF;

            -- убедимся, что есть PRIMARY KEY
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname='public'
                  AND t.relname='screeners'
                  AND c.contype='p'
            ) THEN
                EXECUTE 'ALTER TABLE public.screeners ADD PRIMARY KEY (screener_id)';
            END IF;

            -- гарантируем UNIQUE(name, version)
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname='public'
                  AND t.relname='screeners'
                  AND c.contype='u'
            ) THEN
                -- На всякий случай (если unique не было)
                BEGIN
                    EXECUTE 'ALTER TABLE public.screeners ADD CONSTRAINT ux_screeners_name_version UNIQUE(name, version)';
                EXCEPTION WHEN others THEN
                    -- если конфликтует (уже существует) — игнор
                END;
            END IF;

        END $$;

        -- 3) Таблица запусков (runs)
        CREATE TABLE IF NOT EXISTS public.screener_runs (
            run_id       BIGSERIAL PRIMARY KEY,
            screener_id  BIGINT NOT NULL REFERENCES public.screeners(screener_id) ON DELETE CASCADE,
            interval     TEXT NULL,
            started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at  TIMESTAMPTZ NULL,
            status       TEXT NOT NULL DEFAULT 'RUNNING',
            error        TEXT NULL,
            stats_json   JSONB NULL
        );

        CREATE INDEX IF NOT EXISTS ix_screener_runs_screener_started
            ON public.screener_runs(screener_id, started_at DESC);

        -- 4) Таблица сигналов
        CREATE TABLE IF NOT EXISTS public.screener_signals (
            signal_id    BIGSERIAL PRIMARY KEY,
            screener_id  BIGINT NOT NULL REFERENCES public.screeners(screener_id) ON DELETE CASCADE,
            run_id       BIGINT NULL REFERENCES public.screener_runs(run_id) ON DELETE SET NULL,

            exchange_id  INT NULL,
            symbol_id    INT NULL,
            timeframe    TEXT NULL,

            signal_ts    TIMESTAMPTZ NULL,
            side         TEXT NULL,

            entry_price  DOUBLE PRECISION NULL,
            up_level     DOUBLE PRECISION NULL,
            down_level   DOUBLE PRECISION NULL,
            score        DOUBLE PRECISION NULL,

            plot_path    TEXT NULL,
            payload_json JSONB NULL,

            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        -- idempotency: можно вставлять повторно без дублей
        CREATE UNIQUE INDEX IF NOT EXISTS ux_screener_signals_idem
            ON public.screener_signals(screener_id, exchange_id, symbol_id, timeframe, signal_ts, side);
        """

        self.exec_ddl(ddl)
        self._screeners_schema_ready = True


    def ensure_screener(self, *, name: str, version: str, description: str | None = None) -> int:
        """
        Ensure screener exists and return screener_id.

        ВАЖНО:
        В БД сейчас UNIQUE только на (name), поэтому ON CONFLICT тоже должен быть по (name).
        Версию просто обновляем.
        """
        self._ensure_screeners_schema()

        name = str(name or "").strip()
        version = str(version or "").strip()

        if not name:
            raise ValueError("ensure_screener(): name is empty")

        sql = """
            INSERT INTO public.screeners (name, version, description, is_enabled, created_at, updated_at)
            VALUES (%s, %s, %s, TRUE, NOW(), NOW())
            ON CONFLICT (name)
            DO UPDATE SET
                version = EXCLUDED.version,
                description = COALESCE(screeners.description, EXCLUDED.description),
                is_enabled = TRUE,
                updated_at = NOW()
            RETURNING screener_id
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (name, version, description))
                screener_id = cur.fetchone()[0]
            conn.commit()

        return int(screener_id)


    def start_screener_run(self, *, screener_id: int, interval: str | None = None) -> int:
        """
        Создаёт запись о запуске скринера и возвращает run_id.
        """
        self._ensure_screeners_schema()

        sql = """
            INSERT INTO public.screener_runs (screener_id, interval, status, started_at)
            VALUES (%s, %s, 'RUNNING', NOW())
            RETURNING run_id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(screener_id), str(interval) if interval else None))
                run_id = cur.fetchone()[0]
            conn.commit()
        return int(run_id)

    def finish_screener_run(self, *, run_id: int, status: str = "OK", error: str | None = None, stats: dict | None = None) -> int:
        """
        Закрывает run (OK/FAILED).
        """
        self._ensure_screeners_schema()

        stats_json = None
        if stats is not None:
            try:
                stats_json = Jsonb(stats)
            except Exception:
                stats_json = None

        sql = """
            UPDATE public.screener_runs
            SET finished_at = NOW(),
                status = %s,
                error = %s,
                stats_json = COALESCE(%s, stats_json)
            WHERE run_id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(status), error, stats_json, int(run_id)))
                n = cur.rowcount or 0
            conn.commit()
        return int(n)

    def insert_screener_signals(self, rows: Sequence[dict]) -> int:
        """
        Сохраняет сигналы скринера (idempotent).
        Ожидаемые ключи (минимум):
          screener_id, run_id, exchange_id, symbol_id, timeframe, signal_ts, side
          entry_price/up_level/down_level/score/plot_path/payload_json - опционально
        """
        self._ensure_screeners_schema()

        rows = list(rows or [])
        if not rows:
            return 0

        prepared: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r or {})

            d["screener_id"] = int(d.get("screener_id") or 0)
            d["run_id"] = int(d.get("run_id") or 0) if d.get("run_id") is not None else None

            d["exchange_id"] = int(d.get("exchange_id") or 0) if d.get("exchange_id") is not None else None
            d["symbol_id"] = int(d.get("symbol_id") or 0) if d.get("symbol_id") is not None else None

            d["timeframe"] = str(d.get("timeframe") or "") or None
            d["side"] = str(d.get("side") or "") or None
            d["signal_ts"] = d.get("signal_ts")

            # числа
            for k in ("entry_price", "up_level", "down_level", "score"):
                if k in d and d[k] is not None:
                    try:
                        d[k] = float(d[k])
                    except Exception:
                        d[k] = None

            # payload_json -> Jsonb
            payload = d.get("payload_json")
            if payload is not None:
                try:
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    d["payload_json"] = Jsonb(payload)
                except Exception:
                    d["payload_json"] = None
            else:
                d["payload_json"] = None

            d["plot_path"] = str(d.get("plot_path")) if d.get("plot_path") is not None else None

            prepared.append(d)

        sql = """
            INSERT INTO public.screener_signals (
                screener_id, run_id,
                exchange_id, symbol_id, timeframe,
                signal_ts, side,
                entry_price, up_level, down_level, score,
                plot_path, payload_json
            )
            VALUES (
                %(screener_id)s, %(run_id)s,
                %(exchange_id)s, %(symbol_id)s, %(timeframe)s,
                %(signal_ts)s, %(side)s,
                %(entry_price)s, %(up_level)s, %(down_level)s, %(score)s,
                %(plot_path)s, %(payload_json)s
            )
            ON CONFLICT (screener_id, exchange_id, symbol_id, timeframe, signal_ts, side)
            DO NOTHING
        """
        return self._exec_many(sql, prepared)


    # ======================================================================
    # POSITIONS (FIXED)
    # ======================================================================

    def upsert_position(self, position: Any) -> int:
        return self.upsert_positions([position])

    @staticmethod
    def _norm_side(v: Any) -> str:
        s = str(v or "").upper().strip()
        if s in ("LONG", "SHORT", "FLAT"):
            return s
        return "FLAT"

    @staticmethod
    def _f(x: Any, default: float = 0.0) -> float:
        try:
            v = float(x)
            if v != v:  # NaN
                return float(default)
            return v
        except Exception:
            return float(default)

    def _normalize_positions_row(self, row: dict) -> dict:
        """
        Делает row безопасным для constraints таблицы positions:

        chk_flat_consistency:
          (side='FLAT' AND qty=0 AND status='CLOSED')
          OR (side IN ('LONG','SHORT') AND qty>0 AND status='OPEN')

        + гарантирует наличие ключей, которые ожидает SQL.
        """
        if not isinstance(row, dict):
            raise TypeError("positions row must be dict")

        now = _utcnow()

        row["exchange_id"] = int(row.get("exchange_id") or 0)
        row["account_id"] = int(row.get("account_id") or 0)
        row["symbol_id"] = int(row.get("symbol_id") or 0)

        row.setdefault("strategy_id", "unknown")
        row.setdefault("strategy_name", None)

        row.setdefault("pos_uid", "")
        row.setdefault("opened_at", None)
        row.setdefault("closed_at", None)

        side = self._norm_side(row.get("side"))
        qty = abs(self._f(row.get("qty"), 0.0))

        if qty <= 0.0:
            row["side"] = "FLAT"
            row["qty"] = 0.0
            row["status"] = "CLOSED"

            row.setdefault("entry_price", 0.0)
            row.setdefault("avg_price", 0.0)
            row.setdefault("exit_price", 0.0)
            row.setdefault("mark_price", 0.0)
            row.setdefault("unrealized_pnl", 0.0)
        else:
            if side not in ("LONG", "SHORT"):
                side = "LONG"
            row["side"] = side
            row["qty"] = float(qty)
            row["status"] = "OPEN"

            row.setdefault("entry_price", None)
            row.setdefault("avg_price", None)
            row.setdefault("exit_price", None)
            row.setdefault("mark_price", None)
            row.setdefault("unrealized_pnl", None)

        row.setdefault("position_value_usdt", None)
        row.setdefault("scale_in_count", 0)
        row.setdefault("realized_pnl", 0.0)
        row.setdefault("fees", 0.0)

        row.setdefault("last_trade_id", None)
        row.setdefault("last_ts", None)

        if row.get("updated_at") is None:
            row["updated_at"] = now

        row.setdefault("source", "position_aggregate")
        return row

    def upsert_positions(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0

        norm_rows: list[dict] = []
        for r in rows:
            rr = dict(r or {})
            rr = self._normalize_positions_row(rr)
            norm_rows.append(rr)

        sql = """
            INSERT INTO positions (
                exchange_id, account_id, symbol_id,
                strategy_id, strategy_name,
                pos_uid,
                side, qty,
                entry_price, avg_price, exit_price,
                mark_price, unrealized_pnl,
                position_value_usdt,
                scale_in_count,
                realized_pnl, fees,
                last_trade_id, last_ts,
                status,
                opened_at, closed_at,
                updated_at, source
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(symbol_id)s,
                %(strategy_id)s, %(strategy_name)s,
                %(pos_uid)s,
                %(side)s, %(qty)s,
                %(entry_price)s, %(avg_price)s, %(exit_price)s,
                %(mark_price)s, %(unrealized_pnl)s,
                %(position_value_usdt)s,
                %(scale_in_count)s,
                %(realized_pnl)s, %(fees)s,
                %(last_trade_id)s, %(last_ts)s,
                %(status)s,
                %(opened_at)s, %(closed_at)s,
                %(updated_at)s, %(source)s
            )
            ON CONFLICT (exchange_id, account_id, symbol_id)
            DO UPDATE SET
                strategy_id = CASE
                    WHEN positions.strategy_id = 'unknown'
                      AND EXCLUDED.strategy_id IS NOT NULL
                      AND EXCLUDED.strategy_id <> 'unknown'
                    THEN EXCLUDED.strategy_id
                    ELSE positions.strategy_id
                END,
                strategy_name = COALESCE(positions.strategy_name, EXCLUDED.strategy_name),

                pos_uid = CASE
                    WHEN COALESCE(positions.pos_uid, '') <> '' THEN positions.pos_uid
                    WHEN COALESCE(EXCLUDED.pos_uid, '') <> '' THEN EXCLUDED.pos_uid
                    ELSE positions.pos_uid
                END,

                opened_at = COALESCE(positions.opened_at, EXCLUDED.opened_at),

                closed_at = CASE
                    WHEN positions.closed_at IS NOT NULL THEN positions.closed_at
                    WHEN EXCLUDED.closed_at IS NOT NULL THEN EXCLUDED.closed_at
                    ELSE positions.closed_at
                END,

                side = EXCLUDED.side,
                qty = EXCLUDED.qty,
                status = EXCLUDED.status,

                entry_price = EXCLUDED.entry_price,
                avg_price = EXCLUDED.avg_price,
                exit_price = EXCLUDED.exit_price,
                mark_price = EXCLUDED.mark_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                position_value_usdt = EXCLUDED.position_value_usdt,

                scale_in_count = COALESCE(EXCLUDED.scale_in_count, positions.scale_in_count),

                realized_pnl = EXCLUDED.realized_pnl,
                fees = EXCLUDED.fees,

                last_trade_id = COALESCE(NULLIF(EXCLUDED.last_trade_id, ''), positions.last_trade_id),
                last_ts = COALESCE(EXCLUDED.last_ts, positions.last_ts),

                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source
        """

        return self._exec_many(sql, norm_rows)

    # ======================================================================
    # POSITION SNAPSHOTS
    # ======================================================================

    def upsert_position_snapshots(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0

        now = _utcnow()
        prepared: list[dict] = []

        for r in rows:
            d = dict(r or {})

            d["exchange_id"] = int(d.get("exchange_id") or 0)
            d["account_id"] = int(d.get("account_id") or 0)
            d["symbol_id"] = int(d.get("symbol_id") or 0)

            qty = abs(self._f(d.get("qty"), 0.0))
            side = self._norm_side(d.get("side"))

            if qty <= 0:
                d["qty"] = 0.0
                d["side"] = "FLAT"
            else:
                d["qty"] = float(qty)
                if side not in ("LONG", "SHORT"):
                    side = "LONG"
                d["side"] = side

            entry = self._f(d.get("entry_price"), 0.0)
            mark = self._f(d.get("mark_price"), entry)

            d["entry_price"] = float(entry)
            d["mark_price"] = float(mark)

            pv = d.get("position_value", None)
            if pv is None:
                pv = abs(qty) * float(mark)
            d["position_value"] = float(self._f(pv, 0.0))

            d["unrealized_pnl"] = float(self._f(d.get("unrealized_pnl"), 0.0))
            d["realized_pnl"] = float(self._f(d.get("realized_pnl"), 0.0))
            d["fees"] = float(self._f(d.get("fees"), 0.0))

            d.setdefault("last_ts", now)
            d.setdefault("updated_at", now)
            d.setdefault("source", "position_aggregate")

            prepared.append(d)

        sql = """
            INSERT INTO position_snapshots (
                exchange_id, account_id, symbol_id,
                side, qty, entry_price, mark_price,
                position_value, unrealized_pnl, realized_pnl, fees,
                last_ts, updated_at, source
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(symbol_id)s,
                %(side)s, %(qty)s, %(entry_price)s, %(mark_price)s,
                %(position_value)s, %(unrealized_pnl)s, %(realized_pnl)s, %(fees)s,
                %(last_ts)s, %(updated_at)s, %(source)s
            )
            ON CONFLICT (exchange_id, account_id, symbol_id)
            DO UPDATE SET
                side = EXCLUDED.side,
                qty = EXCLUDED.qty,
                entry_price = EXCLUDED.entry_price,
                mark_price = EXCLUDED.mark_price,
                position_value = EXCLUDED.position_value,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                realized_pnl = EXCLUDED.realized_pnl,
                fees = EXCLUDED.fees,
                last_ts = EXCLUDED.last_ts,
                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source
        """
        return self._exec_many(sql, prepared)

    # ======================================================================
    # POSITION LEDGER
    # ======================================================================

    def upsert_position_ledger(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0

        now = _utcnow()
        prepared: list[dict] = []

        for r in rows:
            d = dict(r or {})
            d["exchange_id"] = int(d.get("exchange_id") or 0)
            d["account_id"] = int(d.get("account_id") or 0)
            d["symbol_id"] = int(d.get("symbol_id") or 0)

            d["pos_uid"] = str(d.get("pos_uid") or "").strip()
            if not d["pos_uid"]:
                d["pos_uid"] = f"{d['exchange_id']}:{d['account_id']}:{d['symbol_id']}"

            d.setdefault("strategy_id", "unknown")
            d.setdefault("strategy_name", None)

            d.setdefault("updated_at", now)
            d.setdefault("source", "ledger")

            prepared.append(d)

        sql = """
            INSERT INTO position_ledger (
                exchange_id, account_id, pos_uid, symbol_id,
                strategy_id, strategy_name,
                side, status,
                opened_at, closed_at,
                entry_price, avg_price, exit_price,
                qty_opened, qty_current, qty_closed,
                position_value_usdt, scale_in_count,
                realized_pnl, fees,
                updated_at, source
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(pos_uid)s, %(symbol_id)s,
                %(strategy_id)s, %(strategy_name)s,
                %(side)s, %(status)s,
                %(opened_at)s, %(closed_at)s,
                %(entry_price)s, %(avg_price)s, %(exit_price)s,
                %(qty_opened)s, %(qty_current)s, %(qty_closed)s,
                %(position_value_usdt)s, %(scale_in_count)s,
                %(realized_pnl)s, %(fees)s,
                %(updated_at)s, %(source)s
            )
            ON CONFLICT (exchange_id, account_id, pos_uid, opened_at)
            DO UPDATE SET
                closed_at = COALESCE(position_ledger.closed_at, EXCLUDED.closed_at),

                side = EXCLUDED.side,
                status = EXCLUDED.status,

                strategy_id = CASE
                    WHEN position_ledger.strategy_id = 'unknown'
                      AND EXCLUDED.strategy_id IS NOT NULL
                      AND EXCLUDED.strategy_id <> 'unknown'
                    THEN EXCLUDED.strategy_id
                    ELSE position_ledger.strategy_id
                END,
                strategy_name = COALESCE(position_ledger.strategy_name, EXCLUDED.strategy_name),

                entry_price = COALESCE(position_ledger.entry_price, EXCLUDED.entry_price),
                avg_price = COALESCE(EXCLUDED.avg_price, position_ledger.avg_price),
                exit_price = COALESCE(EXCLUDED.exit_price, position_ledger.exit_price),

                qty_opened = COALESCE(position_ledger.qty_opened, EXCLUDED.qty_opened),
                qty_current = EXCLUDED.qty_current,
                qty_closed = GREATEST(COALESCE(position_ledger.qty_closed, 0), COALESCE(EXCLUDED.qty_closed, 0)),

                position_value_usdt = EXCLUDED.position_value_usdt,
                scale_in_count = EXCLUDED.scale_in_count,
                realized_pnl = EXCLUDED.realized_pnl,
                fees = EXCLUDED.fees,

                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source
        """
        return self._exec_many(sql, prepared)

    # ======================================================================
    # BALANCE / EQUITY SNAPSHOTS
    # ======================================================================

    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0

        query = """
            INSERT INTO account_balance_snapshots (
                exchange_id, account_id, ts,
                wallet_balance, equity, available_balance, margin_used,
                unrealized_pnl, source
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(ts)s,
                %(wallet_balance)s, %(equity)s, %(available_balance)s, %(margin_used)s,
                %(unrealized_pnl)s, %(source)s
            )
        """
        return self._exec_many(query, rows)

    # ======================================================================
    # MARKET STATE (backward compatible kwargs)
    # ======================================================================

    @staticmethod
    def _resolve_ids(exchange, account, exchange_id, account_id):
        return exchange_id or exchange, account_id or account

    def get_latest_positions(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
            SELECT s.symbol,
                   p.symbol_id,
                   p.qty,
                   p.entry_price,
                   p.unrealized_pnl,
                   p.mark_price,
                   p.status,
                   p.updated_at
            FROM positions p
                JOIN symbols s ON s.symbol_id = p.symbol_id
            WHERE p.exchange_id = %s
              AND p.account_id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ex_id, acc_id))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

        out = {}
        for row in rows:
            r = dict(zip(cols, row))
            sym = r.pop("symbol")
            out[sym] = r
        return out

    def get_latest_balances(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
            SELECT wallet_balance, available_balance, margin_used, equity, unrealized_pnl
            FROM account_balance_snapshots
            WHERE exchange_id = %s
              AND account_id = %s
            ORDER BY ts DESC
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ex_id, acc_id))
                row = cur.fetchone()
                if not row:
                    return {}
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    # ======================================================================
    # OMS / WS HELPERS
    # ======================================================================

    def client_order_exists(self, *, exchange_id: int, account_id: int, client_order_id: str) -> bool:
        row = self._exec_one(
            """
            SELECT 1 FROM orders
            WHERE exchange_id = %s AND account_id = %s AND client_order_id = %s
            LIMIT 1
            """,
            (exchange_id, account_id, client_order_id),
        )
        return bool(row)

    def get_order(self, *, exchange_id: int, account_id: int, order_id: str) -> dict | None:
        query = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   strategy_id,
                   pos_uid,
                   client_order_id,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   status,
                   created_at,
                   updated_at,
                   source,
                   ts_ms,
                   raw_json
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    def expire_stuck_pending(self, *, exchange_id: int, account_id: int, timeout_sec: int = 60) -> int:
        """
        Expire PENDING_SUBMIT / PENDING placeholders older than timeout_sec.
        """
        query = """
            UPDATE orders
            SET status = 'EXPIRED', updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND status IN ('PENDING', 'PENDING_SUBMIT')
              AND updated_at < NOW() - (INTERVAL '1 second' * %s)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), int(timeout_sec)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    # ======================================================================
    # RETENTION
    # ======================================================================

    def cleanup_candles(self, *, exchange_id: int, interval: str, keep_days: int) -> int:
        """
        Delete old candles from public.candles.
        """
        query = """
            DELETE
            FROM candles
            WHERE exchange_id = %s
              AND interval = %s
              AND open_time < NOW() - (INTERVAL '1 day' * %s)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), str(interval), int(keep_days)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    # ======================================================================
    # EXCHANGE INFO (filters)
    # ======================================================================

    def upsert_symbol_filters(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """
        Upsert symbol trading filters.

        Expected keys:
          exchange_id, symbol_id,
          price_tick, qty_step,
          min_qty, max_qty,
          min_notional,
          max_leverage,
          margin_type,
          updated_at
        """
        if not rows:
            return 0

        sql = """
            INSERT INTO symbol_filters (
                exchange_id,
                symbol_id,
                price_tick,
                qty_step,
                min_qty,
                max_qty,
                min_notional,
                max_leverage,
                margin_type,
                updated_at
            )
            VALUES (
                %(exchange_id)s,
                %(symbol_id)s,
                %(price_tick)s,
                %(qty_step)s,
                %(min_qty)s,
                %(max_qty)s,
                %(min_notional)s,
                %(max_leverage)s,
                %(margin_type)s,
                %(updated_at)s
            )
            ON CONFLICT (exchange_id, symbol_id)
            DO UPDATE SET
                price_tick = EXCLUDED.price_tick,
                qty_step = EXCLUDED.qty_step,
                min_qty = EXCLUDED.min_qty,
                max_qty = EXCLUDED.max_qty,
                min_notional = EXCLUDED.min_notional,
                max_leverage = EXCLUDED.max_leverage,
                margin_type = EXCLUDED.margin_type,
                updated_at = EXCLUDED.updated_at
        """

        now = _utcnow()
        prepared: list[dict[str, Any]] = []

        for r in rows:
            d = dict(r)

            if "updated_at" not in d or d["updated_at"] is None:
                d["updated_at"] = now

            d["exchange_id"] = int(d["exchange_id"])
            d["symbol_id"] = int(d["symbol_id"])

            for k in ("price_tick", "qty_step", "min_qty", "max_qty", "min_notional"):
                if k in d and d[k] is not None:
                    d[k] = float(d[k])

            if d.get("max_leverage") is not None:
                d["max_leverage"] = int(d["max_leverage"])
            if d.get("margin_type") is not None:
                d["margin_type"] = str(d["margin_type"])

            prepared.append(d)

        return self._exec_many(sql, prepared)

    # ======================================================================
    # ORDERS / TRADES
    # ======================================================================

    def upsert_orders(self, orders: list[dict]) -> None:
        """
        Upsert order snapshots.
        Status, qty, filled_qty are already resolved by OrderAggregate (FSM).
        """
        if not orders:
            return


        rows: list[dict] = []
        for o in orders:
            rows.append(
                {
                    "exchange_id": int(o["exchange_id"]),
                    "account_id": int(o["account_id"]),
                    "symbol_id": int(o["symbol_id"]),
                    "order_id": str(o["order_id"]),
                    "client_order_id": o.get("client_order_id"),
                    "side": str(o.get("side") or ""),
                    "type": str(o.get("type") or ""),
                    "reduce_only": bool(o.get("reduce_only") or False),
                    "price": o.get("price"),
                    "qty": float(o.get("qty") or 0.0),
                    "filled_qty": float(o.get("filled_qty") or 0.0),
                    "status": str(o.get("status") or "NEW"),
                    "strategy_id": o.get("strategy_id"),
                    "pos_uid": o.get("pos_uid"),
                    "ts_ms": int(o.get("ts_ms") or 0),
                    "raw_json": self._to_jsonb(o.get("raw") or o.get("raw_json") or {}),

                }
            )

        # Split to avoid duplicate client_order_id (we have partial unique index on it)
        def _has_cid(o: dict) -> bool:
            cid = o.get("client_order_id")
            if cid is None:
                return False
            cid = str(cid).strip()
            return cid != ""

        with_cid = [r for r in rows if _has_cid(r)]
        no_cid = [r for r in rows if not _has_cid(r)]

        if with_cid:
            query_cid = """
                INSERT INTO orders (
                    exchange_id,
                    account_id, 
                    order_id,
                    client_order_id,
                    symbol_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty,
                    status, 
                    strategy_id,
                    pos_uid,
                    ts_ms,
                    raw_json,
                    updated_at
                )
                VALUES (
                    %(exchange_id)s,
                    %(account_id)s,
                    %(order_id)s,
                    %(client_order_id)s,
                    %(symbol_id)s,
                    %(side)s,
                    %(type)s,
                    %(reduce_only)s,
                    %(price)s, 
                    %(qty)s,
                    %(filled_qty)s,
                    %(status)s,
                    %(strategy_id)s,
                    %(pos_uid)s,
                    %(ts_ms)s,
                    %(raw_json)s,
                    NOW()
                )
                ON CONFLICT (exchange_id, account_id, client_order_id)
                WHERE client_order_id IS NOT NULL AND client_order_id <> ''
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    symbol_id = EXCLUDED.symbol_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only, 
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status,
                    strategy_id = COALESCE(orders.strategy_id, EXCLUDED.strategy_id), 
                    pos_uid = COALESCE(orders.pos_uid, EXCLUDED.pos_uid),
                    ts_ms = GREATEST(COALESCE(orders.ts_ms, 0), COALESCE(EXCLUDED.ts_ms, 0)),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json),
                    updated_at = NOW()
            """
            self._exec_many(query_cid, with_cid)

        if no_cid:
            query_oid = """
                INSERT INTO orders (
                    exchange_id,
                    account_id, 
                    order_id,
                    client_order_id,
                    symbol_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty,
                    status, 
                    strategy_id,
                    pos_uid,
                    ts_ms,
                    raw_json,
                    updated_at
                )
                VALUES (
                    %(exchange_id)s,
                    %(account_id)s,
                    %(order_id)s,
                    %(client_order_id)s,
                    %(symbol_id)s,
                    %(side)s,
                    %(type)s,
                    %(reduce_only)s,
                    %(price)s, 
                    %(qty)s,
                    %(filled_qty)s,
                    %(status)s,
                    %(strategy_id)s,
                    %(pos_uid)s,
                    %(ts_ms)s,
                    %(raw_json)s,
                    NOW()
                )
                ON CONFLICT (exchange_id, account_id, order_id)
                DO UPDATE SET
                    client_order_id = COALESCE(EXCLUDED.client_order_id, orders.client_order_id),
                    symbol_id = EXCLUDED.symbol_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only, 
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status,
                    strategy_id = COALESCE(orders.strategy_id, EXCLUDED.strategy_id), 
                    pos_uid = COALESCE(orders.pos_uid, EXCLUDED.pos_uid),
                    ts_ms = GREATEST(COALESCE(orders.ts_ms, 0), COALESCE(EXCLUDED.ts_ms, 0)),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json),
                    updated_at = NOW()
            """
            self._exec_many(query_oid, no_cid)

    # ======================================================================
    # ALGO ORDERS (Binance conditional / openAlgoOrders)
    # ======================================================================

    def upsert_algo_orders(self, rows: list[dict]) -> None:
        """Upsert algo order snapshots (e.g. Binance /fapi/v1/openAlgoOrders).

        Expected keys in each row (best-effort; extra keys ignored):
          exchange_id, account_id, client_algo_id, algo_id, symbol,
          side, position_side, type, quantity, trigger_price, working_type,
          status, strategy_id, pos_uid, raw_json
        """
        if not rows:
            return

        # psycopg3 doesn't adapt plain dict -> jsonb automatically in executemany();
        # wrap payloads explicitly.
        prepared: list[dict] = []
        for r in rows:
            d = dict(r)
            if "raw_json" in d and d["raw_json"] is not None and not isinstance(d["raw_json"], Jsonb):
                if isinstance(d["raw_json"], (dict, list)):
                    d["raw_json"] = Jsonb(d["raw_json"])
            prepared.append(d)

        query = """
            INSERT INTO algo_orders (
                exchange_id,
                account_id,
                client_algo_id,
                algo_id,
                symbol,
                side,
                position_side,
                type,
                quantity,
                trigger_price,
                working_type,
                status,
                strategy_id,
                pos_uid,
                raw_json,
                updated_at
            )
            VALUES (
                %(exchange_id)s,
                %(account_id)s,
                %(client_algo_id)s,
                %(algo_id)s,
                %(symbol)s,
                %(side)s,
                %(position_side)s,
                %(type)s,
                %(quantity)s,
                %(trigger_price)s,
                %(working_type)s,
                %(status)s,
                %(strategy_id)s,
                %(pos_uid)s,
                %(raw_json)s,
                NOW()
            )
            ON CONFLICT (exchange_id, account_id, client_algo_id)
            DO UPDATE SET
                algo_id        = COALESCE(EXCLUDED.algo_id, algo_orders.algo_id),
                symbol         = COALESCE(EXCLUDED.symbol, algo_orders.symbol),
                side           = COALESCE(EXCLUDED.side, algo_orders.side),
                position_side  = COALESCE(EXCLUDED.position_side, algo_orders.position_side),
                type           = COALESCE(EXCLUDED.type, algo_orders.type),
                quantity       = COALESCE(EXCLUDED.quantity, algo_orders.quantity),
                trigger_price  = COALESCE(EXCLUDED.trigger_price, algo_orders.trigger_price),
                working_type   = COALESCE(EXCLUDED.working_type, algo_orders.working_type),
                status         = COALESCE(EXCLUDED.status, algo_orders.status),
                strategy_id    = COALESCE(EXCLUDED.strategy_id, algo_orders.strategy_id),
                pos_uid        = COALESCE(EXCLUDED.pos_uid, algo_orders.pos_uid),
                raw_json       = COALESCE(EXCLUDED.raw_json, algo_orders.raw_json),
                updated_at     = NOW()
        """
        self._exec_many(query, prepared)
    def set_algo_order_status(
        self,
        *,
        exchange_id: int,
        account_id: int,
        client_algo_id: str,
        status: str,
        raw_json: dict | None = None,
    ) -> int:
        """Update algo order status by client_algo_id (best-effort)."""
        if raw_json is not None and not isinstance(raw_json, Jsonb):
            if isinstance(raw_json, (dict, list)):
                raw_json = Jsonb(raw_json)

        query = """
            UPDATE algo_orders
            SET status = %s,
                raw_json = COALESCE(%s::jsonb, raw_json),
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_algo_id = %s
        """
        return self.execute(
            query,
            (
                str(status),
                raw_json,
                int(exchange_id),
                int(account_id),
                str(client_algo_id),
            ),
        )



    def sync_open_algo_orders_not_found(
        self,
        *,
        exchange_id: int,
        account_id: int,
        active_client_algo_ids: list[str],
        prefix: str = "TL_",
        not_found_status: str = "CANCELED",
        cancel_reason: str = "sync_not_in_openAlgoOrders",
        cancel_source: str = "sync_openAlgoOrders",
    ) -> int:
        """Mark OPEN algo orders as *not found* (defaults to CANCELED) when they are no longer present on the exchange.

        This is a best-effort reconciliation helper. Caller should pass the current list of openAlgoOrders
        client IDs from the exchange. When an OPEN record is marked, we also annotate raw_json with
        cancel_reason / cancel_source so it is clear this status came from reconciliation (vs bot-driven cancel).
        """

        active = [str(x).strip() for x in (active_client_algo_ids or []) if str(x).strip()]

        meta = {"cancel_reason": str(cancel_reason), "cancel_source": str(cancel_source)}
        meta_jsonb = Jsonb(meta)

        # If exchange returned nothing, mark all OPEN prefixed orders as not-found.
        if not active:
            query = """
                UPDATE algo_orders
                SET status = %s,
                    raw_json = raw_json || %s,
                    updated_at = NOW()
                WHERE exchange_id = %s
                  AND account_id = %s
                  AND status = 'OPEN'
                  AND client_algo_id LIKE %s
            """
            return int(
                self.execute(
                    query,
                    (
                        str(not_found_status),
                        meta_jsonb,
                        int(exchange_id),
                        int(account_id),
                        str(prefix) + "%",
                    ),
                )
                or 0
            )

        query = """
            UPDATE algo_orders
            SET status = %s,
                raw_json = raw_json || %s,
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND status = 'OPEN'
              AND client_algo_id LIKE %s
              AND NOT (client_algo_id = ANY(%s))
        """
        return int(
            self.execute(
                query,
                (
                    str(not_found_status),
                    meta_jsonb,
                    int(exchange_id),
                    int(account_id),
                    str(prefix) + "%",
                    active,
                ),
            )
            or 0
        )



    def set_order_status(self, *, exchange_id: int, account_id: int, order_id: str, status: str) -> int:
        query = """
            UPDATE orders
            SET status = %s,
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (str(status), int(exchange_id), int(account_id), str(order_id)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return

        query = """
            INSERT INTO trades (
                exchange_id, account_id, trade_id, order_id, symbol_id,
                strategy_id, pos_uid, side, price, qty, fee, fee_asset,
                realized_pnl, ts, source, raw_json
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s,
                %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s, %(fee_asset)s,
                %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s
            )
            ON CONFLICT (exchange_id, account_id, trade_id)
            DO UPDATE SET
                fee = EXCLUDED.fee,
                fee_asset = EXCLUDED.fee_asset,
                realized_pnl = EXCLUDED.realized_pnl,
                ts = EXCLUDED.ts,
                raw_json = EXCLUDED.raw_json
        """

        rows = []
        for t in trades:
            rows.append(
                {
                    "exchange_id": int(t["exchange_id"]),
                    "account_id": int(t["account_id"]),
                    "trade_id": str(t.get("trade_id") or ""),
                    "order_id": str(t.get("order_id") or ""),
                    "symbol_id": int(t["symbol_id"]),
                    "strategy_id": str(t.get("strategy_id") or "unknown"),
                    "pos_uid": str(t.get("pos_uid") or ""),
                    "side": str(t.get("side") or ""),
                    "price": float(t.get("price") or 0.0),
                    "qty": float(t.get("qty") or 0.0),
                    "fee": float(t.get("fee") or 0.0),
                    "fee_asset": str(t.get("fee_asset") or ""),
                    "realized_pnl": float(t.get("realized_pnl") or 0.0),
                    "ts": t["ts"],
                    "source": str(t.get("source") or "ws"),
                    "raw_json": self._to_jsonb(t),

                }
            )

        self._exec_many(query, prepared)

    def get_today_realized_pnl(self, exchange_id: int, account_id: int) -> float:
        query = """
            SELECT COALESCE(SUM(realized_pnl), 0)
            FROM trades
            WHERE exchange_id = %s
              AND account_id = %s
              AND ts >= date_trunc('day', now())
        """
        row = self._exec_one(query, (int(exchange_id), int(account_id)))
        return float(row[0] or 0.0)

    # ======================================================================
    # ID RESOLVERS
    # ======================================================================

    def _resolve_exchange_id(self, exchange: Any, exchange_id: Any):
        if exchange_id is not None:
            return int(exchange_id)
        if isinstance(exchange, int):
            return int(exchange)
        if isinstance(exchange, str):
            row = self._exec_one("SELECT exchange_id FROM exchanges WHERE name = %s", (exchange,))
            return int(row[0]) if row else None
        return None

    def _resolve_account_id(self, exchange_id: int, account: Any, account_id: Any):
        if account_id is not None:
            return int(account_id)
        if isinstance(account, int):
            return int(account)
        if isinstance(account, str) and exchange_id is not None:
            row = self._exec_one(
                "SELECT account_id FROM accounts WHERE exchange_id=%s AND account_name=%s",
                (int(exchange_id), account),
            )
            return int(row[0]) if row else None
        return None

    # ======================================================================
    # PLACEHOLDER ORDERS + ORDER EVENTS
    # ======================================================================

    def upsert_order_placeholder(self, row: dict) -> None:
        """
        row keys expected:
          exchange_id, account_id, order_id, symbol_id,
          strategy_id, pos_uid, client_order_id,
          side, type, reduce_only, price, qty,
          filled_qty, status, source, ts_ms,
          created_at, updated_at, raw_json
        """
        exchange_id = int(row["exchange_id"])
        account_id = int(row["account_id"])
        order_id = str(row["order_id"])
        symbol_id = int(row["symbol_id"])

        strategy_id = str(row.get("strategy_id") or "unknown")
        pos_uid = row.get("pos_uid")
        client_order_id = row.get("client_order_id")

        side = row.get("side")
        order_type = row.get("type")
        reduce_only = bool(row.get("reduce_only", False))
        price = row.get("price", None)
        qty = row.get("qty", None)
        filled_qty = row.get("filled_qty", 0.0)
        status = row.get("status", "PENDING")
        source = row.get("source", "oms")
        ts_ms = int(row.get("ts_ms") or int(time.time() * 1000))
        raw_json = row.get("raw_json")
        cid = ""
        if client_order_id is not None:
            cid = str(client_order_id).strip()

        if cid:
            query = """
                INSERT INTO orders (
                    exchange_id,
                    account_id,
                    order_id,
                    symbol_id,
                    strategy_id,
                    pos_uid,
                    client_order_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty, 
                    status,
                    source,
                    ts_ms,
                    created_at,
                    updated_at,
                    raw_json
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s, NOW()),
                    COALESCE(%s, NOW()),
                    %s
                )
                ON CONFLICT (exchange_id, account_id, client_order_id)
                WHERE client_order_id IS NOT NULL AND client_order_id <> ''
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    symbol_id = EXCLUDED.symbol_id,
                    strategy_id = EXCLUDED.strategy_id,
                    pos_uid = EXCLUDED.pos_uid,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only,
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status, 
                    source = EXCLUDED.source,
                    ts_ms = EXCLUDED.ts_ms,
                    updated_at = NOW(),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json)
            """
        else:
            query = """
                INSERT INTO orders (
                    exchange_id,
                    account_id,
                    order_id,
                    symbol_id,
                    strategy_id,
                    pos_uid,
                    client_order_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty, 
                    status,
                    source,
                    ts_ms,
                    created_at,
                    updated_at,
                    raw_json
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s, NOW()),
                    COALESCE(%s, NOW()),
                    %s
                )
                ON CONFLICT (exchange_id, account_id, order_id)
                DO UPDATE SET
                    symbol_id = EXCLUDED.symbol_id,
                    strategy_id = EXCLUDED.strategy_id,
                    pos_uid = EXCLUDED.pos_uid,
                    client_order_id = EXCLUDED.client_order_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only,
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status, 
                    source = EXCLUDED.source,
                    ts_ms = EXCLUDED.ts_ms,
                    updated_at = NOW(),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json)
            """


        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        exchange_id,
                        account_id,
                        order_id,
                        symbol_id,
                        strategy_id,
                        pos_uid,
                        client_order_id,
                        side,
                        order_type,
                        reduce_only,
                        price,
                        qty,
                        float(filled_qty or 0.0),
                        status,
                        source,
                        ts_ms,
                        row.get("created_at"),
                        row.get("updated_at"),
                        raw_json,
                    ),
                )
            conn.commit()

    def append_order_events(self, rows: Iterable[dict]) -> None:
        """Append immutable order events (idempotent)."""
        rows = list(rows or [])
        if not rows:
            return

        query = """
            INSERT INTO order_events (
                exchange_id, account_id, order_id, symbol_id,
                client_order_id, status, side, type, reduce_only,
                price, qty, filled_qty,
                source, ts_ms, recv_ts, raw_json
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s,
                %(client_order_id)s, %(status)s, %(side)s, %(type)s, %(reduce_only)s,
                %(price)s, %(qty)s, %(filled_qty)s,
                %(source)s, %(ts_ms)s, %(recv_ts)s, %(raw_json)s
            )
            ON CONFLICT DO NOTHING
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    def fetch_order_events(
        self,
        *,
        exchange_id: int,
        account_id: int,
        since_ts_ms: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Fetch order_events ordered by ts_ms ASC.
        Used for OMS rebuild.
        """
        sql = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   client_order_id,
                   status,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   source,
                   ts_ms,
                   recv_ts,
                   raw_json
            FROM order_events
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
        """

        params = {"exchange_id": int(exchange_id), "account_id": int(account_id)}

        if since_ts_ms is not None:
            sql += " AND ts_ms >= %(since_ts_ms)s"
            params["since_ts_ms"] = int(since_ts_ms)

        sql += " ORDER BY ts_ms ASC"

        if limit is not None:
            sql += " LIMIT %(limit)s"
            params["limit"] = int(limit)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [c.name for c in cur.description]

        return [dict(zip(cols, r)) for r in rows]

    # ======================================================================
    # CANDLES
    # ======================================================================

    def upsert_candles(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        now = _utcnow()
        for r in rows:
            r.setdefault("quote_volume", None)
            r.setdefault("trades", None)
            r.setdefault("taker_buy_base", None)
            r.setdefault("taker_buy_quote", None)
            r.setdefault("taker_sell_base", None)
            r.setdefault("taker_sell_quote", None)
            r.setdefault("delta_quote", None)
            r.setdefault("delta_base", None)
            r.setdefault("cvd_quote", None)
            r.setdefault("source", "unknown")
            r.setdefault("updated_at", now)

        sql = """
            INSERT INTO candles (
                exchange_id, symbol_id, interval, open_time,
                open, high, low, close, volume,
                quote_volume, trades,
                taker_buy_base, taker_buy_quote,
                taker_sell_base, taker_sell_quote,
                delta_quote, delta_base,
                cvd_quote,
                source, updated_at
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(interval)s, %(open_time)s,
                %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s,
                %(quote_volume)s, %(trades)s,
                %(taker_buy_base)s, %(taker_buy_quote)s,
                %(taker_sell_base)s, %(taker_sell_quote)s,
                %(delta_quote)s, %(delta_base)s,
                %(cvd_quote)s,
                %(source)s, %(updated_at)s
            )
            ON CONFLICT (exchange_id, symbol_id, interval, open_time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,

                quote_volume = COALESCE(EXCLUDED.quote_volume, candles.quote_volume),
                trades = COALESCE(EXCLUDED.trades, candles.trades),
                taker_buy_base = COALESCE(EXCLUDED.taker_buy_base, candles.taker_buy_base),
                taker_buy_quote = COALESCE(EXCLUDED.taker_buy_quote, candles.taker_buy_quote),
                taker_sell_base = COALESCE(EXCLUDED.taker_sell_base, candles.taker_sell_base),
                taker_sell_quote = COALESCE(EXCLUDED.taker_sell_quote, candles.taker_sell_quote),
                delta_quote = COALESCE(EXCLUDED.delta_quote, candles.delta_quote),
                delta_base = COALESCE(EXCLUDED.delta_base, candles.delta_base),
                cvd_quote = COALESCE(EXCLUDED.cvd_quote, candles.cvd_quote),
                source = EXCLUDED.source,
                updated_at = GREATEST(candles.updated_at, EXCLUDED.updated_at)
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        return len(rows)

    def get_candles_watermarks(self, *, exchange_id: int, intervals: List[str]) -> Dict[Tuple[int, str], datetime]:
        """
        Возвращает watermarks для candles:
          (symbol_id, interval) -> MAX(open_time)
        """
        wm: Dict[Tuple[int, str], datetime] = {}
        intervals = [str(x).strip() for x in (intervals or []) if str(x).strip()]
        if not intervals:
            return wm

        sql = """
            SELECT symbol_id, MAX(open_time) AS last_open_time
            FROM candles
            WHERE exchange_id = %s
              AND interval = %s
            GROUP BY symbol_id
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for itv in intervals:
                    cur.execute(sql, (int(exchange_id), str(itv)))
                    for sid, last_dt in (cur.fetchall() or []):
                        if sid is None or last_dt is None:
                            continue
                        wm[(int(sid), str(itv))] = last_dt

        return wm

    # ======================================================================
    # ORDERS QUERIES
    # ======================================================================

    def list_non_terminal_orders(self, *, exchange_id: int, account_id: int, symbol_ids=None) -> list[dict]:
        base = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   strategy_id,
                   pos_uid,
                   client_order_id,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   status,
                   created_at,
                   updated_at,
                   source,
                   ts_ms,
                   raw_json
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND status NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')
        """
        params = [int(exchange_id), int(account_id)]
        if symbol_ids:
            base += " AND symbol_id = ANY(%s)"
            params.append(list(map(int, symbol_ids)))
        base += " ORDER BY updated_at DESC LIMIT 500"

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(base, tuple(params))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in rows]

    def get_order_by_client_order_id(
        self,
        *,
        exchange_id: int,
        account_id: int,
        client_order_id: str,
        prefer_placeholder: bool = True,
    ) -> dict | None:
        if not client_order_id:
            return None

        q_ph = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_order_id = %s
              AND order_id LIKE 'PH::%%'
            ORDER BY updated_at DESC
            LIMIT 1
        """

        q_any = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_order_id = %s
            ORDER BY updated_at DESC
            LIMIT 1
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if prefer_placeholder:
                    cur.execute(q_ph, (int(exchange_id), int(account_id), str(client_order_id)))
                    row = cur.fetchone()
                    if row:
                        cols = [d[0] for d in cur.description]
                        return dict(zip(cols, row))

                cur.execute(q_any, (int(exchange_id), int(account_id), str(client_order_id)))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    # ======================================================================
    # FILTERS LOADER
    # ======================================================================

    def get_symbol_filters(self, *, exchange_id: int, symbol_id: int) -> dict | None:
        query = """
            SELECT price_tick,
                   qty_step,
                   min_qty,
                   max_qty,
                   min_notional,
                   max_leverage,
                   margin_type
            FROM symbol_filters
            WHERE exchange_id = %s
              AND symbol_id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(symbol_id)))
                row = cur.fetchone()

        if row is None:
            return None

        price_tick = float(row[0]) if row[0] is not None else None
        qty_step = float(row[1]) if row[1] is not None else None

        return {
            "price_tick": price_tick,
            "qty_step": qty_step,
            "min_qty": float(row[2]) if row[2] is not None else None,
            "max_qty": float(row[3]) if row[3] is not None else None,
            "min_notional": float(row[4]) if row[4] is not None else None,
            "max_leverage": int(row[5]) if row[5] is not None else None,
            "margin_type": row[6],
        }

    # ======================================================================
    # METRICS WINDOW
    # ======================================================================

    def fetch_orders_metrics_window(self, *, exchange_id: int, account_id: int, since_ts_ms: int) -> list[dict]:
        query = """
            SELECT client_order_id, order_id, status, source, ts_ms
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND ts_ms >= %s
              AND client_order_id IS NOT NULL
            ORDER BY ts_ms ASC
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), int(since_ts_ms)))
                rows = cur.fetchall()

        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "client_order_id": r[0],
                    "order_id": r[1],
                    "status": r[2],
                    "source": r[3],
                    "ts_ms": r[4],
                }
            )
        return out

    # ======================================================================
    # ACCOUNT STATE
    # ======================================================================

    def upsert_account_state(self, *, exchange_id: int, account_id: int, state: dict) -> None:
        sql = """
            INSERT INTO account_state (
                exchange_id,
                account_id,
                ts,
                wallet_balance,
                equity,
                available_balance,
                unrealized_pnl
            )
            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        int(exchange_id),
                        int(account_id),
                        state.get("wallet_balance"),
                        state.get("equity"),
                        state.get("available_balance"),
                        state.get("unrealized_pnl"),
                    ),
                )
            conn.commit()

    # ======================================================================
    # DDL EXECUTOR
    # ======================================================================

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

    # ======================================================================
    # TRADES FETCH
    # ======================================================================

    def fetch_trades(
        self,
        *,
        exchange_id: int,
        account_id: int,
        since_ts_ms: int = 0,
        limit: int = 200_000,
    ) -> list[dict]:
        query = """
            SELECT exchange_id,
                   account_id,
                   symbol_id,
                   trade_id,
                   order_id,
                   side,
                   qty,
                   price,
                   realized_pnl,
                   fee,
                   (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT AS ts_ms,
                   raw_json
            FROM trades
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND (
                    %(since_ts_ms)s = 0
                    OR ts >= to_timestamp(%(since_ts_ms)s / 1000.0)
                  )
            ORDER BY ts ASC
            LIMIT %(limit)s
        """

        params = {
            "exchange_id": int(exchange_id),
            "account_id": int(account_id),
            "since_ts_ms": int(since_ts_ms or 0),
            "limit": int(limit or 200_000),
        }

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [c.name for c in cur.description]

        return [dict(zip(cols, r)) for r in rows]

    # ======================================================================
    # RAW SQL
    # ======================================================================


    # ------------------------------------------------------------------
    # Generic helpers (used by traders/screeners when нужен произвольный SQL)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Dict helpers (convenience)
    # ------------------------------------------------------------------
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

    # ======================================================================
    # ACTIVE SYMBOLS
    # ======================================================================

    def list_active_symbols(self, *, exchange_id: int, active_ttl_sec: int = 1800) -> List[str]:
        """
        Active symbols = symbols with OPEN positions OR NEW orders recently updated.
        """
        exchange_id = int(exchange_id)
        ttl = int(active_ttl_sec)

        sql = """
            WITH active_symbol_ids AS (
                SELECT p.symbol_id
                FROM positions p
                WHERE p.exchange_id = %s
                  AND abs(p.qty) > 0
                  AND p.status = 'OPEN'
                  AND p.closed_at IS NULL

                UNION ALL

                SELECT o.symbol_id
                FROM orders o
                WHERE o.exchange_id = %s
                  AND o.status = 'NEW'
                  AND o.updated_at >= (now() - (%s || ' seconds')::interval)
            )
            SELECT DISTINCT s.symbol
            FROM symbols s
            JOIN active_symbol_ids a ON a.symbol_id = s.symbol_id
            WHERE s.exchange_id = %s
            ORDER BY s.symbol;
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, exchange_id, ttl, exchange_id))
                return [str(r[0]) for r in (cur.fetchall() or []) if r and r[0]]

    def get_symbol_id(self, *, exchange_id: int, symbol: str) -> int | None:
        symbol = str(symbol).upper().strip()
        if not symbol:
            return None

        sql = "SELECT symbol_id FROM symbols WHERE exchange_id=%s AND symbol=%s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), symbol))
                row = cur.fetchone()
        return int(row[0]) if row else None

    def fetch_symbols_map(self, *, exchange_id: int, only_active: bool = True) -> dict[str, int]:
        where = "WHERE exchange_id=%s"
        params = [int(exchange_id)]
        if only_active:
            where += " AND is_active = TRUE"

        sql = f"""
            SELECT symbol, symbol_id
            FROM symbols
            {where}
            ORDER BY symbol
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

    def deactivate_missing_symbols(self, *, exchange_id: int, active_symbols: Iterable[str]) -> int:
        active = [str(s).upper() for s in (active_symbols or [])]
        if not active:
            return 0

        sql = """
            UPDATE symbols
            SET is_active = FALSE
            WHERE exchange_id = %s
              AND is_active = TRUE
              AND NOT (symbol = ANY(%s))
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), active))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    # ======================================================================
    # FUNDING / OPEN INTEREST / TICKER_24H
    # ======================================================================

    @staticmethod
    def _normalize_funding_time(dt: Any) -> Any:
        """
        Binance funding time дискретен (обычно 00/08/16 UTC).
        Приводим к началу часа, чтобы не плодить "почти одинаковые" ключи.
        """
        if dt is None:
            return None
        if isinstance(dt, datetime):
            # в UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.replace(minute=0, second=0, microsecond=0)
        return dt

    def upsert_funding(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        # ✅ safety-normalize input rows
        prepared: list[dict] = []
        for r in rows:
            d = dict(r or {})
            d["exchange_id"] = int(d.get("exchange_id") or 0)
            d["symbol_id"] = int(d.get("symbol_id") or 0)

            ft = d.get("funding_time")
            ft = self._normalize_funding_time(ft)
            if ft is None:
                continue
            d["funding_time"] = ft

            # числа
            if d.get("funding_rate") is not None:
                try:
                    d["funding_rate"] = float(d["funding_rate"])
                except Exception:
                    d["funding_rate"] = 0.0
            else:
                d["funding_rate"] = 0.0

            if d.get("mark_price") is not None:
                try:
                    d["mark_price"] = float(d["mark_price"])
                except Exception:
                    d["mark_price"] = None
            else:
                d["mark_price"] = None

            d["source"] = str(d.get("source") or "unknown")

            prepared.append(d)

        if not prepared:
            return 0

        query = """
            INSERT INTO funding (
                exchange_id, symbol_id, funding_time,
                funding_rate, mark_price, source
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(funding_time)s,
                %(funding_rate)s, %(mark_price)s, %(source)s
            )
            ON CONFLICT (exchange_id, symbol_id, funding_time)
            DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate,
                mark_price   = COALESCE(EXCLUDED.mark_price, funding.mark_price),
                source       = COALESCE(NULLIF(EXCLUDED.source, ''), funding.source)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, prepared)
                n = cur.rowcount
            conn.commit()

        return int(n if n is not None and n >= 0 else len(prepared))

    def get_next_funding_eta(self, *, exchange_id: int, symbol_id: int) -> dict:
        """
        Возвращает ближайший будущий funding_time и сколько осталось до начисления.
        Если будущего funding_time в таблице нет — рассчитывает по расписанию 00/08/16 UTC.
        """
        exchange_id = int(exchange_id)
        symbol_id = int(symbol_id)
        now = _utcnow()

        # 1) пробуем взять из БД (если WS пишет будущую точку funding_time)
        q = """
            SELECT funding_time, funding_rate, mark_price, source
            FROM funding
            WHERE exchange_id = %s
              AND symbol_id = %s
              AND funding_time > NOW()
            ORDER BY funding_time ASC
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, symbol_id))
                row = cur.fetchone()

        if row:
            next_ft, rate, mp, src = row
            next_ft = self._normalize_funding_time(next_ft)
        else:
            # 2) fallback: Binance USD-M funding обычно каждые 8 часов: 00:00 / 08:00 / 16:00 UTC
            h = now.hour
            next_h = ((h // 8) + 1) * 8
            if next_h >= 24:
                next_ft = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                next_ft = now.replace(hour=next_h, minute=0, second=0, microsecond=0)
            rate, mp, src = None, None, "schedule_fallback"

        eta = next_ft - now
        eta_sec = int(eta.total_seconds())

        def _fmt(sec: int) -> str:
            if sec < 0:
                sec = 0
            m, s = divmod(sec, 60)
            h, m = divmod(m, 60)
            d, h = divmod(h, 24)
            if d > 0:
                return f"{d}d {h:02d}:{m:02d}:{s:02d}"
            return f"{h:02d}:{m:02d}:{s:02d}"

        return {
            "now_utc": now,
            "next_funding_time": next_ft,
            "eta_sec": eta_sec,
            "eta_str": _fmt(eta_sec),
            "funding_rate": (float(rate) if rate is not None else None),
            "mark_price": (float(mp) if mp is not None else None),
            "source": src,
        }


    def fetch_next_funding_bulk(
            self,
            *,
            exchange_id: int,
            symbol_ids: list[int],
            as_of: datetime | None = None,
    ) -> dict[int, dict]:
        """
        Возвращает по каждому symbol_id:
          funding_time (NEXT, строго > as_of; если в БД future нет — вычисляем),
          funding_rate (если future нет — берём последний известный),
          mark_price,
          is_next=True,
          funding_interval_hours (4/8/...), is_estimated (если вычислено).

        out: {sid: {"funding_time": dt, "funding_rate": float|None, "mark_price": float|None,
                    "is_next": bool, "funding_interval_hours": int|None, "is_estimated": bool}}
        """
        symbol_ids = [int(x) for x in (symbol_ids or []) if x is not None]
        if not symbol_ids:
            return {}

        if as_of is None:
            as_of = _utcnow()
        as_of = _utc(as_of)

        def _infer_interval_hours(last_t: Optional[datetime], prev_t: Optional[datetime]) -> int:
            # 1) по разнице двух последних
            if isinstance(last_t, datetime) and isinstance(prev_t, datetime):
                dh = (last_t - prev_t).total_seconds() / 3600.0
                dh_r = int(round(dh))
                if dh_r in (1, 2, 4, 8, 12, 24):
                    return dh_r

            # 2) по “сетке часов” как fallback
            if isinstance(last_t, datetime):
                h = int(last_t.hour)
                if h in (0, 8, 16):
                    return 8
                if h in (0, 4, 8, 12, 16, 20):
                    return 4

            return 8  # дефолт

        def _calc_next_time(last_t: datetime, now_t: datetime, interval_h: int) -> datetime:
            step = timedelta(hours=int(interval_h))
            # хотим строго > now_t, чтобы не было "next in 0s"
            # (и чтобы не зависеть от миллисекунд)
            if last_t > now_t:
                nxt = last_t
            else:
                delta_s = (now_t - last_t).total_seconds()
                step_s = step.total_seconds()
                k = int(delta_s // step_s) + 1
                nxt = last_t + k * step

            # защита: если получилось почти "сейчас" — сдвинем на шаг
            if (nxt - now_t).total_seconds() <= 1.0:
                nxt = nxt + step
            return nxt

        sql = """
              WITH next_row AS (SELECT DISTINCT \
              ON (symbol_id)
                  symbol_id, funding_time, funding_rate, mark_price
              FROM funding
              WHERE exchange_id = %s
                AND symbol_id = ANY (%s)
                AND funding_time \
                  > %s
              ORDER BY symbol_id, funding_time ASC
                  ),
                  hist AS (
              SELECT
                  symbol_id, funding_time, funding_rate, mark_price, LAG(funding_time) OVER (PARTITION BY symbol_id ORDER BY funding_time ASC) AS prev_time
              FROM funding
              WHERE exchange_id = %s
                AND symbol_id = ANY (%s)
                AND funding_time <= %s
                  ) \
                  , last_row AS (
              SELECT DISTINCT \
              ON (symbol_id)
                  symbol_id,
                  funding_time AS last_time,
                  prev_time,
                  funding_rate AS last_rate,
                  mark_price AS last_mark
              FROM hist
              ORDER BY symbol_id, funding_time DESC
                  )
              SELECT COALESCE(n.symbol_id, l.symbol_id) AS symbol_id, \

                     n.funding_time                     AS next_time, \
                     n.funding_rate                     AS next_rate, \
                     n.mark_price                       AS next_mark, \

                     l.last_time, \
                     l.prev_time, \
                     l.last_rate, \
                     l.last_mark
              FROM last_row l
                       FULL OUTER JOIN next_row n USING (symbol_id) \
              """

        out: dict[int, dict] = {}

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (int(exchange_id), symbol_ids, as_of, int(exchange_id), symbol_ids, as_of),
                )
                for row in (cur.fetchall() or []):
                    (
                        sid,
                        next_time, next_rate, next_mark,
                        last_time, prev_time, last_rate, last_mark
                    ) = row

                    if sid is None:
                        continue

                    sid = int(sid)

                    # нормализуем tz
                    next_time = _utc(next_time) if isinstance(next_time, datetime) else None
                    last_time = _utc(last_time) if isinstance(last_time, datetime) else None
                    prev_time = _utc(prev_time) if isinstance(prev_time, datetime) else None

                    if isinstance(next_time, datetime):
                        # future реально есть в БД
                        interval_h = _infer_interval_hours(last_time, prev_time)
                        out[sid] = {
                            "funding_time": next_time,
                            "funding_rate": float(next_rate) if next_rate is not None else None,
                            "mark_price": float(next_mark) if next_mark is not None else None,
                            "is_next": True,
                            "funding_interval_hours": int(interval_h),
                            "is_estimated": False,
                        }
                        continue

                    if isinstance(last_time, datetime):
                        # future нет -> вычисляем NEXT
                        interval_h = _infer_interval_hours(last_time, prev_time)
                        computed_next = _calc_next_time(last_time, as_of, interval_h)

                        out[sid] = {
                            "funding_time": computed_next,
                            "funding_rate": float(last_rate) if last_rate is not None else None,  # последний известный
                            "mark_price": float(last_mark) if last_mark is not None else None,
                            "is_next": True,  # важное: теперь это "next" (пусть вычисленное)
                            "funding_interval_hours": int(interval_h),
                            "is_estimated": True,
                        }
                        continue

                    # вообще нет данных по символу
                    out[sid] = {
                        "funding_time": None,
                        "funding_rate": None,
                        "mark_price": None,
                        "is_next": False,
                        "funding_interval_hours": None,
                        "is_estimated": False,
                    }

        return out

    def upsert_open_interest(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        query = """
            INSERT INTO open_interest (
                exchange_id, symbol_id, interval, ts,
                open_interest, open_interest_value, source
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(interval)s, %(ts)s,
                %(open_interest)s, %(open_interest_value)s, %(source)s
            )
            ON CONFLICT (exchange_id, symbol_id, interval, ts)
            DO UPDATE SET
                open_interest = EXCLUDED.open_interest,
                open_interest_value = EXCLUDED.open_interest_value,
                source = EXCLUDED.source
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

        return len(rows)

    def cleanup_funding(self, *, exchange_id: int, keep_days: int) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        query = """
            DELETE FROM funding
            WHERE exchange_id = %s
              AND funding_time < (NOW() - (%s || ' days')::interval)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), keep_days))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def cleanup_open_interest(self, *, exchange_id: int, interval: str, keep_days: int) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        query = """
            DELETE FROM open_interest
            WHERE exchange_id = %s
              AND interval = %s
              AND ts < (NOW() - (%s || ' days')::interval)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), str(interval), keep_days))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_ticker_24h(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0

        query = """
            INSERT INTO ticker_24h (
                exchange_id, symbol_id,
                open_time, close_time,
                open_price, high_price, low_price, last_price,
                volume, quote_volume,
                price_change, price_change_percent, weighted_avg_price,
                trades, source
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s,
                %(open_time)s, %(close_time)s,
                %(open_price)s, %(high_price)s, %(low_price)s, %(last_price)s,
                %(volume)s, %(quote_volume)s,
                %(price_change)s, %(price_change_percent)s, %(weighted_avg_price)s,
                %(trades)s, %(source)s
            )
            ON CONFLICT (exchange_id, symbol_id, close_time)
            DO UPDATE SET
                open_time = EXCLUDED.open_time,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                last_price = EXCLUDED.last_price,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                price_change = EXCLUDED.price_change,
                price_change_percent = EXCLUDED.price_change_percent,
                weighted_avg_price = EXCLUDED.weighted_avg_price,
                trades = EXCLUDED.trades,
                source = EXCLUDED.source
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
                n = cur.rowcount
            conn.commit()

        return int(n if n is not None and n >= 0 else len(rows))

    def cleanup_ticker_24h(self, exchange_id: int, keep_days: int) -> int:
        """
        Удаляет записи ticker_24h старше keep_days (по close_time).
        """
        exchange_id = int(exchange_id)
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        cutoff = _utcnow() - timedelta(days=keep_days)

        sql = """
            DELETE
            FROM public.ticker_24h
            WHERE exchange_id = %s
              AND close_time < %s
        """

        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (exchange_id, cutoff))
                    deleted = cur.rowcount or 0
                conn.commit()

            logger.info(
                "[Retention] ticker_24h cleaned exchange_id=%s keep_days=%s deleted=%s",
                exchange_id, keep_days, deleted
            )
            return int(deleted)

        except Exception:
            logger.exception("cleanup_ticker_24h failed exchange_id=%s keep_days=%s", exchange_id, keep_days)
            return 0

    # ======================================================================
    # ORDER FILLS (OMS invariant)
    # ======================================================================

    def insert_order_fill(self, fill: Any) -> int:
        return self.insert_order_fills([fill])

    def insert_order_fills(self, fills: Sequence[Any]) -> int:
        """
        INSERT fills into public.order_fills.

        OMS invariants:
          • fill_uid == trade_id (ВСЕГДА)
          • без trade_id → это НЕ fill
          • qty / price обязательны
          • NULL / мусор не пишем
          • idempotent (ON CONFLICT DO NOTHING)
        """
        if not fills:
            return 0

        sql = """
            INSERT INTO order_fills (
                exchange_id,
                account_id,
                fill_uid,
                symbol_id,
                order_id,
                trade_id,
                client_order_id,
                price,
                qty,
                realized_pnl,
                ts,
                source
            )
            VALUES (
                %(exchange_id)s,
                %(account_id)s,
                %(fill_uid)s,
                %(symbol_id)s,
                %(order_id)s,
                %(trade_id)s,
                %(client_order_id)s,
                %(price)s,
                %(qty)s,
                %(realized_pnl)s,
                %(ts)s,
                %(source)s
            )
            ON CONFLICT (exchange_id, account_id, fill_uid)
            DO NOTHING
        """

        now = _utcnow()

        def get(obj: Any, key: str, default=None):
            return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)

        rows: list[dict[str, Any]] = []

        for f in fills:
            ex_id = get(f, "exchange_id")
            acc_id = get(f, "account_id")
            sym_id = get(f, "symbol_id")
            if ex_id is None or acc_id is None or sym_id is None:
                continue

            trade_id = get(f, "trade_id")
            if trade_id is None:
                continue

            fill_uid = str(trade_id)

            price = get(f, "price")
            qty = get(f, "qty")
            if price is None or qty is None:
                continue

            ts = get(f, "ts")
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            elif ts is None:
                ts = now

            rows.append(
                {
                    "exchange_id": int(ex_id),
                    "account_id": int(acc_id),
                    "fill_uid": fill_uid,
                    "symbol_id": int(sym_id),
                    "order_id": str(get(f, "order_id")) if get(f, "order_id") is not None else None,
                    "trade_id": str(trade_id),
                    "client_order_id": (
                        str(get(f, "client_order_id"))
                        if get(f, "client_order_id") is not None
                        else None
                    ),
                    "price": price,
                    "qty": qty,
                    "realized_pnl": get(f, "realized_pnl") or 0,
                    "ts": ts,
                    "source": get(f, "source") or "ws_user",
                }
            )

        if not rows:
            return 0

        return self._exec_many(sql, rows)

    # ======================================================================
    # ORDER EVENT UPSERT (single)
    # ======================================================================

    def upsert_order_event(self, ev: Any) -> None:
        # side normalization
        side = ev.side
        if hasattr(side, "name"):
            side = side.name
        elif isinstance(side, str):
            side = side.upper()
        else:
            side = None

        status = ev.status.name if hasattr(ev.status, "name") else ev.status
        otype = ev.type.name if hasattr(ev.type, "name") else ev.type

        raw = getattr(ev, "raw_json", None) or getattr(ev, "payload", None)
        if raw is None:
            raw = {}

        if isinstance(raw, str):
            raw_json = raw
        else:
            try:
                raw_json = json.dumps(raw, ensure_ascii=False, default=str)
            except Exception:
                raw_json = "{}"

        recv_ts = getattr(ev, "recv_ts", None)
        if isinstance(recv_ts, (int, float)):
            recv_ts = datetime.fromtimestamp(recv_ts, tz=timezone.utc)
        elif recv_ts is None:
            recv_ts = _utcnow()

        if (
            ev.exchange_id is None
            or ev.account_id is None
            or ev.symbol_id is None
            or ev.order_id is None
            or ev.ts_ms is None
        ):
            return

        strategy_id = getattr(ev, "strategy_id", None) or "unknown"
        pos_uid = getattr(ev, "pos_uid", None)

        row = {
            "exchange_id": int(ev.exchange_id),
            "account_id": int(ev.account_id),
            "order_id": str(ev.order_id),
            "symbol_id": int(ev.symbol_id),

            "client_order_id": getattr(ev, "client_order_id", None),
            "status": str(status or ""),
            "side": side,
            "type": otype,
            "reduce_only": bool(getattr(ev, "reduce_only", False)),
            "price": getattr(ev, "price", None),
            "qty": getattr(ev, "qty", None),
            "filled_qty": getattr(ev, "filled_qty", None),

            "strategy_id": str(strategy_id),
            "pos_uid": str(pos_uid) if pos_uid else None,

            "source": getattr(ev, "source", None) or "ws_user",
            "ts_ms": int(ev.ts_ms),

            "recv_ts": recv_ts,
            "raw_json": raw_json,
        }

        query = """
            INSERT INTO order_events (
                exchange_id, account_id, order_id, symbol_id,
                client_order_id, status, side, type,
                reduce_only, price, qty, filled_qty,
                strategy_id, pos_uid,
                source, ts_ms, recv_ts, raw_json
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s,
                %(client_order_id)s, %(status)s, %(side)s, %(type)s,
                %(reduce_only)s, %(price)s, %(qty)s, %(filled_qty)s,
                %(strategy_id)s, %(pos_uid)s,
                %(source)s, %(ts_ms)s, %(recv_ts)s, %(raw_json)s
            )
            ON CONFLICT (exchange_id, account_id, order_id, ts_ms, status, filled_qty)
            DO NOTHING
        """
        self._exec_many(query, [row])

    def exists_order_event(self, *, exchange_id: int, account_id: int, order_id: str, status: str) -> bool:
        sql = """
            SELECT 1
            FROM order_events
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
              AND status = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), int(account_id), str(order_id), str(status)))
                return cur.fetchone() is not None

    def get_order_by_id(self, *, exchange_id: int, account_id: int, order_id: str) -> dict | None:
        query = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()
                if row:
                    cols = [d[0] for d in cur.description]
                    return dict(zip(cols, row))
        return None

    # ======================================================================
    # LIQUIDATIONS
    # ======================================================================

    def insert_liquidation_events(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        sql = """
            INSERT INTO liquidation_events (
                exchange_id, symbol_id, ts, event_ms,
                side, price, qty, filled_qty, avg_price,
                status, order_type, time_in_force,
                notional, is_long_liq, raw_json
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(ts)s, %(event_ms)s,
                %(side)s, %(price)s, %(qty)s, %(filled_qty)s, %(avg_price)s,
                %(status)s, %(order_type)s, %(time_in_force)s,
                %(notional)s, %(is_long_liq)s, %(raw_json)s
            )
            ON CONFLICT DO NOTHING
        """

        fixed = []
        for r in rows:
            rr = dict(r)
            rr["raw_json"] = Jsonb(rr.get("raw_json"))
            fixed.append(rr)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, fixed)
            conn.commit()
        return len(fixed)

    def upsert_liquidation_1m(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        sql = """
            INSERT INTO liquidation_1m (
                exchange_id, symbol_id, bucket_ts,
                long_notional, short_notional, long_qty, short_qty, events, updated_at
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(bucket_ts)s,
                %(long_notional)s, %(short_notional)s, %(long_qty)s, %(short_qty)s, %(events)s, %(updated_at)s
            )
            ON CONFLICT (exchange_id, symbol_id, bucket_ts)
            DO UPDATE SET
                long_notional = liquidation_1m.long_notional + EXCLUDED.long_notional,
                short_notional = liquidation_1m.short_notional + EXCLUDED.short_notional,
                long_qty = liquidation_1m.long_qty + EXCLUDED.long_qty,
                short_qty = liquidation_1m.short_qty + EXCLUDED.short_qty,
                events = liquidation_1m.events + EXCLUDED.events,
                updated_at = EXCLUDED.updated_at
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    # ======================================================================
    # MARKET STATE RETENTION
    # ======================================================================

    def cleanup_market_state_5m(self, *, exchange_id: int, keep_days: int) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        sql = """
            DELETE
            FROM market_state_5m
            WHERE exchange_id = %(exchange_id)s
              AND open_time < now() - (%(keep_days)s || ' days')::interval
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"exchange_id": int(exchange_id), "keep_days": keep_days})
                deleted = cur.rowcount or 0
            conn.commit()

        return int(deleted)

    def get_open_interest_watermarks_bulk(self, exchange_id: int, intervals: list[str]) -> dict[str, dict[int, datetime]]:
        """
        out: { "5m": {symbol_id: ts, ...}, ... }
        """
        exchange_id = int(exchange_id)
        intervals = list(intervals or [])
        if not intervals:
            return {}

        sql = """
            SELECT interval, symbol_id, MAX(ts) AS last_ts
            FROM open_interest
            WHERE exchange_id = %s
              AND interval = ANY (%s)
            GROUP BY interval, symbol_id
        """

        out: dict[str, dict[int, datetime]] = {iv: {} for iv in intervals}

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, intervals))
                for interval, symbol_id, last_ts in cur.fetchall():
                    if last_ts is None:
                        continue
                    out[str(interval)][int(symbol_id)] = last_ts

        return out

    # ======================================================================
    # SYMBOL HELPERS (for collectors)
    # ======================================================================

    def list_active_symbol_ids(self, exchange_id: int) -> list[int]:
        sql = """
            SELECT symbol_id
            FROM symbols
            WHERE exchange_id = %s AND is_active = true
            ORDER BY symbol_id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id),))
                rows = cur.fetchall() or []
        return [int(r[0]) for r in rows]

    def list_active_symbols_map(self, exchange_id: int) -> dict[str, int]:
        sql = """
            SELECT symbol, symbol_id
            FROM symbols
            WHERE exchange_id = %s AND is_active = true
            ORDER BY symbol
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id),))
                rows = cur.fetchall() or []
        return {str(sym).upper(): int(sid) for sym, sid in rows}

    def get_symbol_name(self, symbol_id: int) -> str | None:
        sql = "SELECT symbol FROM symbols WHERE symbol_id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(symbol_id),))
                row = cur.fetchone()
        if not row:
            return None
        return str(row[0]).upper()

    # ======================================================================
    # CLEANUP BATCHED TABLES
    # ======================================================================

    def cleanup_market_trades(
        self,
        *,
        exchange_id: int,
        keep_days: int,
        batch_size: int = 50_000,
        sleep_sec: float = 0.05,
        max_batches: int = 0,
    ) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        batch_size = max(1000, int(batch_size))
        sleep_sec = max(0.0, float(sleep_sec))
        max_batches = int(max_batches or 0)

        total_deleted = 0
        batches = 0

        sql = """
            WITH del AS (
                SELECT ctid
                FROM market_trades
                WHERE exchange_id = %(exchange_id)s
                  AND ts < now() - (%(keep_days)s || ' days')::interval
                LIMIT %(batch_size)s
            )
            DELETE FROM market_trades
            WHERE ctid IN (SELECT ctid FROM del)
        """

        while True:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        {
                            "exchange_id": int(exchange_id),
                            "keep_days": keep_days,
                            "batch_size": batch_size,
                        },
                    )
                    deleted = int(cur.rowcount or 0)
                conn.commit()

            if deleted <= 0:
                break

            total_deleted += deleted
            batches += 1

            if max_batches > 0 and batches >= max_batches:
                break

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        return total_deleted

    def cleanup_candles_trades_agg(
        self,
        *,
        exchange_id: int,
        keep_days: int,
        batch_size: int = 50_000,
        sleep_sec: float = 0.05,
        max_batches: int = 0,
    ) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        batch_size = max(1000, int(batch_size))
        sleep_sec = max(0.0, float(sleep_sec))
        max_batches = int(max_batches or 0)

        total_deleted = 0
        batches = 0

        sql = """
            WITH del AS (
                SELECT ctid
                FROM candles_trades_agg
                WHERE exchange_id = %(exchange_id)s
                  AND open_time < now() - (%(keep_days)s || ' days')::interval
                LIMIT %(batch_size)s
            )
            DELETE FROM candles_trades_agg
            WHERE ctid IN (SELECT ctid FROM del)
        """

        while True:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        {
                            "exchange_id": int(exchange_id),
                            "keep_days": keep_days,
                            "batch_size": batch_size,
                        },
                    )
                    deleted = int(cur.rowcount or 0)
                conn.commit()

            if deleted <= 0:
                break

            total_deleted += deleted
            batches += 1

            if max_batches > 0 and batches >= max_batches:
                break

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        return total_deleted

    def fetch_candles_window(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        center_ts: datetime,
        lookback: int,
        lookforward: int,
    ) -> list[dict]:
        delta = _interval_to_timedelta(interval)
        start_ts = center_ts - (delta * int(lookback))
        end_ts = center_ts + (delta * int(lookforward))

        q = """
            SELECT open_time AS ts, open, high, low, close, volume, quote_volume
            FROM candles
            WHERE exchange_id = %s
              AND symbol_id = %s
              AND interval = %s
              AND open_time >= %s
              AND open_time <= %s
            ORDER BY open_time
        """
        with self.pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), start_ts, end_ts))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in (cur.fetchall() or [])]

    def fetch_open_interest_window(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        center_ts: datetime,
        lookback: int,
        lookforward: int,
    ) -> list[dict]:
        delta = _interval_to_timedelta(interval)
        start_ts = center_ts - (delta * int(lookback))
        end_ts = center_ts + (delta * int(lookforward))

        q = """
            SELECT ts, open_interest
            FROM open_interest
            WHERE exchange_id = %s
              AND symbol_id = %s
              AND interval = %s
              AND ts >= %s
              AND ts <= %s
            ORDER BY ts
        """
        with self.pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), start_ts, end_ts))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in (cur.fetchall() or [])]

    def fetch_cvd_window(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        center_ts: datetime,
        lookback: int,
        lookforward: int,
    ) -> list[dict]:
        delta = _interval_to_timedelta(interval)
        start_ts = center_ts - (delta * int(lookback))
        end_ts = center_ts + (delta * int(lookforward))

        q = """
            SELECT open_time AS ts, cvd_quote
            FROM candles
            WHERE exchange_id = %s
              AND symbol_id = %s
              AND interval = %s
              AND open_time >= %s
              AND open_time <= %s
            ORDER BY open_time
        """
        with self.pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), start_ts, end_ts))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in (cur.fetchall() or [])]

    def fetch_funding_window(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
        center_ts: datetime,
        lookback: int,
        lookforward: int,
        interval: str = "1h",
    ) -> list[dict]:
        delta = _interval_to_timedelta(interval)
        start_ts = center_ts - (delta * int(lookback))
        end_ts = center_ts + (delta * int(lookforward))

        q = """
            SELECT funding_time AS ts, funding_rate
            FROM funding
            WHERE exchange_id = %s
              AND symbol_id = %s
              AND funding_time >= %s
              AND funding_time <= %s
            ORDER BY funding_time
        """
        with self.pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), start_ts, end_ts))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in (cur.fetchall() or [])]
