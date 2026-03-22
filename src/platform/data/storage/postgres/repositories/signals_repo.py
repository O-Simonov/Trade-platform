from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Sequence

from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")


class SignalsRepositoryMixin:
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

        Почему не INSERT ... ON CONFLICT:
        even on conflict Postgres may advance the sequence before checking uniqueness,
        so frequent calls can exhaust a SMALLINT/old sequence.
        """
        self._ensure_screeners_schema()

        name = str(name or "").strip()
        version = str(version or "").strip()

        if not name:
            raise ValueError("ensure_screener(): name is empty")

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # 1) сначала пытаемся найти существующий screener
                cur.execute(
                    """
                    SELECT screener_id, version, description, is_enabled
                    FROM public.screeners
                    WHERE name = %s LIMIT 1
                    """,
                    (name,),
                )
                row = cur.fetchone()

                if row:
                    screener_id, old_version, old_description, old_is_enabled = row

                    need_update = (
                            str(old_version or "") != version
                            or (description is not None and str(old_description or "") != str(description))
                            or (old_is_enabled is not True)
                    )

                    if need_update:
                        cur.execute(
                            """
                            UPDATE public.screeners
                            SET version     = %s,
                                description = COALESCE(%s, description),
                                is_enabled  = TRUE,
                                updated_at  = NOW()
                            WHERE screener_id = %s
                            """,
                            (version, description, int(screener_id)),
                        )

                    conn.commit()
                    return int(screener_id)

                # 2) если нет — только тогда INSERT
                cur.execute(
                    """
                    INSERT INTO public.screeners
                        (name, version, description, is_enabled, created_at, updated_at)
                    VALUES (%s, %s, %s, TRUE, NOW(), NOW()) RETURNING screener_id
                    """,
                    (name, version, description),
                )
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

