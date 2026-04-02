from __future__ import annotations

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Sequence, Mapping, List, Tuple, Dict, Optional

from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

from .base import *
from .base import _utcnow

class MarketDataRepositoryMixin:
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

