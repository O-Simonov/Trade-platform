# src/platform/exchanges/binance/collector_market_trades.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from psycopg.types.json import Json

log = logging.getLogger("collector.market_trades")


def _ms_to_dt(ms: int) -> datetime:
    # Binance ms -> aware datetime (timestamptz)
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


@dataclass
class MarketTradesCollector:
    pool: Any
    rest: Any  # BinanceFuturesREST
    exchange_id: int
    symbol_id: int

    # эти параметры у тебя приходят из config/market_data.yaml
    interval: str = "5m"
    poll_sleep: float = 1.0
    overlap_minutes: int = 120  # ✅ ВАЖНО: чтобы run_market_data.py не падал

    # при отставании > lag_tail_sec делаем tail-sync (подхват последних трейдов)
    lag_tail_sec: int = 120

    def _get_symbol(self) -> str:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol FROM symbols WHERE exchange_id=%s AND symbol_id=%s",
                    (int(self.exchange_id), int(self.symbol_id)),
                )
                row = cur.fetchone()
        if not row:
            raise RuntimeError(f"symbol not found exchange_id={self.exchange_id} symbol_id={self.symbol_id}")
        return str(row[0]).upper()

    def _get_last_trade_id(self) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(trade_id), 0)
                    FROM market_trades
                    WHERE exchange_id=%s AND symbol_id=%s
                    """,
                    (int(self.exchange_id), int(self.symbol_id)),
                )
                v = cur.fetchone()
        return int(v[0] if v else 0)

    def _get_last_ts_and_lag(self) -> tuple[Optional[datetime], Optional[float]]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(ts)
                    FROM market_trades
                    WHERE exchange_id=%s AND symbol_id=%s
                    """,
                    (int(self.exchange_id), int(self.symbol_id)),
                )
                row = cur.fetchone()

        max_ts = row[0] if row else None
        if max_ts is None:
            return None, None

        now_utc = datetime.now(timezone.utc)
        lag = (now_utc - max_ts).total_seconds()
        return max_ts, lag

    def _exec_many(self, sql: str, params_list: list[tuple[Any, ...]]) -> int:
        if not params_list:
            return 0
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params_list)
            conn.commit()
        return len(params_list)

    def _normalize_agg_trade(self, item: dict[str, Any]) -> dict[str, Any]:
        """
        Binance /fapi/v1/aggTrades:
          a: aggTradeId
          p: price
          q: qty
          T: timestamp ms
          m: isBuyerMaker (true => агрессор SELL)
        """
        trade_id = item.get("a", item.get("aggTradeId"))
        price = item.get("p", item.get("price"))
        qty = item.get("q", item.get("qty"))
        ts_ms = item.get("T", item.get("timestamp"))

        if trade_id is None or ts_ms is None:
            raise ValueError(f"Bad aggTrade payload: {item!r}")

        is_buyer_maker = item.get("m", item.get("isBuyerMaker"))
        taker_side = "SELL" if bool(is_buyer_maker) else "BUY"

        price_f = float(price)
        qty_f = float(qty)
        quote_qty = price_f * qty_f

        return {
            "exchange_id": int(self.exchange_id),
            "symbol_id": int(self.symbol_id),
            "trade_id": int(trade_id),
            "ts": _ms_to_dt(int(ts_ms)),  # ✅ timestamptz
            "price": price_f,
            "qty": qty_f,
            "quote_qty": quote_qty,
            "taker_side": taker_side,
            "source": "aggTrades",
            "raw_json": item,
        }

    def _insert_trades(self, rows: list[dict[str, Any]]) -> int:
        sql = """
        INSERT INTO market_trades (
          exchange_id, symbol_id, trade_id,
          ts, price, qty, quote_qty,
          taker_side, source, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (exchange_id, symbol_id, trade_id) DO NOTHING
        """

        params: list[tuple[Any, ...]] = []
        for r in rows:
            raw = r.get("raw_json")
            if isinstance(raw, (dict, list)):
                raw = Json(raw)
            elif raw is not None and not isinstance(raw, (str, bytes)):
                raw = Json({"raw": str(raw)})

            params.append(
                (
                    r["exchange_id"],
                    r["symbol_id"],
                    r["trade_id"],
                    r["ts"],
                    r["price"],
                    r["qty"],
                    r["quote_qty"],
                    r["taker_side"],
                    r.get("source"),
                    raw,
                )
            )

        return self._exec_many(sql, params)

    def _upsert_agg_5m(self, ts_from: datetime, ts_to: datetime) -> int:
        """
        Пересчёт candles_trades_agg только на свежем диапазоне.
        """
        sql = """
        WITH agg AS (
          SELECT
            exchange_id,
            symbol_id,
            '5m'::text AS interval,
            date_trunc('hour', ts)
              + (floor(extract(minute from ts)::numeric / 5) * interval '5 minute') AS open_time,
            SUM(quote_qty) AS quote_volume,
            COUNT(*) AS trades,
            SUM(CASE WHEN taker_side='BUY'  THEN quote_qty ELSE 0 END) AS taker_buy_quote,
            SUM(CASE WHEN taker_side='SELL' THEN quote_qty ELSE 0 END) AS taker_sell_quote,
            SUM(CASE WHEN taker_side='BUY'  THEN quote_qty ELSE -quote_qty END) AS delta_quote
          FROM market_trades
          WHERE exchange_id=%s
            AND symbol_id=%s
            AND ts >= %s::timestamptz
            AND ts <  %s::timestamptz
          GROUP BY 1,2,3,4
        ),
        up AS (
          INSERT INTO candles_trades_agg(
            exchange_id, symbol_id, interval, open_time,
            quote_volume, trades, taker_buy_quote, taker_sell_quote,
            delta_quote, cvd_quote, updated_at
          )
          SELECT
            a.exchange_id, a.symbol_id, a.interval, a.open_time,
            a.quote_volume, a.trades, a.taker_buy_quote, a.taker_sell_quote,
            a.delta_quote,
            0::double precision AS cvd_quote,
            now()
          FROM agg a
          ON CONFLICT (exchange_id, symbol_id, interval, open_time) DO UPDATE SET
            quote_volume     = EXCLUDED.quote_volume,
            trades           = EXCLUDED.trades,
            taker_buy_quote  = EXCLUDED.taker_buy_quote,
            taker_sell_quote = EXCLUDED.taker_sell_quote,
            delta_quote      = EXCLUDED.delta_quote,
            updated_at       = now()
          RETURNING open_time
        )
        SELECT COUNT(*) FROM up;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        int(self.exchange_id),
                        int(self.symbol_id),
                        ts_from,
                        ts_to,
                    ),
                )
                n = int(cur.fetchone()[0])
            conn.commit()
        return n

    def _recalc_cvd_5m(self, hours_back: int = 72) -> int:
        sql = """
        WITH w AS (
          SELECT
            exchange_id, symbol_id, interval, open_time,
            SUM(delta_quote) OVER (
              PARTITION BY exchange_id, symbol_id, interval
              ORDER BY open_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cvd
          FROM candles_trades_agg
          WHERE exchange_id=%s AND symbol_id=%s AND interval='5m'
            AND open_time >= now() - (%s || ' hours')::interval
        )
        UPDATE candles_trades_agg t
        SET cvd_quote = w.cvd,
            updated_at = now()
        FROM w
        WHERE t.exchange_id=w.exchange_id
          AND t.symbol_id=w.symbol_id
          AND t.interval=w.interval
          AND t.open_time=w.open_time;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(self.exchange_id), int(self.symbol_id), int(hours_back)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def _tail_sync_if_needed(self, symbol: str) -> Optional[int]:
        """
        Если отстали — забираем последнюю пачку 1000 aggTrades и прыгаем на хвост.
        """
        max_ts, lag = self._get_last_ts_and_lag()
        if max_ts is None:
            log.warning("[MarketTrades] empty table -> tail sync start")
        else:
            log.info("[MarketTrades] last_ts=%s lag_sec=%.1f", max_ts, float(lag or 0.0))

        if max_ts is not None and lag is not None and lag <= float(self.lag_tail_sec):
            return None

        try:
            batch = self.rest.get_agg_trades(symbol=symbol, from_id=None, limit=1000)
        except Exception:
            log.exception("[MarketTrades] tail_sync REST error")
            return None

        if not batch:
            log.warning("[MarketTrades] tail_sync empty batch")
            return None

        normed: list[dict[str, Any]] = []
        max_id = None
        min_ts = None
        max_ts2 = None

        for item in batch:
            if not isinstance(item, dict):
                continue
            r = self._normalize_agg_trade(item)
            normed.append(r)

            tid = int(r["trade_id"])
            ts = r["ts"]

            if max_id is None or tid > max_id:
                max_id = tid
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts2 is None or ts > max_ts2:
                max_ts2 = ts

        inserted = self._insert_trades(normed)
        log.info("[MarketTrades] tail_sync fetched=%d inserted=%d max_id=%s", len(batch), inserted, max_id)

        if inserted > 0 and min_ts and max_ts2:
            buf = timedelta(minutes=max(10, int(self.overlap_minutes)))
            self._upsert_agg_5m(min_ts - buf, max_ts2 + buf)
            self._recalc_cvd_5m(hours_back=72)

        return int(max_id) + 1 if max_id is not None else None

    def run_forever(self, stop_event: Optional[Any] = None) -> None:
        symbol = self._get_symbol()

        last_trade_id = self._get_last_trade_id()
        from_id = int(last_trade_id) + 1 if last_trade_id > 0 else None

        log.info(
            "[MarketTrades] start exchange_id=%s symbol_id=%s symbol=%s last_trade_id=%s from_id=%s interval=%s overlap_minutes=%s",
            self.exchange_id,
            self.symbol_id,
            symbol,
            last_trade_id,
            from_id,
            self.interval,
            self.overlap_minutes,
        )

        # если отстали — прыгаем на хвост
        tail_from_id = self._tail_sync_if_needed(symbol)
        if tail_from_id is not None:
            from_id = tail_from_id
            log.info("[MarketTrades] tail_sync applied -> new from_id=%s", from_id)

        while True:
            if stop_event is not None and getattr(stop_event, "is_set", lambda: False)():
                log.warning("[MarketTrades] stop_event set -> exit")
                return

            try:
                batch = self.rest.get_agg_trades(symbol=symbol, from_id=from_id, limit=1000)

                if not batch:
                    time.sleep(float(self.poll_sleep))
                    continue

                normed: list[dict[str, Any]] = []
                max_id = None
                min_ts = None
                max_ts2 = None

                for item in batch:
                    if not isinstance(item, dict):
                        continue
                    r = self._normalize_agg_trade(item)
                    normed.append(r)

                    tid = int(r["trade_id"])
                    ts = r["ts"]

                    if max_id is None or tid > max_id:
                        max_id = tid
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                    if max_ts2 is None or ts > max_ts2:
                        max_ts2 = ts

                if normed:
                    inserted = self._insert_trades(normed)
                    log.info("[MarketTrades] fetched=%d inserted=%d last_id=%s", len(batch), inserted, max_id)

                    if inserted > 0 and min_ts and max_ts2:
                        buf = timedelta(minutes=max(10, int(self.overlap_minutes)))
                        self._upsert_agg_5m(min_ts - buf, max_ts2 + buf)
                        self._recalc_cvd_5m(hours_back=72)

                if max_id is not None:
                    from_id = int(max_id) + 1
                else:
                    time.sleep(float(self.poll_sleep))

            except Exception:
                log.exception("[MarketTrades] error")
                time.sleep(1.0)
