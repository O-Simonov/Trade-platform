# src/platform/backtester/data_feed.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .core import Candle, DictSignalProvider, Side, SignalEvent


@dataclass(slots=True)
class PostgresConfig:
    dsn: str
    echo: bool = False


@dataclass(slots=True)
class BacktestRange:
    start: datetime
    end: datetime


class PostgresDataFeed:
    """
    Data feed for backtesting from the current Postgres schema.

    Real schema used:
      - candles
      - signals
      - screeners
      - symbols

    Notes:
      - signals are read only from `signals`
      - screener name is joined from `screeners`
      - candles symbol is joined from `symbols`
      - signal side in DB is BUY/SELL and is mapped to LONG/SHORT
    """

    def __init__(self, config: PostgresConfig):
        self.config = config
        self.engine: Engine = create_engine(config.dsn, echo=config.echo, future=True)

    def load_candles(
        self,
        *,
        time_range: BacktestRange,
        symbols: Optional[Sequence[str]] = None,
        interval: str = "1m",
        exchange_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Candle]:
        query = self._build_candles_query(
            symbols=symbols,
            interval=interval,
            exchange_id=exchange_id,
            limit=limit,
        )
        params = {
            "start_ts": time_range.start,
            "end_ts": time_range.end,
            "symbols": list(symbols) if symbols else None,
            "interval_value": interval,
            "exchange_id": exchange_id,
            "limit_rows": limit,
        }

        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).mappings().all()

        return [self._map_candle_row(row) for row in rows]

    def load_signals(
        self,
        *,
        time_range: BacktestRange,
        symbols: Optional[Sequence[str]] = None,
        strategy_ids: Optional[Sequence[str]] = None,
        exchange_id: Optional[int] = None,
        statuses: Optional[Sequence[str]] = None,
        timeframes: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> List[SignalEvent]:
        query = self._build_signals_query(
            symbols=symbols,
            strategy_ids=strategy_ids,
            exchange_id=exchange_id,
            statuses=statuses,
            timeframes=timeframes,
            limit=limit,
        )
        params = {
            "start_ts": time_range.start,
            "end_ts": time_range.end,
            "symbols": list(symbols) if symbols else None,
            "strategy_ids": list(strategy_ids) if strategy_ids else None,
            "exchange_id": exchange_id,
            "statuses": list(statuses) if statuses else None,
            "timeframes": list(timeframes) if timeframes else None,
            "limit_rows": limit,
        }

        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).mappings().all()

        return [self._map_signal_row(row) for row in rows]

    def build_signal_provider(
        self,
        *,
        time_range: BacktestRange,
        symbols: Optional[Sequence[str]] = None,
        strategy_ids: Optional[Sequence[str]] = None,
        exchange_id: Optional[int] = None,
        statuses: Optional[Sequence[str]] = None,
        timeframes: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> DictSignalProvider:
        events = self.load_signals(
            time_range=time_range,
            symbols=symbols,
            strategy_ids=strategy_ids,
            exchange_id=exchange_id,
            statuses=statuses,
            timeframes=timeframes,
            limit=limit,
        )
        return DictSignalProvider(events)

    def list_columns(self, table_name: str) -> List[str]:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table_name
        ORDER BY ordinal_position
        """
        with self.engine.begin() as conn:
            rows = conn.execute(text(sql), {"table_name": table_name}).scalars().all()
        return list(rows)

    def _map_candle_row(self, row) -> Candle:
        return Candle(
            ts=row["ts"],
            symbol=row["symbol"],
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row.get("volume") or 0.0),
            extras={
                "symbol_id": row.get("symbol_id"),
                "timeframe": row.get("timeframe"),
                "exchange_id": row.get("exchange_id"),
                "source": row.get("source"),
                "quote_volume": float(row["quote_volume"]) if row.get("quote_volume") is not None else None,
                "trades": int(row["trades"]) if row.get("trades") is not None else None,
                "taker_buy_base": float(row["taker_buy_base"]) if row.get("taker_buy_base") is not None else None,
                "taker_buy_quote": float(row["taker_buy_quote"]) if row.get("taker_buy_quote") is not None else None,
                "taker_sell_base": float(row["taker_sell_base"]) if row.get("taker_sell_base") is not None else None,
                "taker_sell_quote": float(row["taker_sell_quote"]) if row.get("taker_sell_quote") is not None else None,
                "delta_quote": float(row["delta_quote"]) if row.get("delta_quote") is not None else None,
                "delta_base": float(row["delta_base"]) if row.get("delta_base") is not None else None,
                "cvd_quote": float(row["cvd_quote"]) if row.get("cvd_quote") is not None else None,
            },
        )

    def _map_signal_row(self, row) -> SignalEvent:
        ts = row["ts"]

        raw_side = str(row.get("side") or "BUY").upper()
        if raw_side == "BUY":
            side = Side.LONG
        elif raw_side == "SELL":
            side = Side.SHORT
        else:
            raise ValueError(f"Unsupported signal side: {raw_side}")

        strategy_id = str(row.get("screener_name") or row.get("screener_id") or "unknown")

        payload = {
            "entry_price": float(row["entry_price"]) if row.get("entry_price") is not None else None,
            "stop_loss": float(row["stop_loss"]) if row.get("stop_loss") is not None else None,
            "take_profit": float(row["take_profit"]) if row.get("take_profit") is not None else None,
            "timeframe": row.get("timeframe"),
            "confidence": float(row["confidence"]) if row.get("confidence") is not None else None,
            "score": float(row["score"]) if row.get("score") is not None else None,
            "reason": row.get("reason"),
            "source": row.get("source"),
            "context": row.get("context"),
            "signal_id": row.get("id"),
            "screener_id": row.get("screener_id"),
            "screener_name": row.get("screener_name"),
            "status": row.get("status"),
            "exchange_id": row.get("exchange_id"),
            "symbol_id": row.get("symbol_id"),
            "raw": dict(row),
        }

        return SignalEvent(
            ts=ts,
            symbol=row["symbol"],
            strategy_id=strategy_id,
            side=side,
            payload=payload,
        )

    def _build_candles_query(
        self,
        *,
        symbols: Optional[Sequence[str]],
        interval: str,
        exchange_id: Optional[int],
        limit: Optional[int],
    ) -> str:
        sql = """
        SELECT
            c.open_time AS ts,
            s.symbol AS symbol,
            c.exchange_id,
            c.symbol_id,
            c.interval AS timeframe,
            c.open,
            c.high,
            c.low,
            c.close,
            c.volume,
            c.source,
            c.quote_volume,
            c.trades,
            c.taker_buy_base,
            c.taker_buy_quote,
            c.taker_sell_base,
            c.taker_sell_quote,
            c.delta_quote,
            c.delta_base,
            c.cvd_quote
        FROM candles c
        JOIN symbols s ON s.symbol_id = c.symbol_id
        WHERE c.open_time >= :start_ts
          AND c.open_time <= :end_ts
          AND c.interval = :interval_value
        """

        if exchange_id is not None:
            sql += "\n  AND c.exchange_id = :exchange_id"

        if symbols:
            sql += "\n  AND s.symbol = ANY(:symbols)"

        sql += "\nORDER BY c.open_time ASC, s.symbol ASC"

        if limit is not None:
            sql += "\nLIMIT :limit_rows"

        return sql

    def _build_signals_query(
        self,
        *,
        symbols: Optional[Sequence[str]],
        strategy_ids: Optional[Sequence[str]],
        exchange_id: Optional[int],
        statuses: Optional[Sequence[str]],
        timeframes: Optional[Sequence[str]],
        limit: Optional[int],
    ) -> str:
        sql = """
        SELECT
            sg.id,
            sg.exchange_id,
            sg.symbol_id,
            sg.screener_id,
            sc.name AS screener_name,
            sg.timeframe,
            sg.signal_ts AS ts,
            sg.signal_day,
            sg.day_seq,
            sg.side,
            sg.status,
            sg.entry_price,
            sg.exit_price,
            sg.stop_loss,
            sg.take_profit,
            sg.confidence,
            sg.score,
            sg.reason,
            sg.context,
            sg.source,
            sg.created_at,
            sg.updated_at,
            sg.symbol
        FROM signals sg
        JOIN screeners sc ON sc.screener_id = sg.screener_id
        WHERE sg.signal_ts >= :start_ts
          AND sg.signal_ts <= :end_ts
        """

        if exchange_id is not None:
            sql += "\n  AND sg.exchange_id = :exchange_id"

        if statuses:
            sql += "\n  AND sg.status::text = ANY(:statuses)"

        if timeframes:
            sql += "\n  AND sg.timeframe = ANY(:timeframes)"

        if symbols:
            sql += "\n  AND sg.symbol = ANY(:symbols)"

        if strategy_ids:
            sql += "\n  AND (sc.name = ANY(:strategy_ids) OR CAST(sg.screener_id AS text) = ANY(:strategy_ids))"

        sql += "\nORDER BY sg.signal_ts ASC, sg.symbol ASC"

        if limit is not None:
            sql += "\nLIMIT :limit_rows"

        return sql


def build_postgres_signal_provider(
    *,
    dsn: str,
    start: datetime,
    end: datetime,
    symbols: Optional[Sequence[str]] = None,
    strategy_ids: Optional[Sequence[str]] = None,
    exchange_id: Optional[int] = None,
    statuses: Optional[Sequence[str]] = None,
    timeframes: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> DictSignalProvider:
    feed = PostgresDataFeed(PostgresConfig(dsn=dsn))
    return feed.build_signal_provider(
        time_range=BacktestRange(start=start, end=end),
        symbols=symbols,
        strategy_ids=strategy_ids,
        exchange_id=exchange_id,
        statuses=statuses,
        timeframes=timeframes,
        limit=limit,
    )


def load_postgres_candles(
    *,
    dsn: str,
    start: datetime,
    end: datetime,
    symbols: Optional[Sequence[str]] = None,
    interval: str = "1m",
    exchange_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Candle]:
    feed = PostgresDataFeed(PostgresConfig(dsn=dsn))
    return feed.load_candles(
        time_range=BacktestRange(start=start, end=end),
        symbols=symbols,
        interval=interval,
        exchange_id=exchange_id,
        limit=limit,
    )