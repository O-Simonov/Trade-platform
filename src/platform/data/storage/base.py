# src/platform/data/storage/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Any


class Storage(ABC):
    @abstractmethod
    def exec_ddl(self, ddl_sql: str) -> None: ...

    @abstractmethod
    def ensure_exchange_account_symbol(self, exchange: str, account: str, symbols: list[str]) -> dict[str, int]: ...

    @abstractmethod
    def get_last_pos_uid(self, exchange_id: int, account_id: int, symbol_id: int, strategy_id: str) -> str | None: ...

    @abstractmethod
    def get_latest_open_pos_uid(self, exchange_id: int, account_id: int, symbol_id: int, strategy_id: str) -> str | None: ...

    @abstractmethod
    def upsert_hedge_link(self, row: dict) -> None: ...

    # ------------------------------------------------------------------
    # OMS helpers
    # ------------------------------------------------------------------

    @abstractmethod
    def client_order_exists(self, exchange_id: int, account_id: int, client_order_id: str) -> bool: ...

    @abstractmethod
    def upsert_order_placeholder(self, row: dict) -> None: ...

    @abstractmethod
    def expire_stuck_pending(self, *, exchange_id: int, account_id: int, timeout_sec: int) -> int: ...

    @abstractmethod
    def resolve_placeholders_with_open_orders(
        self, *, exchange_id: int, account_id: int, open_orders: list[dict]
    ) -> int: ...

    @abstractmethod
    def get_pending_order_by_client_id(
        self, *, exchange_id: int, account_id: int, client_order_id: str
    ) -> dict | None: ...

    # ------------------------------------------------------------------
    # risk helpers
    # ------------------------------------------------------------------

    @abstractmethod
    def get_today_realized_pnl(self, exchange_id: int, account_id: int) -> float: ...

    # ------------------------------------------------------------------
    # retention (единая точка)
    # ------------------------------------------------------------------

    @abstractmethod
    def retention_cleanup(
        self,
        *,
        candles_days: int,
        snapshots_days: int,
        funding_days: int,
        orders_days: int,
        trades_days: int,
        fills_days: int,
        dry_run: bool,
    ) -> dict: ...

    # ------------------------------------------------------------------
    # upserts
    # ------------------------------------------------------------------

    @abstractmethod
    def upsert_positions(self, rows: Iterable[dict] | Iterable[Any]) -> None: ...

    @abstractmethod
    def upsert_orders(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def upsert_trades(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def upsert_order_fills(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def upsert_candles(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def upsert_funding(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def insert_snapshots(self, rows: Iterable[dict]) -> None: ...

    # --- v9 Market State Layer ---
    @abstractmethod
    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def upsert_open_interest(self, rows: Iterable[dict]) -> None: ...

    @abstractmethod
    def get_latest_account_balance_snapshot(self, exchange_id: int, account_id: int) -> dict | None: ...

    @abstractmethod
    def get_equity_peak(self, exchange_id: int, account_id: int, lookback_days: int = 90) -> float | None: ...

    # --- symbol filters ---
    @abstractmethod
    def get_symbol_filters(
        self,
        *,
        exchange_id: int,
        symbol_id: int,
    ) -> dict | None:
        """
        Returns symbol filters:
        {
            price_tick,
            qty_step,
            min_qty,
            max_qty,
            min_notional,
            max_leverage,
            margin_type,
        }
        """