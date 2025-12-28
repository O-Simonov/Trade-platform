# src/platform/core/oms/reconcile.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Protocol, Sequence

log = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class OmsRestExchange(Protocol):
    """
    Минимальный интерфейс для REST reconciliation.

    Должен вернуть список "нормализованных" ордеров в формате dict, совместимом со storage.upsert_orders().
    """
    def fetch_open_orders(self, *, symbol_ids: Sequence[int]) -> list[dict]: ...


class StorageLike(Protocol):
    """
    Мы используем только эти методы и/или прямой доступ к pool (psycopg_pool.ConnectionPool).
    """
    def upsert_orders(self, orders: list[dict]) -> None: ...

    # В Postgres storage обычно есть .pool: ConnectionPool
    pool: Any


@dataclass(frozen=True)
class ReconcileStats:
    fetched_open: int = 0
    upserted_open: int = 0
    merged_real: int = 0
    resolved_ph: int = 0


class OmsReconciler:
    """
    Step 3 — REST reconciliation:
      - подтягиваем openOrders (REST)
      - upsert в таблицу orders
      - MERGE placeholder PH::... -> real order (по client_order_id)
        переносим strategy_id и pos_uid на реальный order_id

    ВАЖНО:
      - Мы НЕ мутируем OrderIntent
      - Мы НЕ ломаем WS пайплайн
      - Мы НЕ трогаем filled/canceled, только "обогащаем" real orders, где strategy_id='unknown'
    """

    def __init__(
        self,
        *,
        storage: StorageLike,
        exchange: OmsRestExchange,
        exchange_id: int,
        account_id: int,
        logger: logging.Logger | None = None,
    ):
        self.storage = storage
        self.exchange = exchange
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.logger = logger or log

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_once(self, *, symbol_ids: Sequence[int]) -> ReconcileStats:
        """
        1) fetch open orders from REST
        2) upsert orders
        3) merge placeholders -> real by client_order_id
        """
        stats = ReconcileStats()

        # 1) fetch
        try:
            open_orders = self.exchange.fetch_open_orders(symbol_ids=list(symbol_ids))
        except Exception as e:
            self.logger.warning("[OMS][RECON] fetch_open_orders failed: %s", e)
            return stats

        stats = ReconcileStats(
            fetched_open=len(open_orders),
            upserted_open=0,
            merged_real=0,
            resolved_ph=0,
        )

        # 2) upsert open orders
        try:
            if open_orders:
                self.storage.upsert_orders(open_orders)
                stats = ReconcileStats(
                    fetched_open=stats.fetched_open,
                    upserted_open=len(open_orders),
                    merged_real=stats.merged_real,
                    resolved_ph=stats.resolved_ph,
                )
        except Exception as e:
            self.logger.warning("[OMS][RECON] upsert_orders failed: %s", e)

        # 3) merge PH -> REAL
        try:
            merged_real, resolved_ph = self._merge_placeholders_into_real()
            stats = ReconcileStats(
                fetched_open=stats.fetched_open,
                upserted_open=stats.upserted_open,
                merged_real=merged_real,
                resolved_ph=resolved_ph,
            )
        except Exception as e:
            self.logger.warning("[OMS][RECON] merge_placeholders failed: %s", e)

        if stats.merged_real or stats.resolved_ph:
            self.logger.info(
                "[OMS][RECON] open=%d upsert=%d merged_real=%d resolved_ph=%d",
                stats.fetched_open, stats.upserted_open, stats.merged_real, stats.resolved_ph
            )

        return stats

    # ------------------------------------------------------------------
    # Core: MERGE placeholder -> real order
    # ------------------------------------------------------------------

    def _merge_placeholders_into_real(self) -> tuple[int, int]:
        """
        Находим пары:
          PH.order_id LIKE 'PH::%'
          REAL.client_order_id == PH.client_order_id
          REAL.strategy_id = 'unknown' OR NULL
          (и/или REAL.pos_uid IS NULL)

        И делаем:
          REAL.strategy_id = PH.strategy_id
          REAL.pos_uid = PH.pos_uid
          REAL.updated_at = NOW()

        Затем помечаем PH как RESOLVED (если поле status есть).
        """
        pool = getattr(self.storage, "pool", None)
        if pool is None:
            raise RuntimeError("storage.pool not found (OmsReconciler requires Postgres pool)")

        # 1) Обогащаем реальные ордера
        #    ВАЖНО: client_order_id должен совпадать, exchange_id/account_id одинаковые.
        merge_sql = """
        WITH ph AS (
            SELECT exchange_id, account_id, client_order_id, strategy_id, pos_uid
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id LIKE 'PH::%%'
              AND client_order_id IS NOT NULL
        ),
        real AS (
            SELECT exchange_id, account_id, order_id, client_order_id, strategy_id, pos_uid
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id NOT LIKE 'PH::%%'
              AND client_order_id IS NOT NULL
        ),
        pairs AS (
            SELECT
                real.order_id AS real_order_id,
                ph.client_order_id AS client_order_id,
                ph.strategy_id AS ph_strategy_id,
                ph.pos_uid AS ph_pos_uid
            FROM real
            JOIN ph USING (exchange_id, account_id, client_order_id)
            WHERE (real.strategy_id IS NULL OR real.strategy_id = 'unknown'
                   OR real.pos_uid IS NULL)
        )
        UPDATE orders o
        SET
            strategy_id = COALESCE(o.strategy_id, pairs.ph_strategy_id),
            pos_uid     = COALESCE(o.pos_uid, pairs.ph_pos_uid),
            updated_at  = NOW()
        FROM pairs
        WHERE o.exchange_id = %s
          AND o.account_id = %s
          AND o.order_id = pairs.real_order_id
        RETURNING o.order_id, pairs.client_order_id;
        """

        resolved_sql = """
        WITH matched AS (
            SELECT DISTINCT ph.order_id AS ph_order_id
            FROM orders ph
            JOIN orders real
              ON real.exchange_id = ph.exchange_id
             AND real.account_id = ph.account_id
             AND real.client_order_id = ph.client_order_id
            WHERE ph.exchange_id = %s
              AND ph.account_id = %s
              AND ph.order_id LIKE 'PH::%%'
              AND real.order_id NOT LIKE 'PH::%%'
              AND real.client_order_id IS NOT NULL
        )
        UPDATE orders o
        SET
            status = CASE
                WHEN o.status IS NULL THEN 'RESOLVED'
                WHEN o.status IN ('PENDING_SUBMIT','PENDING') THEN 'RESOLVED'
                ELSE o.status
            END,
            updated_at = NOW()
        FROM matched
        WHERE o.exchange_id = %s
          AND o.account_id = %s
          AND o.order_id = matched.ph_order_id
        RETURNING o.order_id;
        """

        merged_real = 0
        resolved_ph = 0

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    merge_sql,
                    (
                        self.exchange_id, self.account_id,
                        self.exchange_id, self.account_id,
                        self.exchange_id, self.account_id,
                    ),
                )
                rows = cur.fetchall()
                merged_real = len(rows or [])

                # Помечаем placeholder как RESOLVED (мягко)
                cur.execute(
                    resolved_sql,
                    (
                        self.exchange_id, self.account_id,
                        self.exchange_id, self.account_id,
                    ),
                )
                ph_rows = cur.fetchall()
                resolved_ph = len(ph_rows or [])

        return merged_real, resolved_ph
