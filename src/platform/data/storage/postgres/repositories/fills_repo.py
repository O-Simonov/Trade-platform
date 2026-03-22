from __future__ import annotations

import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Sequence

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

from .base import _utcnow

class FillsRepositoryMixin:

    @staticmethod
    def _mk_fill_uid(
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

    def upsert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return

        query = """
                INSERT INTO trades (exchange_id, account_id, trade_id, order_id, symbol_id, \
                                    strategy_id, pos_uid, side, price, qty, fee, fee_asset, \
                                    realized_pnl, ts, source, raw_json)
                VALUES (%(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s, \
                        %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s, %(fee_asset)s, \
                        %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s) ON CONFLICT (exchange_id, account_id, trade_id)
            DO \
                UPDATE SET
                    fee = EXCLUDED.fee, \
                    fee_asset = EXCLUDED.fee_asset, \
                    realized_pnl = EXCLUDED.realized_pnl, \
                    ts = EXCLUDED.ts, \
                    raw_json = EXCLUDED.raw_json \
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

        self._exec_many(query, rows)

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

