from __future__ import annotations

import logging
from typing import Any, Iterable

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

from .base import *

class PositionsRepositoryMixin:
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
        close_side = side if side in ("LONG", "SHORT") else "FLAT"

        if qty <= 0.0:
            row["side"] = close_side
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
            ON CONFLICT (exchange_id, account_id, symbol_id, side)
            DO UPDATE SET
                strategy_id = CASE
                    WHEN positions.strategy_id = 'unknown'
                      AND EXCLUDED.strategy_id IS NOT NULL
                      AND EXCLUDED.strategy_id <> 'unknown'
                    THEN EXCLUDED.strategy_id
                    ELSE positions.strategy_id
                END,
                strategy_name = COALESCE(EXCLUDED.strategy_name, positions.strategy_name),

                -- positions = current state table, so active pos_uid/opened_at must follow latest row
                pos_uid = COALESCE(NULLIF(EXCLUDED.pos_uid, ''), positions.pos_uid),

                opened_at = COALESCE(EXCLUDED.opened_at, positions.opened_at),

                closed_at = CASE
                    WHEN EXCLUDED.status = 'OPEN' THEN NULL
                    ELSE COALESCE(EXCLUDED.closed_at, positions.closed_at)
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

            close_side = side if side in ("LONG", "SHORT") else "FLAT"
            if qty <= 0:
                d["qty"] = 0.0
                d["side"] = close_side
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
            ON CONFLICT (exchange_id, account_id, symbol_id, side)
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

    def get_latest_positions(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
            SELECT s.symbol,
                   p.symbol_id,
                   p.side,
                   p.pos_uid,
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
            ORDER BY s.symbol, p.side
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
            side = str(r.get("side") or "").upper()
            prev = out.get(sym)
            if prev is None:
                out[sym] = r
            else:
                if isinstance(prev, dict) and "side" in prev:
                    out[sym] = {str(prev.get("side") or "UNKNOWN").upper(): prev, side or "UNKNOWN": r}
                elif isinstance(prev, dict):
                    prev[side or "UNKNOWN"] = r
                    out[sym] = prev
                else:
                    out[sym] = {side or "UNKNOWN": r}
        return out

