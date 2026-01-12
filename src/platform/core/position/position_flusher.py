# src/platform/core/position/position_flusher.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional


class PositionFlusher:
    """
    Flush dirty PositionAggregate objects to DB.

    Writes:
      1) positions (current state per symbol)
      2) position_snapshots (latest snapshot rows)
      3) position_ledger (lifecycle history)

    Key fix:
      - opened_at/closed_at/pos_uid are taken from PositionManager lifecycle cache,
        not from PositionAggregate (because it is slots=True).
    """

    EPS_QTY_ABS: float = 1e-12

    def __init__(
        self,
        *,
        position_manager: Any,
        storage: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        self.pm = position_manager
        self.storage = storage
        self.logger = logger or logging.getLogger("positions.flusher")

    # -------------------------
    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _f(x: Any, default: float = 0.0) -> float:
        try:
            v = float(x)
            if v != v:  # NaN
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def _norm_side(side: Any) -> str:
        s = str(side or "").upper()
        if s in ("LONG", "SHORT", "FLAT"):
            return s
        if s.endswith(".LONG"):
            return "LONG"
        if s.endswith(".SHORT"):
            return "SHORT"
        if s.endswith(".FLAT"):
            return "FLAT"
        return "FLAT"

    # -------------------------
    def _build_positions_row(self, agg: Any, life: Any) -> dict:
        row = agg.to_row()
        if not isinstance(row, dict):
            raise TypeError("agg.to_row() must return dict")

        ex_id = int(row.get("exchange_id") or getattr(agg, "exchange_id", 0) or 0)
        acc_id = int(row.get("account_id") or getattr(agg, "account_id", 0) or 0)
        sym_id = int(row.get("symbol_id") or getattr(agg, "symbol_id", 0) or 0)

        side = self._norm_side(row.get("side") or getattr(agg, "side", "FLAT"))
        qty = self._f(row.get("qty") or getattr(agg, "qty", 0.0), 0.0)

        # --- lifecycle from cache ---
        opened_at = getattr(life, "opened_at", None) if life is not None else None
        closed_at = getattr(life, "closed_at", None) if life is not None else None
        pos_uid = str(getattr(life, "pos_uid", "") or "") if life is not None else ""

        now = self._utc_now()

        # ВАЖНО: ключи должны существовать всегда (иначе exec_many может упасть)
        row.setdefault("opened_at", None)
        row.setdefault("closed_at", None)
        row.setdefault("pos_uid", "")

        # ---- OPEN ----
        if qty > self.EPS_QTY_ABS:
            # side must be LONG/SHORT
            if side not in ("LONG", "SHORT"):
                # крайне нежелательно, но лучше, чем открыть “непонятно что”
                side = "LONG"

            # если lifecycle ещё не создан — создаём
            if opened_at is None:
                opened_at = now
                # best-effort сохранить в life/agg (чтобы не менять каждый flush)
                try:
                    if life is not None:
                        setattr(life, "opened_at", opened_at)
                except Exception:
                    pass
                try:
                    # иногда life хранится прямо в agg
                    if hasattr(agg, "opened_at"):
                        setattr(agg, "opened_at", opened_at)
                except Exception:
                    pass

            if not pos_uid:
                opened_ms = int(opened_at.timestamp() * 1000)
                pos_uid = f"{ex_id}:{acc_id}:{sym_id}:{opened_ms}"
                try:
                    if life is not None:
                        setattr(life, "pos_uid", pos_uid)
                except Exception:
                    pass
                try:
                    if hasattr(agg, "pos_uid"):
                        setattr(agg, "pos_uid", pos_uid)
                except Exception:
                    pass

            # пишем состояние OPEN
            row["side"] = side
            row["qty"] = float(qty)
            row["status"] = "OPEN"

            row["opened_at"] = opened_at
            row["closed_at"] = None
            row["pos_uid"] = pos_uid

        # ---- CLOSED / FLAT ----
        else:
            row["side"] = "FLAT"
            row["qty"] = 0.0
            row["status"] = "CLOSED"

            # закрытие ставим, если есть opened_at (или если хотим фиксировать “закрылось без открытия”)
            if opened_at is not None and closed_at is None:
                closed_at = now
                try:
                    if life is not None:
                        setattr(life, "closed_at", closed_at)
                except Exception:
                    pass
                try:
                    if hasattr(agg, "closed_at"):
                        setattr(agg, "closed_at", closed_at)
                except Exception:
                    pass

            row["opened_at"] = opened_at  # может быть None, но ключ есть
            row["closed_at"] = closed_at  # может быть None, если вообще нет lifecycle
            row["pos_uid"] = pos_uid

            # опционально, но очень полезно: чистим ценовые поля для FLAT
            row["entry_price"] = 0.0
            row["avg_price"] = 0.0
            row["exit_price"] = row.get("exit_price") if row.get("exit_price") is not None else 0.0
            row["mark_price"] = 0.0
            row["unrealized_pnl"] = 0.0
            row["position_value_usdt"] = 0.0

        # --- be explicit base fields ---
        row["exchange_id"] = ex_id
        row["account_id"] = acc_id
        row["symbol_id"] = sym_id
        row["updated_at"] = now

        # ВАЖНО: positions.strategy_id NOT NULL
        row.setdefault("strategy_id", "unknown")
        row.setdefault("strategy_name", None)

        # прочие дефолты под твой upsert_positions()
        row.setdefault("fees", 0.0)
        row.setdefault("realized_pnl", self._f(row.get("realized_pnl", 0.0), 0.0))
        row.setdefault("scale_in_count", 0)
        row.setdefault("last_trade_id", row.get("last_trade_id"))
        row.setdefault("last_ts", row.get("last_ts"))

        row.setdefault("source", "position_aggregate")

        return row

    def _build_snapshot_row(self, agg: Any, pos_row: dict) -> dict:
        ex_id = int(pos_row.get("exchange_id") or getattr(agg, "exchange_id", 0) or 0)
        acc_id = int(pos_row.get("account_id") or getattr(agg, "account_id", 0) or 0)
        sym_id = int(pos_row.get("symbol_id") or getattr(agg, "symbol_id", 0) or 0)

        side = self._norm_side(pos_row.get("side") or getattr(agg, "side", "FLAT"))
        qty = self._f(pos_row.get("qty") or getattr(agg, "qty", 0.0), 0.0)

        entry_price = self._f(pos_row.get("entry_price") or getattr(agg, "entry_price", 0.0), 0.0)
        mark_price = self._f(pos_row.get("mark_price") or getattr(agg, "mark_price", None), entry_price)

        position_value = pos_row.get("position_value")
        if position_value is None:
            position_value = pos_row.get("position_value_usdt")
        if position_value is None:
            position_value = abs(qty) * float(mark_price)

        unrealized_pnl = pos_row.get("unrealized_pnl", getattr(agg, "unrealized_pnl", 0.0))
        realized_pnl = pos_row.get("realized_pnl", getattr(agg, "realized_pnl", 0.0))
        fees = pos_row.get("fees", getattr(agg, "fees", 0.0))

        last_ts = pos_row.get("last_ts") or self._utc_now()

        return {
            "exchange_id": ex_id,
            "account_id": int(acc_id),
            "symbol_id": sym_id,
            "side": side,
            "qty": float(qty),
            "entry_price": float(entry_price),
            "mark_price": float(mark_price),
            "position_value": float(position_value),
            "unrealized_pnl": float(self._f(unrealized_pnl, 0.0)),
            "realized_pnl": float(self._f(realized_pnl, 0.0)),
            "fees": float(self._f(fees, 0.0)),
            "last_ts": last_ts,
            "updated_at": self._utc_now(),
            "source": "position_aggregate",
        }

    def _build_ledger_row(self, agg: Any, pos_row: dict, life: Any) -> Optional[dict]:
        if life is None:
            return None

        opened_at = getattr(life, "opened_at", None)
        if opened_at is None:
            return None  # lifecycle not started

        closed_at = getattr(life, "closed_at", None)
        pos_uid = str(getattr(life, "pos_uid", "") or "")
        if not pos_uid:
            return None

        ex_id = int(pos_row.get("exchange_id") or getattr(agg, "exchange_id", 0) or 0)
        acc_id = int(pos_row.get("account_id") or getattr(agg, "account_id", 0) or 0)
        sym_id = int(pos_row.get("symbol_id") or getattr(agg, "symbol_id", 0) or 0)

        qty = self._f(pos_row.get("qty") or getattr(agg, "qty", 0.0), 0.0)

        side = self._norm_side(pos_row.get("side") or getattr(life, "last_side", "FLAT"))
        if side == "FLAT":
            side = self._norm_side(getattr(life, "last_side", "LONG")) or "LONG"

        status = "CLOSED" if (abs(qty) <= self.EPS_QTY_ABS and closed_at is not None) else "OPEN"

        entry_price = self._f(pos_row.get("entry_price") or getattr(agg, "entry_price", 0.0), 0.0)
        avg_price = self._f(pos_row.get("avg_price") or getattr(agg, "avg_price", None), entry_price)
        mark_price = self._f(pos_row.get("mark_price") or getattr(agg, "mark_price", None), avg_price)

        exit_price = getattr(agg, "exit_price", None)
        if exit_price is None and status == "CLOSED":
            exit_price = mark_price

        # DELTA realized/fees for lifecycle
        realized_total = self._f(pos_row.get("realized_pnl") if pos_row.get("realized_pnl") is not None else getattr(agg, "realized_pnl", 0.0), 0.0)
        fees_total = self._f(pos_row.get("fees") if pos_row.get("fees") is not None else getattr(agg, "fees", 0.0), 0.0)

        realized_at_open = self._f(getattr(life, "realized_at_open", 0.0), 0.0)
        fees_at_open = self._f(getattr(life, "fees_at_open", 0.0), 0.0)

        realized_delta = realized_total - realized_at_open
        fees_delta = fees_total - fees_at_open

        strategy_id = pos_row.get("strategy_id") or getattr(agg, "strategy_id", None) or "unknown"
        strategy_name = pos_row.get("strategy_name") or getattr(agg, "strategy_name", None)

        qty_opened = float(self._f(getattr(agg, "qty_opened", None), 0.0))
        qty_current = float(abs(qty))
        qty_closed = float(self._f(getattr(agg, "qty_closed", None), 0.0))
        if status == "OPEN" and qty_opened <= 0:
            qty_opened = qty_current
        if status == "CLOSED" and qty_closed <= 0:
            qty_closed = qty_opened if qty_opened > 0 else 0.0

        position_value_usdt = pos_row.get("position_value_usdt")
        if position_value_usdt is None:
            position_value_usdt = abs(qty_current) * float(mark_price)

        scale_in_count = pos_row.get("scale_in_count", getattr(agg, "scale_in_count", 0))

        return {
            "exchange_id": ex_id,
            "account_id": int(acc_id),
            "pos_uid": pos_uid,
            "symbol_id": sym_id,
            "strategy_id": str(strategy_id or "unknown"),
            "strategy_name": strategy_name,
            "side": side,
            "status": status,
            "opened_at": opened_at,
            "closed_at": closed_at,
            "entry_price": float(entry_price),
            "avg_price": float(avg_price),
            "exit_price": float(exit_price) if exit_price is not None else None,
            "qty_opened": float(self._f(qty_opened, 0.0)),
            "qty_current": float(self._f(qty_current, 0.0)),
            "qty_closed": float(self._f(qty_closed, 0.0)),
            "position_value_usdt": float(self._f(position_value_usdt, 0.0)),
            "scale_in_count": int(scale_in_count or 0),
            "realized_pnl": float(self._f(realized_delta, 0.0)),
            "fees": float(self._f(fees_delta, 0.0)),
            "updated_at": self._utc_now(),
            "source": "ledger",
        }

    # -------------------------
    def flush(self) -> int:
        pos_rows: list[dict] = []
        snap_rows: list[dict] = []
        ledger_rows: list[dict] = []
        dirty_aggs: list[Any] = []

        for agg in self.pm.iter_dirty_positions():
            sym_id = int(getattr(agg, "symbol_id", 0) or 0)
            if sym_id <= 0:
                try:
                    agg.clear_dirty()
                except Exception:
                    pass
                continue

            ex_id = int(getattr(agg, "exchange_id", 0) or 0)
            acc_id = int(getattr(agg, "account_id", 0) or 0)

            life = None
            try:
                if hasattr(self.pm, "get_lifecycle"):
                    life = self.pm.get_lifecycle(ex_id, acc_id, sym_id)
            except Exception:
                life = None

            try:
                pos_row = self._build_positions_row(agg, life)
            except Exception:
                self.logger.exception("[POSITIONS][FLUSH] build positions row failed symbol_id=%s", sym_id)
                continue

            try:
                snap_row = self._build_snapshot_row(agg, pos_row)
            except Exception:
                self.logger.exception("[POSITIONS][FLUSH] build snapshot failed symbol_id=%s", sym_id)
                snap_row = None

            led = None
            try:
                led = self._build_ledger_row(agg, pos_row, life)
            except Exception:
                self.logger.exception("[POSITIONS][FLUSH] build ledger failed symbol_id=%s", sym_id)
                led = None

            pos_rows.append(pos_row)
            if snap_row is not None:
                snap_rows.append(snap_row)
            if led is not None:
                ledger_rows.append(led)

            dirty_aggs.append(agg)

        if not pos_rows and not snap_rows and not ledger_rows:
            return 0

        try:
            if pos_rows:
                self.storage.upsert_positions(pos_rows)
            if snap_rows:
                self.storage.upsert_position_snapshots(snap_rows)
            if ledger_rows:
                self.storage.upsert_position_ledger(ledger_rows)

            for agg in dirty_aggs:
                try:
                    agg.clear_dirty()
                except Exception:
                    pass

            self.logger.info(
                "[POSITIONS][FLUSH] positions=%d snapshots=%d ledger=%d",
                len(pos_rows), len(snap_rows), len(ledger_rows)
            )
            return max(len(pos_rows), len(snap_rows), len(ledger_rows))

        except Exception:
            self.logger.exception("[POSITIONS][FLUSH] DB write failed")
            return 0
