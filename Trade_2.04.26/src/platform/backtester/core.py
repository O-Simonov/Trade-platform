# src/platform/backtester/core.py

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAILING_STOP = "TRAILING_STOP"


class ExitReason(str, Enum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT_1 = "TAKE_PROFIT_1"
    TAKE_PROFIT_2 = "TAKE_PROFIT_2"
    TRAILING_STOP = "TRAILING_STOP"
    TIME_EXIT = "TIME_EXIT"
    MANUAL = "MANUAL"
    END_OF_DATA = "END_OF_DATA"


@dataclass(slots=True)
class Candle:
    ts: Any
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalEvent:
    ts: Any
    symbol: str
    strategy_id: str
    side: Side
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PositionLot:
    qty: float
    price: float
    tag: str = "entry"


@dataclass(slots=True)
class Position:
    symbol: str
    side: Side
    opened_at: Any
    lots: List[PositionLot]
    scale_in_count: int = 0
    closed_qty: float = 0.0
    realized_pnl: float = 0.0
    fees: float = 0.0
    best_price: Optional[float] = None
    worst_price: Optional[float] = None
    tp1_done: bool = False
    tp2_done: bool = False
    trailing_armed: bool = False
    trailing_anchor: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def qty(self) -> float:
        return sum(l.qty for l in self.lots)

    @property
    def avg_price(self) -> float:
        qty = self.qty
        if qty <= 0:
            return 0.0
        return sum(l.qty * l.price for l in self.lots) / qty

    def add_lot(self, qty: float, price: float, tag: str) -> None:
        self.lots.append(PositionLot(qty=qty, price=price, tag=tag))


@dataclass(slots=True)
class Fill:
    ts: Any
    symbol: str
    side: Side
    qty: float
    price: float
    fee: float = 0.0
    tag: str = ""


@dataclass(slots=True)
class ClosedTrade:
    symbol: str
    side: Side
    opened_at: Any
    closed_at: Any
    entry_price: float
    exit_price: float
    qty_opened: float
    qty_closed: float
    realized_pnl: float
    fees: float
    net_pnl: float
    scale_in_count: int
    exit_reason: ExitReason
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: Side
    order_type: OrderType
    qty: float
    tag: str
    stop_price: Optional[float] = None
    limit_price: Optional[float] = None
    reduce_only: bool = False
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyParams:
    initial_qty_usdt: float = 100.0
    commission_rate: float = 0.0004
    sl_pct: float = 2.0
    tp_pct: float = 6.0
    averaging_enabled: bool = True
    averaging_add_position_multiplier: float = 2.0
    max_adds: int = 2
    defer_stop_loss_until_last_add: bool = True
    add_distance_pct: float = 2.0
    main_partial_tp_enabled: bool = True
    main_partial_tp1_pct: float = 1.8
    main_partial_tp1_share: float = 0.30
    main_partial_tp2_pct: float = 3.8
    main_partial_tp2_share: float = 0.30
    trailing_enabled: bool = True
    trailing_activation_pct: float = 1.2
    trailing_trail_pct: float = 0.6


class SignalProvider(Protocol):
    def events_for_candle(self, candle: Candle) -> Iterable[SignalEvent]: ...


class StrategyAdapter(Protocol):
    def on_signal(self, signal: SignalEvent, portfolio: "Portfolio", broker: "Broker") -> None: ...

    def on_candle(self, candle: Candle, portfolio: "Portfolio", broker: "Broker") -> None: ...


class Broker:
    def __init__(self, params: StrategyParams):
        self.params = params
        self.pending_orders: List[OrderRequest] = []

    def submit(self, order: OrderRequest) -> None:
        self.pending_orders.append(order)

    def clear_pending(self) -> List[OrderRequest]:
        orders = self.pending_orders[:]
        self.pending_orders.clear()
        return orders


class Portfolio:
    def __init__(self, params: StrategyParams):
        self.params = params
        self.open_positions: Dict[str, Position] = {}
        self.closed_trades: List[ClosedTrade] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.cash_pnl: float = 0.0

    def get(self, symbol: str) -> Optional[Position]:
        return self.open_positions.get(symbol)

    def has_open_position(self, symbol: str) -> bool:
        return symbol in self.open_positions

    def open_position(
        self,
        ts: Any,
        symbol: str,
        side: Side,
        price: float,
        qty: float,
        tag: str = "entry",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Position:
        pos = Position(
            symbol=symbol,
            side=side,
            opened_at=ts,
            lots=[PositionLot(qty=qty, price=price, tag=tag)],
            meta=dict(meta or {}),
        )
        fee = qty * price * self.params.commission_rate
        pos.fees += fee
        self.open_positions[symbol] = pos
        return pos

    def scale_in(self, ts: Any, symbol: str, price: float, qty: float, tag: str) -> None:
        pos = self.open_positions[symbol]
        pos.add_lot(qty=qty, price=price, tag=tag)
        pos.scale_in_count += 1
        pos.fees += qty * price * self.params.commission_rate

    def reduce_position(self, ts: Any, symbol: str, price: float, qty: float, reason: ExitReason) -> None:
        pos = self.open_positions[symbol]
        remaining = qty
        realized = 0.0
        new_lots: List[PositionLot] = []

        for lot in pos.lots:
            if remaining <= 0:
                new_lots.append(lot)
                continue

            take = min(lot.qty, remaining)
            pnl = self._lot_pnl(pos.side, lot.price, price, take)
            realized += pnl

            left = lot.qty - take
            if left > 0:
                new_lots.append(PositionLot(qty=left, price=lot.price, tag=lot.tag))

            remaining -= take

        actual_qty = qty - remaining
        fee = actual_qty * price * self.params.commission_rate
        pos.realized_pnl += realized
        pos.fees += fee
        pos.closed_qty += actual_qty
        pos.lots = new_lots

        if pos.qty <= 1e-12:
            self.close_position(ts, symbol, price, reason)

    def close_position(self, ts: Any, symbol: str, price: float, reason: ExitReason) -> None:
        pos = self.open_positions[symbol]

        if pos.qty > 1e-12:
            self.reduce_position(ts, symbol, price, pos.qty, reason)
            return

        entry_price = float(pos.meta.get("initial_entry_price") or pos.avg_price or 0.0)
        qty_opened = float(pos.meta.get("initial_qty") or pos.closed_qty)
        net_pnl = pos.realized_pnl - pos.fees
        self.cash_pnl += net_pnl

        self.closed_trades.append(
            ClosedTrade(
                symbol=pos.symbol,
                side=pos.side,
                opened_at=pos.opened_at,
                closed_at=ts,
                entry_price=entry_price,
                exit_price=price,
                qty_opened=qty_opened,
                qty_closed=pos.closed_qty,
                realized_pnl=pos.realized_pnl,
                fees=pos.fees,
                net_pnl=net_pnl,
                scale_in_count=pos.scale_in_count,
                exit_reason=reason,
                meta=dict(pos.meta),
            )
        )
        del self.open_positions[symbol]

    def mark_to_market(self, ts: Any, prices: Dict[str, float]) -> None:
        unrealized = 0.0
        for symbol, pos in self.open_positions.items():
            mark = prices.get(symbol)
            if mark is None:
                continue

            if pos.best_price is None:
                pos.best_price = mark
                pos.worst_price = mark
            else:
                pos.best_price = max(pos.best_price, mark) if pos.side is Side.LONG else min(pos.best_price, mark)
                pos.worst_price = min(pos.worst_price, mark) if pos.side is Side.LONG else max(pos.worst_price, mark)

            unrealized += self._lot_pnl(pos.side, pos.avg_price, mark, pos.qty)

        self.equity_curve.append(
            {
                "ts": ts,
                "equity": self.cash_pnl + unrealized,
                "cash_pnl": self.cash_pnl,
                "unrealized": unrealized,
            }
        )

    @staticmethod
    def _lot_pnl(side: Side, entry: float, exit_price: float, qty: float) -> float:
        if side is Side.LONG:
            return (exit_price - entry) * qty
        return (entry - exit_price) * qty


class GenericEventDrivenStrategy:
    """
    Reusable baseline strategy engine.
    It supports:
      - one entry per symbol
      - up to N scale-ins
      - optional deferred SL until final add
      - TP1/TP2 partial exits
      - trailing stop on the remainder
    This is intentionally generic so specific strategies only need to produce signals.
    """

    def __init__(self, params: StrategyParams):
        self.params = params

    def on_signal(self, signal: SignalEvent, portfolio: Portfolio, broker: Broker) -> None:
        if portfolio.has_open_position(signal.symbol):
            return

        entry_price = float(signal.payload["entry_price"])
        qty = self.params.initial_qty_usdt / entry_price
        broker.submit(
            OrderRequest(
                symbol=signal.symbol,
                side=signal.side,
                order_type=OrderType.MARKET,
                qty=qty,
                tag="entry",
                meta={
                    "fill_price": entry_price,
                    "signal_payload": dict(signal.payload),
                    "strategy_id": signal.strategy_id,
                },
            )
        )

    def on_candle(self, candle: Candle, portfolio: Portfolio, broker: Broker) -> None:
        pos = portfolio.get(candle.symbol)
        if not pos:
            return

        avg = pos.avg_price
        side = pos.side
        p = self.params

        if p.averaging_enabled and pos.scale_in_count < p.max_adds:
            next_level_price = self._next_add_price(avg, side, pos.scale_in_count + 1)
            if self._crossed(candle, next_level_price):
                mult = max(1.0, p.averaging_add_position_multiplier)
                add_qty = pos.qty * (mult - 1.0)
                if add_qty > 0:
                    broker.submit(
                        OrderRequest(
                            symbol=pos.symbol,
                            side=side,
                            order_type=OrderType.MARKET,
                            qty=add_qty,
                            tag=f"add_{pos.scale_in_count + 1}",
                            meta={"fill_price": next_level_price},
                        )
                    )

        sl_active = (not p.defer_stop_loss_until_last_add) or (pos.scale_in_count >= p.max_adds)
        if sl_active:
            sl_price = self._stop_price(pos.avg_price, side)
            if self._crossed(candle, sl_price):
                broker.submit(
                    OrderRequest(
                        symbol=pos.symbol,
                        side=self._close_side(side),
                        order_type=OrderType.STOP,
                        qty=pos.qty,
                        tag="stop_loss",
                        reduce_only=True,
                        meta={"fill_price": sl_price, "exit_reason": ExitReason.STOP_LOSS},
                    )
                )
                return

        if p.main_partial_tp_enabled and not pos.tp1_done:
            tp1 = self._tp_price(pos.avg_price, side, p.main_partial_tp1_pct)
            if self._crossed(candle, tp1):
                broker.submit(
                    OrderRequest(
                        symbol=pos.symbol,
                        side=self._close_side(side),
                        order_type=OrderType.TAKE_PROFIT,
                        qty=pos.qty * p.main_partial_tp1_share,
                        tag="tp1",
                        reduce_only=True,
                        meta={"fill_price": tp1, "exit_reason": ExitReason.TAKE_PROFIT_1},
                    )
                )

        if p.main_partial_tp_enabled and pos.tp1_done and not pos.tp2_done:
            tp2 = self._tp_price(pos.avg_price, side, p.main_partial_tp2_pct)
            if self._crossed(candle, tp2):
                broker.submit(
                    OrderRequest(
                        symbol=pos.symbol,
                        side=self._close_side(side),
                        order_type=OrderType.TAKE_PROFIT,
                        qty=pos.qty * p.main_partial_tp2_share,
                        tag="tp2",
                        reduce_only=True,
                        meta={"fill_price": tp2, "exit_reason": ExitReason.TAKE_PROFIT_2},
                    )
                )

        if p.trailing_enabled:
            activation = self._tp_price(pos.avg_price, side, p.trailing_activation_pct)
            if not pos.trailing_armed and self._crossed(candle, activation):
                pos.trailing_armed = True
                pos.trailing_anchor = candle.high if side is Side.LONG else candle.low
            elif pos.trailing_armed:
                if side is Side.LONG:
                    pos.trailing_anchor = max(float(pos.trailing_anchor or candle.high), candle.high)
                    stop = pos.trailing_anchor * (1 - p.trailing_trail_pct / 100)
                else:
                    pos.trailing_anchor = min(float(pos.trailing_anchor or candle.low), candle.low)
                    stop = pos.trailing_anchor * (1 + p.trailing_trail_pct / 100)

                if self._crossed(candle, stop):
                    broker.submit(
                        OrderRequest(
                            symbol=pos.symbol,
                            side=self._close_side(side),
                            order_type=OrderType.TRAILING_STOP,
                            qty=pos.qty,
                            tag="trailing",
                            reduce_only=True,
                            meta={"fill_price": stop, "exit_reason": ExitReason.TRAILING_STOP},
                        )
                    )

    def _crossed(self, candle: Candle, price: float) -> bool:
        return candle.low <= price <= candle.high

    def _next_add_price(self, avg_price: float, side: Side, add_number: int) -> float:
        dist = self.params.add_distance_pct * add_number / 100.0
        return avg_price * (1 - dist) if side is Side.LONG else avg_price * (1 + dist)

    def _stop_price(self, avg_price: float, side: Side) -> float:
        pct = self.params.sl_pct / 100.0
        return avg_price * (1 - pct) if side is Side.LONG else avg_price * (1 + pct)

    def _tp_price(self, avg_price: float, side: Side, pct_value: float) -> float:
        pct = pct_value / 100.0
        return avg_price * (1 + pct) if side is Side.LONG else avg_price * (1 - pct)

    def _close_side(self, side: Side) -> Side:
        return Side.SHORT if side is Side.LONG else Side.LONG


class BacktestEngine:
    def __init__(self, *, strategy: StrategyAdapter, signal_provider: SignalProvider, params: StrategyParams):
        self.strategy = strategy
        self.signal_provider = signal_provider
        self.params = params
        self.portfolio = Portfolio(params)
        self.broker = Broker(params)

    def run(self, candles: Iterable[Candle]) -> Portfolio:
        prices: Dict[str, float] = {}
        last_ts: Any = None

        for candle in candles:
            last_ts = candle.ts
            prices[candle.symbol] = candle.close

            for signal in self.signal_provider.events_for_candle(candle):
                self.strategy.on_signal(signal, self.portfolio, self.broker)

            self.strategy.on_candle(candle, self.portfolio, self.broker)
            self._execute_orders(candle)
            self.portfolio.mark_to_market(candle.ts, prices)

        if last_ts is not None:
            for symbol in list(self.portfolio.open_positions.keys()):
                mark_price = prices.get(symbol)
                if mark_price is None:
                    continue
                self.portfolio.close_position(last_ts, symbol, mark_price, ExitReason.END_OF_DATA)

        return self.portfolio

    def _execute_orders(self, candle: Candle) -> None:
        for order in self.broker.clear_pending():
            fill_price = float(order.meta.get("fill_price") or candle.close)

            if order.tag == "entry":
                meta = dict(order.meta)
                meta.setdefault("initial_entry_price", fill_price)
                meta.setdefault("initial_qty", order.qty)
                self.portfolio.open_position(
                    candle.ts,
                    order.symbol,
                    order.side,
                    fill_price,
                    order.qty,
                    tag=order.tag,
                    meta=meta,
                )
                continue

            if order.tag.startswith("add_"):
                self.portfolio.scale_in(candle.ts, order.symbol, fill_price, order.qty, tag=order.tag)
                continue

            reason = order.meta.get("exit_reason") or ExitReason.MANUAL
            self.portfolio.reduce_position(candle.ts, order.symbol, fill_price, order.qty, reason=reason)
            pos = self.portfolio.get(order.symbol)
            if pos:
                if order.tag == "tp1":
                    pos.tp1_done = True
                elif order.tag == "tp2":
                    pos.tp2_done = True


class DictSignalProvider:
    def __init__(self, events: Iterable[SignalEvent]):
        self._events_by_key: Dict[tuple, List[SignalEvent]] = {}
        for e in events:
            self._events_by_key.setdefault((e.symbol, e.ts), []).append(e)

    def events_for_candle(self, candle: Candle) -> Iterable[SignalEvent]:
        return self._events_by_key.get((candle.symbol, candle.ts), [])