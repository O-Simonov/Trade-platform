# src/platform/data/retention/retention_worker.py
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Iterable, Mapping

logger = logging.getLogger("src.platform.data.retention.retention_worker")

DEFAULT_CANDLE_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")


def _to_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _clean_intervals(raw: Any) -> list[str]:
    if raw is None:
        return list(DEFAULT_CANDLE_INTERVALS)
    if isinstance(raw, str):
        raw_list: Iterable[Any] = [raw]
    elif isinstance(raw, (list, tuple, set)):
        raw_list = raw
    else:
        raw_list = list(DEFAULT_CANDLE_INTERVALS)

    out: list[str] = []
    for v in raw_list:
        s = str(v).strip()
        if s:
            out.append(s)

    # уникализируем, сохраняя порядок
    seen = set()
    uniq: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _clean_oi_keep_days(raw: Any) -> Dict[str, int]:
    """
    Универсальный парсер retention.oi_keep_days:
      oi_keep_days:
        "5m": 14
        "15m": 60
        "1h": 90
        "4h": 180
        "1d": 365
    """
    out: Dict[str, int] = {}

    if not raw:
        return out

    if isinstance(raw, Mapping):
        for k, v in raw.items():
            itv = str(k).strip()
            if not itv:
                continue
            keep = _to_int(v, 0)
            out[itv] = keep
        return out

    logger.warning("[Retention] oi_keep_days должен быть dict, получили: %s -> игнор", type(raw).__name__)
    return {}


class RetentionWorker(threading.Thread):
    """
    Periodic DB cleanup (safe).

    Делает cleanup, только если в storage есть соответствующий метод:
      - cleanup_candles(exchange_id, interval, keep_days)
      - cleanup_open_interest(exchange_id, interval, keep_days)
      - cleanup_funding(exchange_id, keep_days)
      - cleanup_ticker_24h(exchange_id, keep_days)
      - cleanup_market_state_5m(exchange_id, keep_days)

      - cleanup_market_trades(exchange_id, keep_days)        ✅ NEW
      - cleanup_candles_trades_agg(exchange_id, keep_days)   ✅ NEW

      - cleanup_trades(exchange_id, keep_days) (опционально)
    """

    def __init__(self, *, storage, exchange_id: int, cfg: dict, run_sec: int = 3600):
        super().__init__(daemon=True, name="RetentionWorker")
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.cfg: dict = dict(cfg or {})
        self.run_sec = max(5, int(run_sec or 3600))

        # ВАЖНО: не называем _stop, у Thread уже есть внутренний _stop()
        self._stop_event = threading.Event()

        # logging knobs
        self.log_each_cycle = bool(self.cfg.get("log_each_cycle", False))
        self.log_zero_deletes = bool(self.cfg.get("log_zero_deletes", False))

        # candles
        self.candles_days = max(0, _to_int(self.cfg.get("candles_days", 180), 180))
        self.candle_intervals = _clean_intervals(self.cfg.get("candle_intervals"))

        # ----------------------------------------------------
        # open_interest retention per interval
        # приоритет:
        #   1) retention.oi_keep_days (универсально)
        #   2) fallback на oi_5m_days / oi_15m_days / oi_1h_days (старый формат)
        # ----------------------------------------------------
        oi_keep_days_new = _clean_oi_keep_days(self.cfg.get("oi_keep_days"))
        if oi_keep_days_new:
            self.oi_keep_days: Dict[str, int] = dict(oi_keep_days_new)
        else:
            self.oi_keep_days = {
                "5m": _to_int(self.cfg.get("oi_5m_days", 14), 14),
                "15m": _to_int(self.cfg.get("oi_15m_days", 60), 60),
                "1h": _to_int(self.cfg.get("oi_1h_days", 365), 365),
            }

        # подчистим мусор: отрицательные/нулевые
        self.oi_keep_days = {k: int(v) for k, v in (self.oi_keep_days or {}).items() if int(v) > 0}

        # funding
        self.funding_days = max(0, _to_int(self.cfg.get("funding_days", 365), 365))

        # ticker_24h
        self.ticker_24h_days = max(0, _to_int(self.cfg.get("ticker_24h_days", 90), 90))

        # market_state_5m
        self.market_state_5m_days = max(0, _to_int(self.cfg.get("market_state_5m_days", 30), 30))

        # trades (orders/trades from OMS) optional
        self.trade_cleanup_days = max(0, _to_int(self.cfg.get("trade_cleanup_days", 180), 180))

        # ✅ NEW: market_trades retention
        self.market_trades_days = max(0, _to_int(self.cfg.get("market_trades_days", 0), 0))

        # ✅ NEW: candles_trades_agg retention
        self.candles_trades_agg_days = max(0, _to_int(self.cfg.get("candles_trades_agg_days", 0), 0))

        logger.info(
            "RetentionWorker init: run_sec=%s candles_days=%s intervals=%s "
            "oi_keep_days=%s funding_days=%s ticker_24h_days=%s market_state_5m_days=%s "
            "market_trades_days=%s candles_trades_agg_days=%s trades_days=%s "
            "log_each_cycle=%s log_zero_deletes=%s",
            self.run_sec,
            self.candles_days,
            ",".join(self.candle_intervals) if self.candle_intervals else "-",
            dict(self.oi_keep_days),
            self.funding_days,
            self.ticker_24h_days,
            self.market_state_5m_days,
            self.market_trades_days,
            self.candles_trades_agg_days,
            self.trade_cleanup_days,
            self.log_each_cycle,
            self.log_zero_deletes,
        )

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception:
                logger.exception("Retention error")
            self._stop_event.wait(self.run_sec)

    def _safe_call(self, method_name: str, **kwargs) -> int:
        fn = getattr(self.storage, method_name, None)
        if not callable(fn):
            if self.log_each_cycle:
                logger.debug("[Retention] skip %s: method missing on storage", method_name)
            return 0
        try:
            r = fn(**kwargs)
            return int(r or 0)
        except Exception:
            logger.exception("[Retention] call failed: %s(%s)", method_name, kwargs)
            return 0

    def _log_section(self, name: str, deleted: int) -> None:
        if deleted > 0:
            logger.info("[Retention] %s deleted=%d", name, deleted)
        elif self.log_each_cycle and self.log_zero_deletes:
            logger.info("[Retention] %s deleted=0", name)

    def _run_once(self) -> None:
        deleted_total = 0

        # =====================================================
        # candles
        # =====================================================
        deleted_candles = 0
        if self.candles_days > 0 and self.candle_intervals:
            for itv in self.candle_intervals:
                deleted_candles += self._safe_call(
                    "cleanup_candles",
                    exchange_id=self.exchange_id,
                    interval=str(itv),
                    keep_days=self.candles_days,
                )
        deleted_total += deleted_candles
        self._log_section("candles", deleted_candles)

        # =====================================================
        # open_interest
        # =====================================================
        deleted_oi = 0
        for itv, keep_days in (self.oi_keep_days or {}).items():
            if int(keep_days) <= 0:
                continue
            deleted_oi += self._safe_call(
                "cleanup_open_interest",
                exchange_id=self.exchange_id,
                interval=str(itv),
                keep_days=int(keep_days),
            )
        deleted_total += deleted_oi
        self._log_section("open_interest", deleted_oi)

        # =====================================================
        # funding
        # =====================================================
        deleted_funding = 0
        if self.funding_days > 0:
            deleted_funding = self._safe_call(
                "cleanup_funding",
                exchange_id=self.exchange_id,
                keep_days=self.funding_days,
            )
        deleted_total += deleted_funding
        self._log_section("funding", deleted_funding)

        # =====================================================
        # ticker_24h
        # =====================================================
        deleted_t24 = 0
        if self.ticker_24h_days > 0:
            deleted_t24 = self._safe_call(
                "cleanup_ticker_24h",
                exchange_id=self.exchange_id,
                keep_days=self.ticker_24h_days,
            )
        deleted_total += deleted_t24
        self._log_section("ticker_24h", deleted_t24)

        # =====================================================
        # market_state_5m
        # =====================================================
        deleted_ms5m = 0
        if self.market_state_5m_days > 0:
            deleted_ms5m = self._safe_call(
                "cleanup_market_state_5m",
                exchange_id=self.exchange_id,
                keep_days=self.market_state_5m_days,
            )
        deleted_total += deleted_ms5m
        self._log_section(f"market_state_5m(keep_days={self.market_state_5m_days})", deleted_ms5m)

        # =====================================================
        # ✅ NEW: market_trades
        # =====================================================
        deleted_mtr = 0
        if self.market_trades_days > 0:
            deleted_mtr = self._safe_call(
                "cleanup_market_trades",
                exchange_id=self.exchange_id,
                keep_days=self.market_trades_days,
            )
        deleted_total += deleted_mtr
        if callable(getattr(self.storage, "cleanup_market_trades", None)):
            self._log_section(f"market_trades(keep_days={self.market_trades_days})", deleted_mtr)

        # =====================================================
        # ✅ NEW: candles_trades_agg
        # =====================================================
        deleted_cta = 0
        if self.candles_trades_agg_days > 0:
            deleted_cta = self._safe_call(
                "cleanup_candles_trades_agg",
                exchange_id=self.exchange_id,
                keep_days=self.candles_trades_agg_days,
            )
        deleted_total += deleted_cta
        if callable(getattr(self.storage, "cleanup_candles_trades_agg", None)):
            self._log_section(f"candles_trades_agg(keep_days={self.candles_trades_agg_days})", deleted_cta)

        # =====================================================
        # trades (опционально, только если метод есть)
        # =====================================================
        deleted_trades = 0
        if self.trade_cleanup_days > 0:
            deleted_trades = self._safe_call(
                "cleanup_trades",
                exchange_id=self.exchange_id,
                keep_days=self.trade_cleanup_days,
            )
        deleted_total += deleted_trades
        if callable(getattr(self.storage, "cleanup_trades", None)):
            self._log_section("trades", deleted_trades)

        # итоговый лог — один раз
        if deleted_total > 0:
            logger.info("[Retention] cleanup done deleted_total=%d", deleted_total)
        elif self.log_each_cycle and self.log_zero_deletes:
            logger.info("[Retention] cleanup done deleted_total=0")
