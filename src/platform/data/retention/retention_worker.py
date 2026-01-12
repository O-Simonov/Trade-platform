# src/platform/data/retention/retention_worker.py
from __future__ import annotations

import logging
import threading

logger = logging.getLogger("src.platform.data.retention.retention_worker")

DEFAULT_CANDLE_INTERVALS = ("1m", "5m", "1h", "4h", "1d")

class RetentionWorker(threading.Thread):
    """
    Periodic DB cleanup (safe):
      - candles by candle_intervals + candles_days
      - open_interest by oi_*_days
      - funding by funding_days
      - ticker_24h by ticker_24h_days
      - trades by trade_cleanup_days (new feature)
    """

    def __init__(self, *, storage, exchange_id: int, cfg: dict, run_sec: int = 3600):
        super().__init__(daemon=True, name="RetentionWorker")
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.cfg = cfg or {}
        self.run_sec = int(run_sec or 3600)
        self._stop = threading.Event()

        # candles
        self.candles_days = int(self.cfg.get("candles_days", 180))
        raw_intervals = self.cfg.get("candle_intervals") or list(DEFAULT_CANDLE_INTERVALS)
        self.candle_intervals = [str(x).strip() for x in raw_intervals if str(x).strip()]

        # open_interest
        self.oi_keep_days = {
            "5m": int(self.cfg.get("oi_5m_days", 90)),
            "15m": int(self.cfg.get("oi_15m_days", 180)),
            "1h": int(self.cfg.get("oi_1h_days", 365)),
        }

        # funding
        self.funding_days = int(self.cfg.get("funding_days", 365))

        # ticker_24h
        self.ticker_24h_days = int(self.cfg.get("ticker_24h_days", 90))

        # trade cleanup (new)
        self.trade_cleanup_days = int(self.cfg.get("trade_cleanup_days", 180))

        # Отключаем логи на старте
        # logger.info(
        #     "RetentionWorker started run_sec=%s candles=%s oi=%s funding=%s ticker_24h=%s trades=%s",
        #     self.run_sec,
        #     True,
        #     True,
        #     True,
        #     True,
        #     True,
        # )

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception:
                logger.exception("Retention error")
            self._stop.wait(self.run_sec)

    def _safe_call(self, method_name: str, **kwargs) -> int:
        fn = getattr(self.storage, method_name, None)
        if fn is None:
            return 0
        try:
            r = fn(**kwargs)
            return int(r or 0)
        except Exception:
            logger.exception("Retention call failed: %s(%s)", method_name, kwargs)
            return 0

    def _run_once(self) -> None:
        # Отключаем лог о каждом цикле очистки
        # logger.info("Retention cleanup tick")

        # candles
        for itv in self.candle_intervals:
            deleted = self._safe_call(
                "cleanup_candles",
                exchange_id=self.exchange_id,
                interval=str(itv),
                keep_days=self.candles_days,
            )
            # Отключаем лог о каждом удалении
            # logger.info(
            #     "[Retention] candles cleaned interval=%s keep_days=%d deleted=%d",
            #     itv,
            #     self.candles_days,
            #     deleted,
            # )

        # open_interest
        for itv, keep_days in self.oi_keep_days.items():
            if keep_days <= 0:
                continue
            deleted = self._safe_call(
                "cleanup_open_interest",
                exchange_id=self.exchange_id,
                interval=str(itv),
                keep_days=int(keep_days),
            )
            # Отключаем лог о каждом удалении
            # logger.info(
            #     "[Retention] open_interest cleaned interval=%s keep_days=%d deleted=%d",
            #     itv,
            #     keep_days,
            #     deleted,
            # )

        # funding
        if self.funding_days > 0:
            deleted = self._safe_call(
                "cleanup_funding",
                exchange_id=self.exchange_id,
                keep_days=self.funding_days,
            )
            # Отключаем лог о каждом удалении
            # logger.info("[Retention] funding cleaned keep_days=%d deleted=%d", self.funding_days, deleted)

        # ticker_24h
        if self.ticker_24h_days > 0:
            deleted = self._safe_call(
                "cleanup_ticker_24h",
                exchange_id=self.exchange_id,
                keep_days=self.ticker_24h_days,
            )
            # Отключаем лог о каждом удалении
            # logger.info("[Retention] ticker_24h cleaned keep_days=%d deleted=%d", self.ticker_24h_days, deleted)

        # Trades (new feature)
        if self.trade_cleanup_days > 0:
            deleted = self._safe_call(
                "cleanup_trades",
                exchange_id=self.exchange_id,
                keep_days=self.trade_cleanup_days,
            )
            # Отключаем лог о каждом удалении
            # logger.info("[Retention] trades cleaned keep_days=%d deleted=%d", self.trade_cleanup_days, deleted)
