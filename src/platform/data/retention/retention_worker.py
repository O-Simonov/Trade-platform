import time
import threading
import logging

logger = logging.getLogger(__name__)

class RetentionWorker(threading.Thread):
    def __init__(self, *, storage, exchange_id: int, cfg: dict, run_sec: int = 3600):
        super().__init__(daemon=True, name="RetentionWorker")
        self.storage = storage
        self.exchange_id = exchange_id
        self.cfg = cfg
        self.run_sec = run_sec
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        logger.info("RetentionWorker started")
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                logger.exception("Retention error: %s", e)
            time.sleep(self.run_sec)

    def _run_once(self):
        logger.info("Retention cleanup tick")

        self.storage.cleanup_candles(
            exchange_id=self.exchange_id,
            interval="1m",
            keep_days=self.cfg["candles_days"],
        )
        self.storage.cleanup_candles(
            exchange_id=self.exchange_id,
            interval="5m",
            keep_days=self.cfg["oi_5m_days"],
        )
