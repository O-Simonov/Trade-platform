from __future__ import annotations

import json
import threading
import time
import logging
import websocket

logger = logging.getLogger("binance.ws")

# Binance Futures WS base
WS_BASE = "wss://fstream.binance.com/stream?streams="


class BinanceWS(threading.Thread):
    def __init__(self, *, url: str, on_message, name: str = "BinanceWS"):
        super().__init__(daemon=True, name=name)
        self.url = url
        self.on_message_cb = on_message
        self._ws: websocket.WebSocketApp | None = None
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            try:
                logger.info("[%s] connecting → %s", self.name, self.url)

                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                self._ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                )

            except Exception as e:
                logger.exception("[%s] WS exception", self.name)

            # backoff before reconnect
            time.sleep(2.0)

    def stop(self):
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # WS callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws):
        logger.info("[%s] WS CONNECTED", self.name)

    def _on_message(self, ws, msg: str):
        try:
            data = json.loads(msg)
        except Exception:
            logger.warning("[%s] bad json message", self.name)
            return

        try:
            self.on_message_cb(data)
        except Exception:
            logger.exception("[%s] on_message callback error", self.name)

    def _on_error(self, ws, err):
        logger.error("[%s] WS ERROR: %s", self.name, err)

    def _on_close(self, ws, *args):
        logger.warning("[%s] WS CLOSED", self.name)
