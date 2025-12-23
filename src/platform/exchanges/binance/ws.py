# src/platform/exchanges/binance/ws.py
from __future__ import annotations

import json
import threading
import time
import logging
import websocket

log = logging.getLogger("binance.ws")

WS_STREAM_BASE = "wss://fstream.binance.com/stream?streams="
WS_WS_BASE = "wss://fstream.binance.com/ws"


class BinanceWS(threading.Thread):
    def __init__(self, *, url: str, on_message, name: str = "BinanceWS"):
        super().__init__(daemon=True, name=name)
        self.url = url
        self.on_message_cb = on_message
        self._ws: websocket.WebSocketApp | None = None
        self._stop = threading.Event()

    def run(self):
        log.info("[%s] connecting → %s", self.name, self.url)

        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=lambda ws: log.info("[%s] WS CONNECTED", self.name),
                    on_message=lambda ws, msg: self._handle(msg),
                    on_error=lambda ws, err: log.error("[%s] WS ERROR: %s", self.name, err),
                    on_close=lambda ws, *a: log.warning("[%s] WS CLOSED", self.name),
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.exception("[%s] WS exception: %s", self.name, e)

            # небольшой backoff
            for _ in range(10):
                if self._stop.is_set():
                    break
                time.sleep(0.2)

    def stop(self):
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def _handle(self, msg: str):
        try:
            data = json.loads(msg)
        except Exception:
            return

        # combined stream -> {"stream":"...", "data":{...}}
        if isinstance(data, dict) and "data" in data and "stream" in data:
            payload = data.get("data")
        else:
            payload = data

        if payload is None:
            return

        self.on_message_cb(payload)
