# src/platform/exchanges/binance/ws.py
from __future__ import annotations

import json
import threading
import time
import logging
from typing import Callable, Optional, Any

import websocket

log = logging.getLogger("binance.ws")

WS_STREAM_BASE = "wss://fstream.binance.com/stream?streams="
WS_WS_BASE = "wss://fstream.binance.com/ws"


class BinanceWS(threading.Thread):
    """
    Простая WS-обёртка с автопереподключением.

    Важно:
      ✅ разворачивает combined streams {"stream": "...", "data": {...}} -> отдаёт payload=data
      ✅ ловит исключения callback-а и логирует (иначе OMS может молча не получать события)
    """

    def __init__(
        self,
        *,
        url: str,
        on_message: Callable[[dict[str, Any]], None],
        name: str = "BinanceWS",
        on_open: Optional[Callable[[], None]] = None,
        on_close: Optional[Callable[[], None]] = None,
        ping_interval: int = 20,
        ping_timeout: int = 10,
        reconnect_delay_sec: float = 2.0,
    ):
        super().__init__(daemon=True, name=name)
        self.url = url
        self.on_message_cb = on_message

        self._ws: websocket.WebSocketApp | None = None
        self._stop = threading.Event()

        self.connected = threading.Event()
        self._on_open_hook = on_open
        self._on_close_hook = on_close

        self._ping_interval = int(ping_interval)
        self._ping_timeout = int(ping_timeout)
        self._reconnect_delay_sec = float(reconnect_delay_sec)

    def run(self) -> None:
        log.info("[%s] connecting → %s", self.name, self.url)

        def _on_open(_ws):
            self.connected.set()
            log.info("[%s] WS CONNECTED", self.name)
            try:
                if self._on_open_hook:
                    self._on_open_hook()
            except Exception:
                log.exception("[%s] on_open hook failed", self.name)

        def _on_close(_ws, *_a):
            self.connected.clear()
            log.warning("[%s] WS CLOSED", self.name)
            try:
                if self._on_close_hook:
                    self._on_close_hook()
            except Exception:
                log.exception("[%s] on_close hook failed", self.name)

        def _on_error(_ws, err):
            # error не всегда означает close, поэтому connected не трогаем тут
            log.error("[%s] WS ERROR: %s", self.name, err)

        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=_on_open,
                    on_message=lambda ws, msg: self._handle(msg),
                    on_error=_on_error,
                    on_close=_on_close,
                )
                self._ws.run_forever(
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                )
            except Exception as e:
                self.connected.clear()
                log.exception("[%s] WS exception: %s", self.name, e)

            # backoff перед переподключением
            if self._stop.is_set():
                break
            time.sleep(self._reconnect_delay_sec)

    def stop(self) -> None:
        self._stop.set()
        self.connected.clear()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def _handle(self, msg: str) -> None:
        try:
            data = json.loads(msg)
        except Exception:
            return

        # combined stream -> {"stream":"...", "data":{...}}
        if isinstance(data, dict) and "data" in data and "stream" in data:
            payload = data.get("data")
        else:
            payload = data

        if not isinstance(payload, dict):
            return

        # 🔥 ключевой фикс: ошибки обработчика не должны быть "тихими"
        try:
            self.on_message_cb(payload)
        except Exception:
            ev = payload.get("e") or payload.get("eventType") or "?"
            # обрезаем, чтобы лог не раздувался
            snippet = str(payload)
            if len(snippet) > 800:
                snippet = snippet[:800] + "…"
            log.exception("[%s] on_message_cb failed (event=%s) payload=%s", self.name, ev, snippet)
