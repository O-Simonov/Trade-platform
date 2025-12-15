from __future__ import annotations
import json, threading, time
import websocket

WS_BASE = "wss://fstream.binance.com/ws"

class BinanceWS:
    def __init__(self, url: str, on_message, name: str = "BinanceWS"):
        self.url = url
        self.on_message = on_message
        self.name = name
        self._ws = None
        self._t = None
        self._stop = threading.Event()

    def start(self):
        def _run():
            while not self._stop.is_set():
                try:
                    self._ws = websocket.WebSocketApp(
                        self.url,
                        on_message=lambda ws, msg: self._handle(msg),
                        on_error=lambda ws, err: print(f"[{self.name}] WS error: {err}"),
                        on_close=lambda ws, *a: print(f"[{self.name}] WS closed"),
                    )
                    self._ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as e:
                    print(f"[{self.name}] WS exception: {e}")
                time.sleep(2.0)
        self._t = threading.Thread(target=_run, daemon=True, name=self.name)
        self._t.start()

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
        self.on_message(data)
