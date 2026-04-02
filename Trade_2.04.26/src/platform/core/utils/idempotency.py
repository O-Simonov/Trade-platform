from __future__ import annotations
import hashlib

def make_client_order_id(*parts: str, max_len: int = 32) -> str:
    raw = "|".join(p for p in parts if p is not None and p != "")
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return h[:max_len]
