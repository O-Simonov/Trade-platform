import math


def round_step(value: float, step: float, *, down: bool = True) -> float:
    if step <= 0:
        return value
    k = value / step
    return math.floor(k) * step if down else math.ceil(k) * step


def normalize_qty(qty: float, *, step: float, min_qty: float | None) -> float:
    q = round_step(abs(qty), step=step, down=True)
    if min_qty is not None and q < min_qty:
        return 0.0
    return q


def normalize_price(price: float, *, tick: float) -> float:
    return round_step(price, step=tick, down=True)
