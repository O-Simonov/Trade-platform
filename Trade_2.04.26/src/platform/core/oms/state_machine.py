# src/platform/core/oms/state_machine.py
from __future__ import annotations

from dataclasses import dataclass


TERMINAL: set[str] = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


def status_rank(status: str | None) -> int:
    """Monotonic rank for order status."""
    if not status:
        return 0
    s = str(status).upper()
    mapping = {
        "PENDING": 5,
        "NEW": 10,
        "PARTIALLY_FILLED": 20,
        "FILLED": 90,
        "CANCELED": 90,
        "REJECTED": 90,
        "EXPIRED": 90,
    }
    return mapping.get(s, 0)


@dataclass(frozen=True)
class Decision:
    allow: bool
    reason: str = ""


def should_apply(current_status: str | None, incoming_status: str | None) -> Decision:
    """
    Allow only monotonic status updates.
    - terminal status never regresses
    - otherwise compare status_rank
    """
    cur = (current_status or "").upper()
    inc = (incoming_status or "").upper()

    if not inc:
        return Decision(False, "incoming_status is empty")

    if cur in TERMINAL and inc != cur:
        return Decision(False, f"terminal regression blocked: {cur} -> {inc}")

    if status_rank(inc) < status_rank(cur):
        return Decision(False, f"rank regression blocked: {cur} -> {inc}")

    return Decision(True, "ok")
