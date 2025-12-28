# src/platform/core/engine/runner.py
from __future__ import annotations

import threading


def run_instances(instances) -> None:
    threads: list[threading.Thread] = []

    for inst in instances:
        ex_name = getattr(inst.exchange, "name", inst.exchange.__class__.__name__)
        strat_id = getattr(getattr(inst, "strategy", None), "strategy_id", "strategy")
        t = threading.Thread(
            target=inst.run,
            daemon=True,
            name=f"Instance-{ex_name}-{inst.account}-{strat_id}",
        )
        t.start()
        threads.append(t)

    # main thread waits forever
    for t in threads:
        t.join()
