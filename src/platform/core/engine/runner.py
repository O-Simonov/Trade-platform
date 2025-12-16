from __future__ import annotations
import threading
from src.platform.core.engine.instance import TradingInstance

def run_instances(instances: list[TradingInstance]) -> None:
    threads = []
    for inst in instances:
        t = threading.Thread(target=inst.run, daemon=True, name=f"Instance-{inst.ex.name}-{inst.account}-{inst.strategy.strategy_id}")
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
