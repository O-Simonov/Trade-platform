from __future__ import annotations

import os
import logging
from pathlib import Path
from datetime import datetime, timezone

import yaml

from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.data.storage.postgres.pool import create_pool

from src.platform.screeners.scr_liquidation_binance import ScrLiquidationBinance
from src.platform.screeners.plotting import save_signal_plot


log = logging.getLogger("tools.run_screeners")


def _utc_now():
    return datetime.now(timezone.utc)


def _load_cfg() -> dict:
    cfg_path = os.getenv("SCREENERS_CONFIG", "config/screeners.yaml")
    p = Path(cfg_path)
    if not p.exists():
        raise FileNotFoundError(f"SCREENERS_CONFIG not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = _load_cfg()
    items = cfg if isinstance(cfg, list) else cfg.get("screeners", [])
    if not items:
        log.warning("No screeners in config")
        return

    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env not set")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    for item in items:
        if not item.get("enabled", False):
            continue

        name = str(item["name"])
        version = str(item.get("version", "0.1"))
        params = dict(item.get("params") or {})

        if name != "scr_liquidation_binance":
            log.info("Skip screener=%s (runner currently supports only scr_liquidation_binance)", name)
            continue

        exchange_id = 1  # Binance
        interval = str(params.get("interval", "1h"))

        # ✅ register screener in DB
        screener_id = store.ensure_screener(name=name, version=version)

        log.info("Run screener=%s v=%s interval=%s", name, version, interval)

        scr = ScrLiquidationBinance()

        # ✅ main compute
        signals = scr.run(
            storage=store,
            exchange_id=exchange_id,
            interval=interval,
            params=params,
        )

        if not signals:
            log.info("No signals")
            continue

        # ✅ write signals to DB
        rows = []
        for s in signals:
            signal_day = s.signal_ts.date()  # UTC day
            day_seq = store.next_signal_seq(
                exchange_id=exchange_id,
                symbol_id=s.symbol_id,
                screener_id=screener_id,
                signal_day=signal_day,
            )

            rows.append(
                {
                    "exchange_id": exchange_id,
                    "symbol_id": s.symbol_id,
                    "screener_id": screener_id,
                    "timeframe": s.timeframe,
                    "signal_ts": s.signal_ts,
                    "signal_day": signal_day,
                    "day_seq": day_seq,
                    "side": s.side,
                    "status": "NEW",
                    "entry_price": s.entry_price,
                    "exit_price": s.exit_price,
                    "stop_loss": s.stop_loss,
                    "take_profit": s.take_profit,
                    "confidence": s.confidence,
                    "score": s.score,
                    "reason": s.reason,
                    "context": s.context,
                    "source": "screener",
                }
            )

        store.insert_signals(rows)
        log.info("Signals inserted: %d", len(rows))

        # ✅ optional plots
        enable_plots = bool(params.get("enable_plots", False))
        if enable_plots:
            plots_dir = Path(str(params.get("plots_dir", "artifacts/screener_plots")))
            lookback = int(params.get("plot_lookback", 120))
            lookforward = int(params.get("plot_lookforward", 20))

            for s in signals:
                # свечи вокруг сигнала
                candles = store.fetch_candles_window(
                    exchange_id=exchange_id,
                    symbol_id=s.symbol_id,
                    interval=interval,
                    center_ts=s.signal_ts,
                    lookback=lookback,
                    lookforward=lookforward,
                )

                # OI/CVD/Funding окна (опционально)
                enable_oi = bool(params.get("enable_oi", False))
                enable_cvd = bool(params.get("enable_cvd", False))
                enable_funding = bool(params.get("enable_funding", False))

                oi = store.fetch_open_interest_window(exchange_id, s.symbol_id, interval, s.signal_ts, lookback, lookforward) if enable_oi else None
                cvd = store.fetch_cvd_window(exchange_id, s.symbol_id, interval, s.signal_ts, lookback, lookforward) if enable_cvd else None
                funding = store.fetch_funding_window(exchange_id, s.symbol_id, s.signal_ts, lookback, lookforward) if enable_funding else None

                day_folder = plots_dir / name / s.symbol / str(s.signal_ts.date())
                out_png = day_folder / f"{s.symbol}_{interval}_{s.side}_{s.signal_ts.strftime('%Y%m%d_%H%M%S')}.png"

                save_signal_plot(
                    out_path=out_png,
                    symbol=s.symbol,
                    timeframe=interval,
                    candles=candles,
                    signal_ts=s.signal_ts,
                    side=s.side,
                    entry_price=float(s.entry_price),
                    up_level=s.context.get("up_level"),
                    down_level=s.context.get("down_level"),
                    liq_short_usdt=s.context.get("liq_short_usdt"),
                    liq_long_usdt=s.context.get("liq_long_usdt"),
                    enable_oi=enable_oi,
                    oi_series=oi,
                    enable_cvd=enable_cvd,
                    cvd_series=cvd,
                    enable_funding=enable_funding,
                    funding_series=funding,
                )

            log.info("Plots saved into: %s", plots_dir)

    pool.close()


if __name__ == "__main__":
    main()
