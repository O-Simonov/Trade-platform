# Pump screener fixes applied

## What was improved
- Added **batched preloading** of candles and latest open interest in `scr_pump_binance.py`.
- Added **batched allocation of `day_seq`** in `run_screener_pump.py` instead of per-signal DB round trips.
- Kept full pipeline for pump screener: **save signals -> save charts -> send charts to Telegram**.
- Added lightweight plot data caches inside one run cycle to avoid duplicate fetches when plotting multiple signals.
- Fixed compatibility between YAML config and code for trend filters:
  - `trend_method: ratio` is normalized to `steps`
  - `trend_mode: soft/hard` is normalized to `both`
  - `trend_mode: off` disables trend filter cleanly

## Expected effect
- Faster screener cycle on large universes.
- Better quality filtering because configured trend filter no longer silently degrades due to incompatible enum values.
- Pump screener now stays closer to the working OI-screener pipeline.

## Files changed
- `src/platform/screeners/scr_pump_binance.py`
- `src/platform/run_screener_pump.py`
- `PUMP_SCREENER_FIX_NOTES.md`
