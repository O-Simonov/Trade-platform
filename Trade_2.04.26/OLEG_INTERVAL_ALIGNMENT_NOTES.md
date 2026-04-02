# Interval alignment fix

Applied for pump screener:

- OI/CVD calculations now use only the same DB interval as the signal candles.
- Removed fallback mixing between 5m / 15m / 1h / 4h inside flow calculations.
- Plotting now fetches OI/CVD with the same interval and the same lookback/lookforward in bars as candles.
- If `oi_interval` / `cvd_interval` are present in YAML for pump plots, runtime ignores them and logs that candle interval is enforced.

Result:

- calculations and plots are aligned to one timeframe
- any missing lower-panel history now means missing data in DB for that exact interval
- diagnostics `oi_interval_used` / `cvd_interval_used` should now always equal signal interval
