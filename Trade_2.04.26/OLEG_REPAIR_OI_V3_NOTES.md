# OI/CVD Repair V3

Added `tools/repair_oi_cvd_gaps_v3.py`.

What changed:
- OI repair is now exact-by-timestamp.
- The script first finds missing OI timestamps from `candles LEFT JOIN open_interest`.
- It requests Binance OI history in compact windows around those exact candle timestamps.
- It only upserts rows whose `ts` exactly matches missing candle timestamps.
- It supports `--dry-run`.
- CVD recalculation logic is preserved.

Recommended order:
1. Repair candle gaps.
2. Run `repair_oi_cvd_gaps_v3.py --oi-only ...`.
3. Re-run the SQL health check.
