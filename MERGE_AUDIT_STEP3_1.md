# MERGE AUDIT: step3.1

Собрано на базе `Trade_optim_trigger_fix_v3_optimized_hotfix1_oi_patch_step2.zip` с точечным переносом нужных изменений из `step3`.

## Что сохранено из step2
- batch candles/OI/funding в `scr_oi_binance.py`
- 3 таймфрейма и per-interval overrides в `config/screener_oi.yaml`
- BUY профили `EARLY` / `CONFIRMED`
- score/liquidity/ATR фильтры
- корректная batch-алокация day_seq в `run_screener_oi.py`

## Что перенесено из step3
- historical replay support в `scr_oi_binance.py` через `replay_as_of_ts`
- optimizer `src/platform/backtester/optimize_oi_screener_history.py`
- `OI_OPTIMIZATION_COMMANDS.md`
- launcher scripts: `optimize_oi.ps1`, `run_oi_optimization.sh`, `run_oi_optimization.bat`

## Что сознательно НЕ брали из step3 как источник истины
- `config/screener_oi.yaml` из step3 (там регресс на single interval)
- `run_screener_oi.py` из step3 (там хуже аллокация `day_seq` для нескольких сигналов на один symbol/day)
- `scr_oi_binance.py` из step3 целиком (там пропал batch-fetch и часть step2 логики)

## Проверки
- `python -m py_compile` для ключевых файлов: OK
- `config/screener_oi.yaml` содержит `intervals: ["5m", "15m", "1h"]`
- optimizer использует `replay_as_of_ts`
