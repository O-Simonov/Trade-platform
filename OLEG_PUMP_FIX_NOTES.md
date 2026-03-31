# Oleg Pump Screener Fix Notes

В архиве внесены рабочие правки для pump-скринера:

- Исправлен `trend_mode: soft`: теперь это действительно мягкий режим, а не скрытый hard reject.
- Сохранена совместимость с legacy-значениями `oi/cvd/both`.
- BUY `early` больше не требует одновременного жёсткого подтверждения и по OI, и по CVD.
- BUY `confirmed` по-прежнему требует полного flow-подтверждения.
- Добавлены подробные debug-логи `[PUMP][FLOW_FAIL]` с фактическими OI/CVD/score/trend данными.
- Смягчены BUY-пороги в `config/screener_pump.yaml`, чтобы скринер перестал постоянно выдавать `flow_ok=0`.

Рекомендуемый запуск в PowerShell:

```powershell
$env:PUMP_SCREENERS_CONFIG="config/screener_pump.yaml"
$env:LOG_LEVEL="INFO"
python -m src.platform.run_screener_pump
```

Что ожидается после правки:

- `trend_mode: soft` больше не режет кандидатов на этапе trend.
- Для BUY появятся `early` сигналы при частичном flow-подтверждении.
- В логах станет ясно, чего не хватает кандидату: OI, CVD, score или только полного подтверждения.
