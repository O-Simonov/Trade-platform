# OI collector optimization patch

Что изменено:
- добавлен встроенный repair-цикл для добивки пропущенных OI-точек по timestamps свечей
- нормализация ts OI по сетке интервала перед upsert
- ускорен и выровнен poll/catch-up режим
- включён seed_on_start для OI
- добавлен repair-блок в config/open_interest
- исправлено хранение retention для 5m/15m/1h
- candle_intervals для market_data переведены на только 15m, как и указано в комментариях

Рекомендуемый запуск:
$env:MARKET_DATA_CONFIG="config/market_data_slow.yaml"
python -m src.platform.run_market_data
