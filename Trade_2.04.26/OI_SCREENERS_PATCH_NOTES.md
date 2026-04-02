# OI screener patch notes

## Что изменено

### Ускорение
- Добавлена batch-загрузка `candles` по всем символам за один SQL-запрос.
- Добавлена batch-загрузка `open_interest` по всем символам за один SQL-запрос.
- Добавлена batch-загрузка последних двух `funding` по всем символам за один SQL-запрос.
- В `run_screener_oi.py` добавлен простой кэш данных для построения графиков в рамках одного прогона.

### Качество сигналов
- Добавлены фильтры ликвидности:
  - `min_base_quote_volume`
  - `min_oi_value`
- Для BUY добавлены метрики устойчивости OI:
  - `buy_min_oi_last_rise_pct`
  - `max_oi_drawdown_from_peak_pct`
  - `oi_up_ratio`
- Добавлена ATR-нормализация движения цены:
  - `use_atr_normalization`
  - `atr_period`
  - `buy_price_rise_atr_mult`
  - `buy_price_max_rise_atr_mult`
- Улучшен расчёт `confidence` и `score`.
- Добавлен фильтр `min_signal_score`.

## Файлы
- `src/platform/screeners/scr_oi_binance.py`
- `src/platform/run_screener_oi.py`
- `config/screener_oi.yaml`

## Замечания
- Публичный контракт `ScreenerSignal` не менялся.
- Текущий патч старается не ломать существующий pipeline вставки сигналов/Telegram/plotting.
- Для реальной проверки нужен прогон на вашей БД, потому что в контейнере отсутствует runtime-зависимость `psycopg` и доступ к вашей базе.
