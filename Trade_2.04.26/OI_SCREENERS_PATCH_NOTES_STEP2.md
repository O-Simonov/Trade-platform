# OI screener patch — step 2

## Что внедрено

### 1) BUY-профили `EARLY` и `CONFIRMED`
Скринер по-прежнему пишет в signals `side="BUY"`, но теперь помечает профиль сигнала:
- `context.signal_profile = "EARLY" | "CONFIRMED"`
- `context.signal_label = "BUY_EARLY" | "BUY_CONFIRMED"`
- в Telegram/тексте теперь выводится метка профиля

Логика:
- `CONFIRMED` — строгие условия как основной BUY
- `EARLY` — допускает немного более ранний вход по цене/объёму/свечам, но не ослабляет OI до мусорного уровня
- на один символ/момент времени создаётся максимум один BUY, приоритет у `CONFIRMED`

### 2) Батч-выделение `day_seq`
Вместо вызова `next_signal_seq(...)` на каждый новый сигнал:
- сигналы группируются по `(symbol_id, signal_day)`
- sequence выделяется одним batch upsert в `signals_day_seq`
- затем диапазоны sequence раскладываются по строкам в памяти

Это уменьшает количество roundtrip'ов к БД при массовой записи сигналов.

### 3) Конфиг под 3 ТФ + EARLY/CONFIRMED
Обновлён `config/screener_oi.yaml`:
- `intervals: ["5m", "15m", "1h"]`
- overrides для всех трёх ТФ
- новые параметры профилей BUY:
  - `enable_confirmed_buy`
  - `enable_early_buy`
  - `early_buy_min_green_candles`
  - `early_buy_require_last_green`
  - `early_buy_volume_mult_ratio`
  - `early_buy_volume_spike_ratio`
  - `early_buy_price_rise_pct_ratio`
  - `early_buy_price_rise_atr_mult_ratio`
  - `early_buy_min_signal_score`

## Совместимость
- downstream-логика, ожидающая `side in (BUY, SELL)`, сохраняется
- новые данные о профиле приходят через `context`
- при желании позже можно вынести `BUY_EARLY/BUY_CONFIRMED` в отдельный `signal_type`, не ломая текущую схему

## Что дальше логично делать
1. Прогнать live/runtime тест на БД
2. Посмотреть распределение по `signal_profile`
3. Сравнить конверсию `EARLY` vs `CONFIRMED`
4. Если нужно — добавить отдельные SL/TP для `EARLY`
