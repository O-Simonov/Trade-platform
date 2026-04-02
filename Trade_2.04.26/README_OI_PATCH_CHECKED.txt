ПРОВЕРКА OI PATCH
=================

Что проверено:
- src/platform/screeners/scr_oi_binance.py присутствует
- src/platform/run_screener_oi.py присутствует
- config/screener_oi.yaml присутствует
- есть replay_as_of_ts
- есть signal_profile / signal_label
- есть enable_early_buy / enable_confirmed_buy
- есть intervals: ["5m", "15m", "1h"]
- py_compile для ключевых файлов проходит

Итог:
- предыдущие недостающие части уже есть в текущем рабочем проекте
- опасную автозамену кода я не делал, потому что это могло бы затереть более свежую рабочую логику
- архив ниже можно распаковывать поверх проекта как готовую проверенную копию
