# PUMP config audit after cleanup

Из `config/screener_pump.yaml` удалены параметры, которые сейчас не влияют на runtime-поведение pump runner.

Удалены из YAML:
- `telegram_send_plot_followup_text`
- `alerts.send_only_new`
- `alerts.include_score`
- `alerts.include_context_stats`
- `alerts.include_interval`
- `alerts.include_profile`
- `alerts.include_debug_metrics`
- `alerts.max_alerts_per_cycle`
- `plots.plot_only_for_sent_signals`
- `plots.plot_only_top_scored`
- `plots.plot_top_n`
- `plots.include_oi_panel`
- `plots.include_cvd_panel`
- `plots.include_volume_panel`
- `plots.include_funding_annotation`
- `plots.image_format`
- `plots.dpi`
- `plots.compress_images`

Они всё ещё могут существовать в dataclass / модели конфига как задел на будущее, но в текущем pump runtime их значения не определяют фактическое поведение отправки алертов и построения графиков.

Сохранены в YAML только те legacy и nested параметры, которые сейчас реально участвуют в работе скринера, графиков, Telegram и записи сигналов.
