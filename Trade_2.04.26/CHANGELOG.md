# Changelog

## trade_liquidation updates

### Main position management
- Disabled full main TP placement when `main_partial_tp_enabled=true`
- Added qty-aware main trailing reissue after partial TP and other quantity changes
- Allowed main trailing placement before all averaging adds are completed
- Kept stop-loss deferral logic separate from main trailing placement

### Averaging logic
- Implemented real support for `averaging_levels_method=pivot|combined`
- Unified averaging level selection across entry, candidate selection, and order maintenance
- `combined` mode now uses pivot structure with volume confirmation

### Hedge protection
- Fixed hedge adoption for already-open exchange hedge positions
- Added automatic creation/recovery of `live_hedge` and `hedge_links`
- Restored live placement of `HEDGE_SL` and `HEDGE_TRL` after hedge binding recovery
- Added hedge binding logs to simplify live diagnostics

### Signal intake and config
- Increased `max_signal_age_minutes` to reduce missed valid `NEW` signals
- Added explicit config for main trailing qty reissue
- Updated YAML comments to reflect actual runtime behavior
- Updated hedge unwind recommendations to avoid step/full-exit overlap
