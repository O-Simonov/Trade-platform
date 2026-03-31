# OI flow guard patch

Included in this archive:
- pump flow reads only confirmed OI/CVD bars at exact candle timestamps
- latest unconfirmed OI bar is ignored for flow calculations
- OI plot no longer paints missing values as zero

Main files changed:
- src/platform/screeners/scr_pump_binance.py
- src/platform/screeners/plotting_pump.py
