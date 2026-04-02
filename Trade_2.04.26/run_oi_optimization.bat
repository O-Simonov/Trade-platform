@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM OI screener optimization runner (Windows CMD)
REM ============================================================
REM Usage:
REM   1) Edit BACKTEST_DSN and BACKTEST_POOL_DSN below
REM      or set them before launch.
REM   2) run_oi_optimization.bat quick-5m
REM      run_oi_optimization.bat balanced-15m
REM      run_oi_optimization.bat balanced-1h
REM      run_oi_optimization.bat narrow-15m
REM      run_oi_optimization.bat all-balanced
REM ============================================================

if "%BACKTEST_DSN%"=="" set BACKTEST_DSN=postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME
if "%BACKTEST_POOL_DSN%"=="" set BACKTEST_POOL_DSN=dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432

set YAML_PATH=config/screener_oi.yaml
set EXCHANGE_ID=1
set OUT_DIR=artifacts\opt
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

if "%~1"=="quick-5m" goto quick_5m
if "%~1"=="balanced-5m" goto balanced_5m
if "%~1"=="balanced-15m" goto balanced_15m
if "%~1"=="balanced-1h" goto balanced_1h
if "%~1"=="narrow-15m" goto narrow_15m
if "%~1"=="all-balanced" goto all_balanced

echo Usage: %~nx0 ^<quick-5m^|balanced-5m^|balanced-15m^|balanced-1h^|narrow-15m^|all-balanced^>
exit /b 1

:quick_5m
python -m src.platform.backtester.optimize_oi_screener_history ^
  --yaml "%YAML_PATH%" ^
  --start 2026-02-01T00:00:00+00:00 ^
  --end   2026-03-01T00:00:00+00:00 ^
  --interval 5m ^
  --exchange-id %EXCHANGE_ID% ^
  --min-trades 20 ^
  --top-n 20 ^
  --progress-every 5 ^
  --csv-out "%OUT_DIR%\oi_5m_top.csv"
exit /b %errorlevel%

:balanced_5m
python -m src.platform.backtester.optimize_oi_screener_history ^
  --yaml "%YAML_PATH%" ^
  --start 2026-02-01T00:00:00+00:00 ^
  --end   2026-03-01T00:00:00+00:00 ^
  --interval 5m ^
  --exchange-id %EXCHANGE_ID% ^
  --min-trades 25 ^
  --top-n 30 ^
  --progress-every 5 ^
  --buy-oi-rise-pct "2.0,2.5,3.0" ^
  --oi-value-confirm-pct "1.5,2.0,2.5" ^
  --buy-volume-mult "1.8,2.2,2.6" ^
  --buy-volume-spike-mult "3.0,4.0,5.0" ^
  --buy-price-rise-pct "1.0,1.5,2.0" ^
  --buy-min-green-candles "2,3" ^
  --price-sideways-max-pct "4.0,6.0,8.0" ^
  --price-downtrend-min-pct "0.3,0.5,0.8" ^
  --base-volume-max-mult "3.0,4.0,5.0" ^
  --min-base-quote-volume "150000,250000,400000" ^
  --min-oi-value "500000,1500000,2500000" ^
  --buy-min-oi-last-rise-pct "0.8,1.2,1.5" ^
  --max-oi-drawdown-from-peak-pct "0.8,1.0,1.2" ^
  --min-signal-score "50,55,60" ^
  --use-atr-normalization "true" ^
  --atr-period "14" ^
  --buy-price-rise-atr-mult "0.7,0.8,0.9" ^
  --buy-price-max-rise-atr-mult "1.8,2.2,2.8" ^
  --stop-loss-pct "1.5,2.0,2.5" ^
  --take-profit-pct "4.0,5.0,6.0" ^
  --buy-require-last-green "true" ^
  --enable-funding "true" ^
  --buy-require-funding-decreasing "true,false" ^
  --buy-funding-max-pct "-0.02,-0.01,-0.001,-0.0001" ^
  --csv-out "%OUT_DIR%\oi_5m_balanced.csv"
exit /b %errorlevel%

:balanced_15m
python -m src.platform.backtester.optimize_oi_screener_history ^
  --yaml "%YAML_PATH%" ^
  --start 2026-01-15T00:00:00+00:00 ^
  --end   2026-03-15T00:00:00+00:00 ^
  --interval 15m ^
  --exchange-id %EXCHANGE_ID% ^
  --min-trades 15 ^
  --top-n 25 ^
  --progress-every 5 ^
  --buy-oi-rise-pct "2.0,2.5,3.0" ^
  --oi-value-confirm-pct "1.5,2.0,2.5" ^
  --buy-volume-mult "1.8,2.2,2.6" ^
  --buy-volume-spike-mult "3.0,4.0,5.0" ^
  --min-base-quote-volume "250000,400000,600000" ^
  --min-oi-value "1500000,2500000,4000000" ^
  --buy-min-oi-last-rise-pct "1.2,1.5,1.8" ^
  --max-oi-drawdown-from-peak-pct "0.8,1.0,1.2" ^
  --min-signal-score "55,58,62" ^
  --buy-price-rise-atr-mult "0.7,0.8,0.9" ^
  --buy-price-max-rise-atr-mult "2.0,2.2,2.5" ^
  --csv-out "%OUT_DIR%\oi_15m_balanced.csv"
exit /b %errorlevel%

:balanced_1h
python -m src.platform.backtester.optimize_oi_screener_history ^
  --yaml "%YAML_PATH%" ^
  --start 2025-12-01T00:00:00+00:00 ^
  --end   2026-03-15T00:00:00+00:00 ^
  --interval 1h ^
  --exchange-id %EXCHANGE_ID% ^
  --min-trades 10 ^
  --top-n 25 ^
  --progress-every 5 ^
  --buy-oi-rise-pct "2.0,2.5,3.0,3.5" ^
  --oi-value-confirm-pct "1.5,2.0,2.5,3.0" ^
  --buy-volume-mult "1.8,2.2,2.6" ^
  --buy-volume-spike-mult "3.0,4.0,5.0" ^
  --min-base-quote-volume "400000,800000,1200000" ^
  --min-oi-value "2500000,5000000,8000000" ^
  --buy-min-oi-last-rise-pct "1.5,2.0,2.5" ^
  --max-oi-drawdown-from-peak-pct "1.0,1.2,1.5" ^
  --min-signal-score "55,60,65" ^
  --buy-price-rise-atr-mult "0.8,0.9,1.0" ^
  --buy-price-max-rise-atr-mult "2.2,2.8,3.2" ^
  --csv-out "%OUT_DIR%\oi_1h_balanced.csv"
exit /b %errorlevel%

:narrow_15m
python -m src.platform.backtester.optimize_oi_screener_history ^
  --yaml "%YAML_PATH%" ^
  --start 2026-02-01T00:00:00+00:00 ^
  --end   2026-03-01T00:00:00+00:00 ^
  --interval 15m ^
  --exchange-id %EXCHANGE_ID% ^
  --min-trades 20 ^
  --top-n 20 ^
  --progress-every 5 ^
  --buy-oi-rise-pct "2.2,2.4,2.6" ^
  --oi-value-confirm-pct "1.8,2.0,2.2" ^
  --buy-volume-mult "2.0,2.2,2.4" ^
  --min-base-quote-volume "300000,400000,500000" ^
  --min-oi-value "2000000,2500000,3000000" ^
  --buy-min-oi-last-rise-pct "1.3,1.5,1.7" ^
  --max-oi-drawdown-from-peak-pct "0.9,1.0,1.1" ^
  --min-signal-score "56,58,60" ^
  --buy-price-rise-atr-mult "0.75,0.8,0.85" ^
  --buy-price-max-rise-atr-mult "2.0,2.2,2.4" ^
  --csv-out "%OUT_DIR%\oi_15m_narrow.csv"
exit /b %errorlevel%

:all_balanced
call "%~f0" balanced-5m
if errorlevel 1 exit /b %errorlevel%
call "%~f0" balanced-15m
if errorlevel 1 exit /b %errorlevel%
call "%~f0" balanced-1h
exit /b %errorlevel%
