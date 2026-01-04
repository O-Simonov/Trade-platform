@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM --- всегда стартуем из корня репо (где лежит этот .bat) ---
cd /d "%~dp0"

REM --- активируем venv если есть ---
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

REM --- путь к конфигу (чтобы не искал src\platform\config\... ) ---
set "MARKETDATA_CONFIG=%CD%\config\market_data.yaml"

REM --- диагностика Binance REST: callsite + headers + counters ---
set "BINANCE_REST_CALLSITE=1"
set "BINANCE_REST_HEADERS=1"
set "BINANCE_REST_COUNTERS=1"

REM (опционально) пороги предупреждений счетчиков (если хочешь)
REM set "BINANCE_REST_WARN_TOTAL_PER_MIN=2000"
REM set "BINANCE_REST_WARN_PATH_PER_MIN=800"

REM (опционально) cooldown при 451 restricted location
REM set "BINANCE_REST_451_COOLDOWN_SEC=900"

REM --- лог в файл ---
set "LOG_DIR=%CD%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "LOG_FILE=%LOG_DIR%\run_market_data_%TS%.log"

echo === RUN MARKET DATA ===
echo Repo: %CD%
echo Config: %MARKETDATA_CONFIG%
echo Log: %LOG_FILE%
echo.

REM --- запуск ---
python -m src.platform.run_market_data 1>>"%LOG_FILE%" 2>>&1
set "ERR=%ERRORLEVEL%"

echo.
echo Exit code: %ERR%
echo Log saved: %LOG_FILE%
pause
exit /b %ERR%
