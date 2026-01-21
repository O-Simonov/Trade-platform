@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM ============================================================
REM Optional: activate venv
REM ============================================================
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

REM ============================================================
REM Prefer venv python explicitly, fallback to system python
REM ============================================================
set "PY=%CD%\.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

"%PY%" -c "import sys; print(sys.executable)" >nul 2>nul
if errorlevel 1 (
  echo [BAT] ERROR: Python not found. Check venv or PATH.
  pause
  exit /b 9009
)

REM ============================================================
REM MODE: child supervisor (ONE)
REM Call: this.bat ONE FAST "cfgpath" "logdir" "suplog"
REM ============================================================
if /I "%~1"=="ONE" goto SUPERVISE_ONE

REM ============================================================
REM Parent mode: start FAST + SLOW
REM ============================================================

set "CFG_FAST=%CD%\config\market_data_fast_5m.yaml"
set "CFG_SLOW=%CD%\config\market_data_slow.yaml"

if not exist "%CFG_FAST%" (
  echo [BAT] ERROR: FAST config not found: %CFG_FAST%
  pause
  exit /b 2
)
if not exist "%CFG_SLOW%" (
  echo [BAT] ERROR: SLOW config not found: %CFG_SLOW%
  pause
  exit /b 2
)

REM Optional REST debug flags (harmless)
set "BINANCE_REST_DEBUG_HEADERS=1"
set "BINANCE_REST_CALLSITE=1"
set "BINANCE_REST_COUNTERS=1"

set "LOG_DIR=%CD%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "SUP_LOG=%LOG_DIR%\supervisor_dual_%TS%.log"

echo === RUN MARKET DATA DUAL (FAST + SLOW) ===
echo Repo:   %CD%
echo FAST:   %CFG_FAST%
echo SLOW:   %CFG_SLOW%
echo Logs:   %LOG_DIR%
echo Supervisor log: %SUP_LOG%
echo.

echo [BAT] supervisor start %DATE% %TIME%>>"%SUP_LOG%"
echo [BAT] python: %PY%>>"%SUP_LOG%"
echo [BAT] FAST cfg: %CFG_FAST%>>"%SUP_LOG%"
echo [BAT] SLOW cfg: %CFG_SLOW%>>"%SUP_LOG%"

REM Start FAST child
echo [BAT] starting FAST child...>>"%SUP_LOG%"
start "MarketData FAST" /B cmd /C call "%~f0" ONE FAST "%CFG_FAST%" "%LOG_DIR%" "%SUP_LOG%"

REM Start SLOW child
echo [BAT] starting SLOW child...>>"%SUP_LOG%"
start "MarketData SLOW" /B cmd /C call "%~f0" ONE SLOW "%CFG_SLOW%" "%LOG_DIR%" "%SUP_LOG%"

echo [BAT] FAST+SLOW started (background). Press Ctrl+C to stop this window.
echo.

:WAIT_MAIN
timeout /t 2 /nobreak >nul
goto WAIT_MAIN


REM ============================================================
REM Child mode: supervise ONE config forever with auto-restart
REM ============================================================
:SUPERVISE_ONE
setlocal EnableExtensions EnableDelayedExpansion

set "NAME=%~2"
set "CFG=%~3"
set "LOG_DIR=%~4"
set "SUP_LOG=%~5"

set "RESTART_DELAY_SEC=5"
set /a "RUN=0"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ------------------------------------------------------------
REM Ensure per-process config env
REM ------------------------------------------------------------
set "MARKETDATA_CONFIG=%CFG%"

REM If you set PG_DSN in PowerShell earlier, both children inherit it.
REM Override here so singleton lock & pg stats don't collide.
REM (You can also keep your base PG_DSN parts in one place.)
set "PG_DSN=host=localhost port=5432 dbname=trade_platform user=postgres sslmode=prefer connect_timeout=10 application_name=market_data_%NAME%"

REM Make sure YAML rest_limits apply even if env vars existed before
set "BINANCE_REST_MAX_RPS="
set "BINANCE_REST_MAX_RPM="

:loop_one
set /a "RUN+=1"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "RUN_TS=%%i"
set "RUN_LOG=%LOG_DIR%\run_market_data_%NAME%_%RUN_TS%_run!RUN!.log"

echo.>>"%SUP_LOG%"
echo ==================================================>>"%SUP_LOG%"
echo [BAT][%NAME%] run #!RUN! start %DATE% %TIME%>>"%SUP_LOG%"
echo [BAT][%NAME%] cfg: %CFG%>>"%SUP_LOG%"
echo [BAT][%NAME%] PG_DSN: %PG_DSN%>>"%SUP_LOG%"
echo [BAT][%NAME%] run log: %RUN_LOG%>>"%SUP_LOG%"
echo ==================================================>>"%SUP_LOG%"

echo [BAT][%NAME%] launching python...>>"%SUP_LOG%"
"%PY%" -u -m src.platform.run_market_data 1>>"%RUN_LOG%" 2>>&1
set "ERR=!ERRORLEVEL!"

echo [BAT][%NAME%] exit code=!ERR! %DATE% %TIME%>>"%SUP_LOG%"

REM stop conditions
if "!ERR!"=="0" exit /b 0
if "!ERR!"=="3221225786" exit /b !ERR!
if "!ERR!"=="130" exit /b !ERR!

echo [BAT][%NAME%] crash -> restart in %RESTART_DELAY_SEC%s (code=!ERR!)>>"%SUP_LOG%"
timeout /t %RESTART_DELAY_SEC% /nobreak >nul
goto loop_one
