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
REM Load .env from repo root
REM ============================================================
set "ENV_FILE=%CD%\.env"
if exist "%ENV_FILE%" (
  call :LOAD_DOTENV "%ENV_FILE%"
) else (
  echo [BAT] WARN: .env not found: %ENV_FILE%
)

REM Defaults if not provided in .env
if not defined ENVIRONMENT set "ENVIRONMENT=local"
if not defined LOG_LEVEL set "LOG_LEVEL=INFO"

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

REM Optional REST debug flags
if not defined BINANCE_REST_DEBUG_HEADERS set "BINANCE_REST_DEBUG_HEADERS=1"
if not defined BINANCE_REST_CALLSITE set "BINANCE_REST_CALLSITE=1"
if not defined BINANCE_REST_COUNTERS set "BINANCE_REST_COUNTERS=1"

set "LOG_DIR=%CD%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "SUP_LOG=%LOG_DIR%\supervisor_dual_%TS%.log"

echo === RUN MARKET DATA DUAL (FAST + SLOW) ===
echo Repo:   %CD%
echo ENV:    %ENV_FILE%
echo FAST:   %CFG_FAST%
echo SLOW:   %CFG_SLOW%
echo Logs:   %LOG_DIR%
echo Supervisor log: %SUP_LOG%
echo ENVIRONMENT: %ENVIRONMENT%
echo LOG_LEVEL: %LOG_LEVEL%
if defined PG_DSN (
  echo PG_DSN: defined
) else (
  echo PG_DSN: NOT DEFINED
)
echo.

echo [BAT] supervisor start %DATE% %TIME%>>"%SUP_LOG%"
echo [BAT] python: %PY%>>"%SUP_LOG%"
echo [BAT] env file: %ENV_FILE%>>"%SUP_LOG%"
echo [BAT] FAST cfg: %CFG_FAST%>>"%SUP_LOG%"
echo [BAT] SLOW cfg: %CFG_SLOW%>>"%SUP_LOG%"
echo [BAT] ENVIRONMENT: %ENVIRONMENT%>>"%SUP_LOG%"
echo [BAT] LOG_LEVEL: %LOG_LEVEL%>>"%SUP_LOG%"
if defined PG_DSN (
  echo [BAT] PG_DSN: defined>>"%SUP_LOG%"
) else (
  echo [BAT] PG_DSN: NOT DEFINED>>"%SUP_LOG%"
)

REM Start FAST child
echo [BAT] starting FAST child...>>"%SUP_LOG%"
start "MarketData FAST" /B cmd /C call "%~f0" ONE FAST "%CFG_FAST%" "%LOG_DIR%" "%SUP_LOG%"

REM Start SLOW child
echo [BAT] starting SLOW child...>>"%SUP_LOG%"
start "MarketData SLOW" /B cmd /C call "%~f0" ONE SLOW "%CFG_SLOW%" "%LOG_DIR%" "%SUP_LOG%"

echo [BAT] FAST+SLOW started in background. Press Ctrl+C to stop this window.
echo.

:WAIT_MAIN
timeout /t 2 /nobreak >nul
goto WAIT_MAIN


REM ============================================================
REM Child mode: supervise ONE config forever with auto-restart
REM ============================================================
:SUPERVISE_ONE
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM Re-load .env inside child process too
set "ENV_FILE=%CD%\.env"
if exist "%ENV_FILE%" (
  call :LOAD_DOTENV "%ENV_FILE%"
)

set "NAME=%~2"
set "CFG=%~3"
set "LOG_DIR=%~4"
set "SUP_LOG=%~5"

set "RESTART_DELAY_SEC=5"
set /a "RUN=0"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Required per-process env
set "MARKETDATA_CONFIG=%CFG%"
set "MARKETDATA_INSTANCE=%NAME%"

if not defined ENVIRONMENT set "ENVIRONMENT=local"
if not defined LOG_LEVEL set "LOG_LEVEL=INFO"

REM Add application_name only if missing
if defined PG_DSN (
  echo %PG_DSN% | find /I "application_name=" >nul
  if errorlevel 1 (
    set "PG_DSN=%PG_DSN% application_name=market_data_%NAME%"
  )
)

REM Allow YAML rest_limits to take effect if your Python code uses env override logic
set "BINANCE_REST_MAX_RPS="
set "BINANCE_REST_MAX_RPM="

:LOOP_ONE
set /a "RUN+=1"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "RUN_TS=%%i"
set "RUN_LOG=%LOG_DIR%\run_market_data_%NAME%_%RUN_TS%_run!RUN!.log"

echo.>>"%SUP_LOG%"
echo ==================================================>>"%SUP_LOG%"
echo [BAT][%NAME%] run #!RUN! start %DATE% %TIME%>>"%SUP_LOG%"
echo [BAT][%NAME%] instance: %MARKETDATA_INSTANCE%>>"%SUP_LOG%"
echo [BAT][%NAME%] cfg: %MARKETDATA_CONFIG%>>"%SUP_LOG%"
echo [BAT][%NAME%] environment: %ENVIRONMENT%>>"%SUP_LOG%"
echo [BAT][%NAME%] log_level: %LOG_LEVEL%>>"%SUP_LOG%"
if defined PG_DSN (
  echo [BAT][%NAME%] PG_DSN: defined>>"%SUP_LOG%"
) else (
  echo [BAT][%NAME%] PG_DSN: NOT DEFINED>>"%SUP_LOG%"
)
echo [BAT][%NAME%] run log: %RUN_LOG%>>"%SUP_LOG%"
echo ==================================================>>"%SUP_LOG%"

echo [BAT][%NAME%] launching python...>>"%SUP_LOG%"
"%PY%" -u -m src.platform.run_market_data 1>>"%RUN_LOG%" 2>>&1
set "ERR=!ERRORLEVEL!"

echo [BAT][%NAME%] exit code=!ERR! %DATE% %TIME%>>"%SUP_LOG%"

if "!ERR!"=="0" exit /b 0
if "!ERR!"=="3221225786" exit /b !ERR!
if "!ERR!"=="130" exit /b !ERR!

echo [BAT][%NAME%] crash -> restart in %RESTART_DELAY_SEC%s (code=!ERR!)>>"%SUP_LOG%"
timeout /t %RESTART_DELAY_SEC% /nobreak >nul
goto LOOP_ONE


REM ============================================================
REM Helpers
REM ============================================================
:LOAD_DOTENV
set "DOTENV_FILE=%~1"
if not exist "%DOTENV_FILE%" goto :eof

for /f "usebackq tokens=* delims=" %%L in ("%DOTENV_FILE%") do (
  set "LINE=%%L"
  call :PROCESS_ENV_LINE
)
goto :eof


:PROCESS_ENV_LINE
set "RAW=!LINE!"

if not defined RAW goto :eof

REM Trim leading spaces
for /f "tokens=* delims= " %%A in ("!RAW!") do set "RAW=%%A"

if not defined RAW goto :eof
if "!RAW:~0,1!"=="#" goto :eof

REM Skip lines without "="
echo(!RAW! | find "=" >nul
if errorlevel 1 goto :eof

for /f "tokens=1* delims==" %%A in ("!RAW!") do (
  set "KEY=%%A"
  set "VAL=%%B"
)

REM Trim spaces around KEY
for /f "tokens=* delims= " %%A in ("!KEY!") do set "KEY=%%A"

REM Remove optional surrounding quotes from value
if defined VAL (
  if "!VAL:~0,1!"=="^"" set "VAL=!VAL:~1!"
  if "!VAL:~-1!"=="^"" set "VAL=!VAL:~0,-1!"
)

set "!KEY!=!VAL!"
goto :eof
