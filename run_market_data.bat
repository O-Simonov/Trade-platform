@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM --- activate venv (optional, but ok) ---
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

REM --- prefer venv python explicitly ---
set "PY=%CD%\.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

REM --- config ---
set "MARKETDATA_CONFIG=%CD%\config\market_data.yaml"

REM --- REST debug flags (match rest.py) ---
set "BINANCE_REST_DEBUG_HEADERS=1"
REM optional envs (only if your code uses them; harmless otherwise):
set "BINANCE_REST_CALLSITE=1"
set "BINANCE_REST_COUNTERS=1"

REM --- logs ---
set "LOG_DIR=%CD%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "LOG_FILE=%LOG_DIR%\run_market_data_%TS%.log"

set "RESTART_DELAY_SEC=5"
set /a "RUN=0"

echo === RUN MARKET DATA (AUTO-RESTART) ===
echo Repo:   %CD%
echo Config: %MARKETDATA_CONFIG%
echo Log:    %LOG_FILE%
echo.

:loop
set /a "RUN+=1"
echo.>>"%LOG_FILE%"
echo ==================================================>>"%LOG_FILE%"
echo [BAT] run #!RUN! start %DATE% %TIME%>>"%LOG_FILE%"
echo ==================================================>>"%LOG_FILE%"

echo [BAT] run #!RUN! -> starting python...
"%PY%" -u -m src.platform.run_market_data 1>>"%LOG_FILE%" 2>>&1
set "ERR=%ERRORLEVEL%"

echo [BAT] run #!RUN! exit code=!ERR! %DATE% %TIME%>>"%LOG_FILE%"
echo [BAT] run #!RUN! exited code=!ERR!

REM --- stop conditions ---
if "!ERR!"=="0" goto done

REM Ctrl+C / terminated (often 3221225786 = 0xC000013A)
if "!ERR!"=="3221225786" goto done
if "!ERR!"=="130" goto done

echo [BAT] crash detected -> restart in %RESTART_DELAY_SEC%s ...
timeout /t %RESTART_DELAY_SEC% /nobreak >nul
goto loop

:done
echo [BAT] stop supervisor (exit code=!ERR!)
echo Log saved: %LOG_FILE%
pause
exit /b 0
