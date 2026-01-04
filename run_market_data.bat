@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

set "MARKETDATA_CONFIG=%CD%\config\market_data.yaml"
set "BINANCE_REST_CALLSITE=1"
set "BINANCE_REST_HEADERS=1"
set "BINANCE_REST_COUNTERS=1"

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
echo [BAT] run #!RUN! start %DATE% %TIME%>>"%LOG_FILE%"
echo [BAT] run #!RUN! -> starting python...
python -u -m src.platform.run_market_data 1>>"%LOG_FILE%" 2>>&1
set "ERR=%ERRORLEVEL%"

echo [BAT] run #!RUN! exit code=!ERR! %DATE% %TIME%>>"%LOG_FILE%"
echo [BAT] run #!RUN! exited code=!ERR!

if "!ERR!"=="0" goto done

echo [BAT] crash detected -> restart in %RESTART_DELAY_SEC%s ...
timeout /t %RESTART_DELAY_SEC% /nobreak >nul
goto loop

:done
echo [BAT] exit code 0 -> stop supervisor
echo Log saved: %LOG_FILE%
pause
exit /b 0
