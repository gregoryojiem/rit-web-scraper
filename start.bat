@echo off
setlocal

:: Check admin privileges
NET SESSION >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    PowerShell -Command "Start-Process -Verb RunAs -FilePath '%~dp0main.py'"
    exit /b
)

:: If already admin, run normally
cd /d "%~dp0"
python main.py
pause