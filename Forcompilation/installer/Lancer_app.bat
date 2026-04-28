@echo off
:: Manual / fallback launcher for AGL OCR (no splash, no .exe).
:: Useful for support or when AGL OCR.exe is missing.
setlocal
cd /d "%~dp0"
set "APPDIR=%~dp0"

if exist "%APPDIR%venv_agl\Scripts\python.exe" (
    set "PY=%APPDIR%venv_agl\Scripts\python.exe"
) else if exist "%APPDIR%python_portable\python.exe" (
    set "PY=%APPDIR%python_portable\python.exe"
) else (
    set "PY=python"
)

start "" "%PY%" "%APPDIR%app.py"
exit /b 0
