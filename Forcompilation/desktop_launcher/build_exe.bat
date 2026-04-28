@echo off
:: ============================================================
::  Build the two PyInstaller .exe files (launcher + updater)
::  Output: Forcompilation\desktop_launcher\dist\
:: ============================================================
setlocal
title AGL OCR - build .exe

cd /d "%~dp0"
set "HERE=%~dp0"
set "ROOT=%HERE%..\.."
set "ICON=%HERE%..\icon.ico"
set "LOGO=%HERE%..\AGL_logo.png"

:: Pick a Python: prefer the dev .venv at repo root
if exist "%ROOT%\.venv\Scripts\python.exe" (
    set "PY=%ROOT%\.venv\Scripts\python.exe"
) else if exist "%HERE%..\python_portable\python.exe" (
    set "PY=%HERE%..\python_portable\python.exe"
) else (
    set "PY=python"
)

echo [INFO] Python: %PY%
"%PY%" --version

:: Make sure pyinstaller is available
"%PY%" -m pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing PyInstaller...
    "%PY%" -m pip install --upgrade pyinstaller
)

:: Clean previous build artefacts
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

echo.
echo === [1/2] Building "AGL OCR.exe" (launcher) ====================
rem NOTE: no line continuations; --specpath uses "." to avoid trailing-backslash quoting bug
"%PY%" -m PyInstaller --noconfirm --clean --onefile --windowed --name "AGL OCR" --icon "%ICON%" --add-data "%ICON%;." --add-data "%LOGO%;." --distpath dist --workpath build --specpath . launcher.py
if errorlevel 1 goto :err

echo.
echo === [2/2] Building "AGL OCR Updater.exe" =======================
"%PY%" -m PyInstaller --noconfirm --clean --onefile --windowed --name "AGL OCR Updater" --icon "%ICON%" --add-data "%ICON%;." --distpath dist --workpath build --specpath . updater_gui.py
if errorlevel 1 goto :err

echo.
echo [OK] Built:
dir /b dist
exit /b 0

:err
echo.
echo [FAIL] PyInstaller build failed.
exit /b 1
