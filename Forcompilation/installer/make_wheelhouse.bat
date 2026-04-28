@echo off
:: ============================================================
::  Pre-download every Python dependency wheel into wheelhouse\
::  so the installer can install them OFFLINE on the target machine.
::
::  Run this once per release (or whenever requirements.txt changes).
:: ============================================================
setlocal
title AGL OCR - build wheelhouse

cd /d "%~dp0"
set "HERE=%~dp0"
set "ROOT=%HERE%..\.."

if exist "%ROOT%\.venv\Scripts\python.exe" (
    set "PY=%ROOT%\.venv\Scripts\python.exe"
) else if exist "%HERE%..\python_portable\python.exe" (
    set "PY=%HERE%..\python_portable\python.exe"
) else (
    set "PY=python"
)

echo [INFO] Python: %PY%
"%PY%" --version

:: Refresh wheelhouse
if exist "%HERE%wheelhouse" rmdir /s /q "%HERE%wheelhouse"
mkdir "%HERE%wheelhouse"

echo.
echo [*] Downloading wheels for requirements.txt...
"%PY%" -m pip download -r "%ROOT%\requirements.txt" ^
    --dest "%HERE%wheelhouse" ^
    --only-binary=:all: ^
    --platform win_amd64 ^
    --python-version 314 ^
    --implementation cp ^
    --abi cp314
if errorlevel 1 (
    echo [WARN] strict download failed - retrying without --only-binary...
    "%PY%" -m pip download -r "%ROOT%\requirements.txt" --dest "%HERE%wheelhouse"
)

:: Also include pip / setuptools / wheel themselves
"%PY%" -m pip download pip setuptools wheel --dest "%HERE%wheelhouse"

echo.
echo [OK] Wheelhouse ready:
dir /b "%HERE%wheelhouse" | find /c /v ""
exit /b 0
