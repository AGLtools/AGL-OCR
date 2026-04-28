@echo off
:: ============================================================
::  AGL OCR - command-line updater (called by AGL OCR Updater.exe
::  or runnable manually).
::
::  Usage:
::     UPDATE.bat              -> pulls latest from default branch
::     set AGL_TARGET_REF=v1.2 ^&^& UPDATE.bat   -> pulls a specific ref
:: ============================================================
setlocal enabledelayedexpansion
title AGL OCR - Update
color 0B

cd /d "%~dp0"
set "APPDIR=%~dp0"

:: ----- Pick Python ----------------------------------------------------
if exist "%APPDIR%venv_agl\Scripts\python.exe" (
    set "PY=%APPDIR%venv_agl\Scripts\python.exe"
) else if exist "%APPDIR%python_portable\python.exe" (
    set "PY=%APPDIR%python_portable\python.exe"
) else (
    where python >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] No Python interpreter available.
        if not defined AGL_SILENT pause
        exit /b 1
    )
    set "PY=python"
)

set REPO_OWNER=AGLtools
set REPO_NAME=AGL-OCR
set DEFAULT_BRANCH=main

if not defined AGL_TARGET_REF set "AGL_TARGET_REF=%DEFAULT_BRANCH%"

echo.
echo ============================================================
echo    AGL OCR - update from GitHub
echo    Repository: %REPO_OWNER%/%REPO_NAME%
echo    Target ref: %AGL_TARGET_REF%
echo ============================================================
echo.

"%PY%" "%APPDIR%scripts\apply_update.py" "%AGL_TARGET_REF%"
set "RC=%ERRORLEVEL%"

if "%RC%"=="0" (
    echo.
    echo [OK] Update complete.
) else (
    echo.
    echo [FAIL] Update aborted (code %RC%).
)

if not defined AGL_SILENT pause
exit /b %RC%
