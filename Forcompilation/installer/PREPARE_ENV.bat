@echo off
:: ================================================================
:: AGL OCR - Configuration de l'environnement Python (premiere fois)
:: Utilise goto au lieu de blocs imbr iques pour eviter les erreurs
:: CMD "inattendu" avec enabledelayedexpansion + pip
:: ================================================================
setlocal
title AGL OCR - Configuration

echo ================================================================
echo    AGL OCR - MISE EN PLACE PYTHON (premiere fois)
echo ================================================================
echo.

cd /d "%~dp0"
set "APPDIR=%~dp0"

:: Normalise AGL_SILENT: 1 si defini, 0 sinon
if defined AGL_SILENT (set "SILENT=1") else (set "SILENT=0")

:: ----- Locate Python ------------------------------------------------
if exist "%APPDIR%python_portable\python.exe" goto :found_portable
where python >nul 2>&1
if not errorlevel 1 goto :found_system
echo [ERREUR] Aucun interpreteur Python disponible.
echo Reinstallez AGL OCR.
if "%SILENT%"=="0" pause
exit /b 1

:found_portable
set "PY=%APPDIR%python_portable\python.exe"
goto :have_python

:found_system
set "PY=python"

:have_python
echo [INFO] Python : %PY%

:: ----- Remove EXTERNALLY-MANAGED lock (portable Python) ------------
del /f /q "%APPDIR%python_portable\Lib\EXTERNALLY-MANAGED" >nul 2>&1
for /f "delims=" %%D in ('dir /b /ad "%APPDIR%python_portable\Lib" 2^>nul') do (
    del /f /q "%APPDIR%python_portable\Lib\%%D\EXTERNALLY-MANAGED" >nul 2>&1
)

:: ----- Bootstrap pip if missing ------------------------------------
"%PY%" -m pip --version >nul 2>&1
if not errorlevel 1 goto :pip_ok
echo [INFO] Bootstrap pip...
"%PY%" -m ensurepip --upgrade >nul 2>&1

:pip_ok

:: ----- Create venv_agl if needed -----------------------------------
if exist "%APPDIR%venv_agl\Scripts\python.exe" goto :venv_ok
echo [INFO] Creation de l'environnement virtuel venv_agl...
"%PY%" -m venv "%APPDIR%venv_agl" --system-site-packages
if errorlevel 1 goto :venv_error
goto :venv_ok

:venv_error
echo [ERREUR] Impossible de creer venv_agl.
if "%SILENT%"=="0" pause
exit /b 2

:venv_ok
echo [INFO] venv_agl OK
set "VENVPY=%APPDIR%venv_agl\Scripts\python.exe"

:: NOTE: pip self-upgrade intentionally skipped.
:: Upgrading pip while it is running can crash CMD with code 255.
:: The pip version from the wheelhouse is sufficient.

:: ----- Install requirements (offline first, online fallback) -------
echo [INFO] Installation des paquets Python...
if not exist "%APPDIR%wheelhouse" goto :install_online

"%VENVPY%" -m pip install --no-index --find-links "%APPDIR%wheelhouse" -r "%APPDIR%requirements.txt" --disable-pip-version-check --quiet
if not errorlevel 1 goto :install_ok
echo [AVERT] Installation hors-ligne incomplete, tentative PyPI...

:install_online
"%VENVPY%" -m pip install --find-links "%APPDIR%wheelhouse" -r "%APPDIR%requirements.txt" --disable-pip-version-check --quiet
if errorlevel 1 goto :install_error
goto :install_ok

:install_error
echo [ERREUR] Echec de l installation des paquets.
if "%SILENT%"=="0" pause
exit /b 3

:install_ok

:: ----- Smoke test --------------------------------------------------
echo [INFO] Test de demarrage...
"%VENVPY%" -c "import PyQt5, pdfplumber, pytesseract, openpyxl; print('IMPORTS OK')"
if errorlevel 1 goto :smoke_error

:: ----- Pre-compile bytecode ----------------------------------------
"%VENVPY%" -m compileall -q "%APPDIR%src" "%APPDIR%app.py" >nul 2>&1

echo.
echo ================================================================
echo    CONFIGURATION COMPLETE - AGL OCR est pret.
echo ================================================================
echo.
if "%SILENT%"=="0" timeout /t 3 >nul
exit /b 0

:smoke_error
echo [ERREUR] Test de demarrage echoue - paquets manquants.
if "%SILENT%"=="0" pause
exit /b 4
