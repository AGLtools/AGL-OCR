@echo off
:: ============================================================
::  AGL OCR - one-click full build pipeline
::  1. Build PyInstaller .exe files (launcher + updater)
::  2. Refresh offline wheelhouse
::  3. Compile Inno Setup installer
::  Output: Forcompilation\dist\AGL_OCR_Setup_<version>.exe
:: ============================================================
setlocal
title AGL OCR - BUILD ALL
color 0B

cd /d "%~dp0"
set "HERE=%~dp0"

echo.
echo ===============================================================
echo            AGL OCR - FULL DEPLOYMENT BUILD
echo ===============================================================
echo.

:: ----- [1] Build .exe ------------------------------------------------
echo [STEP 1/3] Building launcher and updater .exe ...
call "%HERE%desktop_launcher\build_exe.bat"
if errorlevel 1 goto :err

:: ----- [2] Build wheelhouse -----------------------------------------
echo.
echo [STEP 2/3] Refreshing offline wheelhouse ...
call "%HERE%installer\make_wheelhouse.bat"
if errorlevel 1 (
    echo [WARN] wheelhouse build had issues - installer will fall back to PyPI.
)

:: ----- [3] Compile Inno Setup ---------------------------------------
echo.
echo [STEP 3/3] Compiling Inno Setup installer ...

:: Locate ISCC.exe
set "ISCC="
for %%P in (
    "C:\Inno Setup 6\ISCC.exe"
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
    "C:\Program Files\Inno Setup 6\ISCC.exe"
    "C:\Program Files (x86)\Inno Setup 5\ISCC.exe"
) do (
    if exist %%P set "ISCC=%%~P"
)
if "%ISCC%"=="" (
    if exist "%HERE%.build_config.json" (
        for /f "usebackq tokens=2 delims=:," %%A in ("%HERE%.build_config.json") do (
            set "ISCC=%%~A"
            set "ISCC=!ISCC:"=!"
        )
    )
)
if "%ISCC%"=="" (
    echo [ERROR] Inno Setup Compiler ^(ISCC.exe^) not found.
    echo Install Inno Setup 6 from https://jrsoftware.org/isinfo.php
    echo Then re-run BUILD_ALL.bat.
    pause
    exit /b 2
)
echo [INFO] ISCC: %ISCC%

if not exist "%HERE%dist" mkdir "%HERE%dist"

"%ISCC%" "%HERE%installer\AGL_OCR.iss"
if errorlevel 1 goto :err

echo.
echo ===============================================================
echo    BUILD COMPLETE
echo    Installer: %HERE%dist\
dir /b "%HERE%dist\*.exe"
echo ===============================================================
echo.
pause
exit /b 0

:err
echo.
echo ===============================================================
echo    BUILD FAILED  - see messages above
echo ===============================================================
pause
exit /b 1
