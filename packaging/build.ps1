# Build a portable AGL OCR distribution with PyInstaller.
# Usage:  .\packaging\build.ps1
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Cleaning previous build…" -ForegroundColor Cyan
Remove-Item -Recurse -Force .\build, .\dist -ErrorAction SilentlyContinue

# Sanity checks
if (-not (Test-Path .\poppler\bin\pdftoppm.exe)) {
    Write-Warning "poppler\bin\pdftoppm.exe not found. Download the Windows release from:"
    Write-Warning "  https://github.com/oschwartz10612/poppler-windows/releases/latest"
    Write-Warning "and extract it into .\poppler\ (so .\poppler\bin\pdftoppm.exe exists)."
    exit 1
}
if (-not (Test-Path .\tesseract\tesseract.exe)) {
    Write-Warning "tesseract\tesseract.exe not found — the .exe will require a system Tesseract."
    Write-Warning "To bundle it, copy a portable Tesseract into .\tesseract\ (with tessdata\eng.traineddata, fra.traineddata)."
}

Write-Host "==> Running PyInstaller…" -ForegroundColor Cyan
python -m PyInstaller --noconfirm --clean .\packaging\AGL_OCR.spec

Write-Host "==> Done. Output: .\dist\AGL_OCR\AGL_OCR.exe" -ForegroundColor Green
Write-Host "    To build the installer, open .\packaging\installer.iss in Inno Setup Compiler."
