# Packaging notes — AGL OCR

This folder contains everything needed to ship AGL OCR as a Windows installer.

## Folder layout expected before building

```
AGL_OCR/
├── poppler/              ← Windows binaries from
│   ├── bin/                oschwartz10612/poppler-windows
│   │   ├── pdftoppm.exe
│   │   ├── pdfinfo.exe
│   │   └── *.dll
│   └── share/
├── tesseract/            ← (OPTIONAL — to make the .exe truly standalone)
│   ├── tesseract.exe
│   ├── *.dll
│   └── tessdata/
│       ├── eng.traineddata
│       └── fra.traineddata
├── packaging/
│   ├── AGL_OCR.spec      ← PyInstaller spec
│   ├── installer.iss     ← Inno Setup script
│   ├── build.ps1         ← One-shot build script
│   └── agl.ico           ← (OPTIONAL) app icon
└── ...
```

> ⚠️ The `poppler/` folder shipped with this repo currently contains the Poppler
> **source code**, not Windows binaries. Replace it with the binary release before
> building.

## How to bundle Tesseract (so the installer is fully self-contained)

The simplest way:
1. Install Tesseract from https://github.com/UB-Mannheim/tesseract/wiki on a
   throwaway machine.
2. Copy `C:\Program Files\Tesseract-OCR\` into `AGL_OCR/tesseract/`.
3. Make sure `tesseract/tessdata/eng.traineddata` **and** `fra.traineddata`
   (download from https://github.com/tesseract-ocr/tessdata_best) are present.

If you skip this, end users will still need a system Tesseract installation
(`settings.yaml` lets them point to it).

## Build sequence

```powershell
# 1. From a clean Python venv with requirements.txt installed:
pip install pyinstaller

# 2. Build the .exe folder (writes dist\AGL_OCR\AGL_OCR.exe):
.\packaging\build.ps1

# 3. Smoke test:
.\dist\AGL_OCR\AGL_OCR.exe

# 4. Build the installer (requires Inno Setup 6 — https://jrsoftware.org/isdl.php):
& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" .\packaging\installer.iss
# → produces dist\AGL_OCR_Setup_1.0.0.exe
```

## Where user data goes after installation

The app writes to the install dir if writable, otherwise to
`%LOCALAPPDATA%\AGL_OCR\`:

- `data/templates/` — saved cartographic templates
- `data/exports/`   — generated `.xlsx` files
- `data/cache/`     — rasterized PDF pages

Configs (`config/settings.yaml`, `config/fields.yaml`) live in the install dir
and can be edited by the user/admin.
