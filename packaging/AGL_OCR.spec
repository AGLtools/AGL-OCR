# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for AGL OCR.

Produces a one-folder distributable in `dist/AGL_OCR/` containing:
- AGL_OCR.exe
- _internal/      (Python runtime + libs)
- config/         (YAML configs — editable post-install)
- poppler/        (bundled pdftoppm/pdfinfo + DLLs)
- tesseract/      (bundled tesseract.exe + tessdata, OPTIONAL)

Build:
    pyinstaller --noconfirm packaging\\AGL_OCR.spec
"""
import os
from pathlib import Path

block_cipher = None

PROJECT_ROOT = Path(os.getcwd())

# ---- bundle data ----
datas = [
    (str(PROJECT_ROOT / "config"), "config"),
]

# Bundle Poppler if present (must be the Windows binary release: bin/, share/, ...)
poppler_dir = PROJECT_ROOT / "poppler"
if (poppler_dir / "bin").exists():
    datas.append((str(poppler_dir), "poppler"))

# Bundle Tesseract if you placed it at ./tesseract (optional)
tesseract_dir = PROJECT_ROOT / "tesseract"
if tesseract_dir.exists():
    datas.append((str(tesseract_dir), "tesseract"))

# Optional app icon
icon_path = PROJECT_ROOT / "packaging" / "agl.ico"
icon_arg = str(icon_path) if icon_path.exists() else None


a = Analysis(
    ['..\\app.py'],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'PyQt5.sip',
        'PIL._tkinter_finder',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'pytest'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='AGL_OCR',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,            # GUI app — no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_arg,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='AGL_OCR',
)
