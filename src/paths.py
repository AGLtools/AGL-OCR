"""Resource path resolution.

Works in 3 modes:
- Dev (`python app.py`) — paths relative to project root.
- PyInstaller one-folder/one-file — uses sys._MEIPASS for bundled read-only assets,
  and the executable's directory for writable data.
"""
from __future__ import annotations
import sys
import os
from pathlib import Path


def is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def resource_dir() -> Path:
    """Directory containing READ-ONLY bundled assets (poppler/, tesseract/, config/)."""
    if is_frozen():
        # PyInstaller extracts data files here (one-file mode), or this is the exe dir
        # (one-folder mode). _MEIPASS is set in both cases.
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
    return Path(__file__).resolve().parent.parent


def app_data_dir() -> Path:
    """Directory for WRITABLE user data (templates, exports, cache).

    - Frozen: next to the .exe (portable). Fall back to %LOCALAPPDATA%\\AGL_OCR
      if the install dir is not writable (e.g. Program Files).
    - Dev: project root.
    """
    if is_frozen():
        exe_dir = Path(sys.executable).parent
        try:
            test = exe_dir / ".write_test"
            test.write_text("ok")
            test.unlink()
            return exe_dir
        except OSError:
            local = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "AGL_OCR"
            local.mkdir(parents=True, exist_ok=True)
            return local
    return Path(__file__).resolve().parent.parent


def poppler_bin() -> str | None:
    """Path to bundled Poppler bin folder, or None if not present.

    Checks multiple common layouts:
    - poppler/bin/           (direct Windows release)
    - poppler/Library/bin/   (conda-style release)
    """
    root = resource_dir()
    candidates = [
        root / "poppler" / "bin",
        root / "poppler" / "Library" / "bin",
    ]
    for candidate in candidates:
        if candidate.exists() and any(candidate.glob("pdftoppm*")):
            return str(candidate)
    # Fallback: also look next to the exe in frozen mode
    if is_frozen():
        exe_root = Path(sys.executable).parent
        for sub in ("poppler/bin", "poppler/Library/bin"):
            alt = exe_root / sub
            if alt.exists() and any(alt.glob("pdftoppm*")):
                return str(alt)
    return None


def tesseract_exe() -> str | None:
    """Path to bundled tesseract.exe, or None if not present (system PATH used)."""
    candidate = resource_dir() / "tesseract" / "tesseract.exe"
    if candidate.exists():
        return str(candidate)
    return None


def tessdata_dir() -> str | None:
    candidate = resource_dir() / "tesseract" / "tessdata"
    if candidate.exists():
        return str(candidate)
    return None
