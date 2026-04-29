"""Debug log for AI calls — saves every Gemini interaction to disk so the
user can inspect what was actually sent / received when extraction fails.

Files are kept under data/ai_debug/<timestamp>_<kind>.txt.
Only the LAST 20 entries are kept (older ones get auto-purged).
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..paths import app_data_dir


_MAX_KEEP = 20
_LAST_PATH: Optional[Path] = None


def _dir() -> Path:
    p = app_data_dir() / "data" / "ai_debug"
    p.mkdir(parents=True, exist_ok=True)
    return p


def log_call(
    *,
    kind: str,                  # "extract" | "learn" | "fix" | "vision_ocr"
    source_file: str = "",
    prompt: str = "",
    raw_response: str = "",
    parsed: object = None,
    ocr_text: str = "",
    error: str = "",
    extra: Optional[dict] = None,
) -> Path:
    """Write a debug entry. Returns the file path."""
    global _LAST_PATH
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    path = _dir() / f"{ts}_{kind}.txt"
    parts = []
    parts.append(f"=== AI DEBUG LOG — {kind.upper()} ===")
    parts.append(f"Timestamp : {datetime.now().isoformat(timespec='seconds')}")
    if source_file:
        parts.append(f"Source    : {source_file}")
    if extra:
        parts.append("Extra     : " + json.dumps(extra, ensure_ascii=False))
    if error:
        parts.append("\n--- ERROR ---\n" + str(error))
    if ocr_text:
        parts.append(f"\n--- OCR TEXT ({len(ocr_text)} chars) ---")
        parts.append(ocr_text[:20000])
        if len(ocr_text) > 20000:
            parts.append(f"... ({len(ocr_text) - 20000} more chars truncated)")
    if prompt:
        parts.append(f"\n--- PROMPT SENT ({len(prompt)} chars) ---")
        parts.append(prompt[:8000])
        if len(prompt) > 8000:
            parts.append(f"... ({len(prompt) - 8000} more chars truncated)")
    if raw_response:
        parts.append(f"\n--- RAW RESPONSE ({len(raw_response)} chars) ---")
        parts.append(raw_response[:30000])
        if len(raw_response) > 30000:
            parts.append(f"... ({len(raw_response) - 30000} more chars truncated)")
    if parsed is not None:
        parts.append("\n--- PARSED ---")
        try:
            parts.append(json.dumps(parsed, ensure_ascii=False, indent=2)[:20000])
        except Exception:
            parts.append(str(parsed)[:20000])
    path.write_text("\n".join(parts), encoding="utf-8")
    _LAST_PATH = path
    _purge_old()
    return path


def get_last_log_path() -> Optional[Path]:
    """Return the most recent log file path, or None."""
    global _LAST_PATH
    if _LAST_PATH and _LAST_PATH.exists():
        return _LAST_PATH
    files = sorted(_dir().glob("*.txt"))
    if not files:
        return None
    _LAST_PATH = files[-1]
    return _LAST_PATH


def list_logs() -> list[Path]:
    return sorted(_dir().glob("*.txt"), reverse=True)


def _purge_old() -> None:
    files = sorted(_dir().glob("*.txt"))
    if len(files) <= _MAX_KEEP:
        return
    for f in files[: len(files) - _MAX_KEEP]:
        try:
            f.unlink()
        except Exception:
            pass
