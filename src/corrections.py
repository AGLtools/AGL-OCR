"""Per-document correction store.

Persists user edits to extracted data so that the next time the same source
document is opened (or the queue is exported), the corrections are restored
automatically.

Storage: one JSON sidecar per source document in ``data/corrections/``,
named after a sanitized hash of the source path so it survives renames /
moves better than a raw filename.

Two correction kinds:

1. ``field_corrections`` — keyed by ``(page_idx, field_key)`` for the
   manual mapping mode (template / auto-mapper).
2. ``manifest_rows`` — full list of dict rows from the Smart Parse
   manifest mode (state-machine output).  Stored AS-IS plus a flag
   ``_user_edited`` per row.

Design choices (per AGL mandate):
- Write IMMEDIATELY on every change (no "save" button).
- Atomic write via temp + replace.
- Tolerant load: malformed file -> start fresh, never crash the app.
- Each armateur format may have radically different page architectures, so
  we never assume a particular schema; corrections are pure overrides.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import DATA_DIR

CORRECTIONS_DIR = DATA_DIR / "corrections"
CORRECTIONS_DIR.mkdir(parents=True, exist_ok=True)


def _sidecar_path(source_path: Path) -> Path:
    """Stable sidecar filename derived from absolute source path."""
    abs_str = str(source_path.resolve()).lower()
    h = hashlib.sha1(abs_str.encode("utf-8")).hexdigest()[:12]
    safe_name = "".join(c if c.isalnum() or c in "-_." else "_" for c in source_path.stem)[:60]
    return CORRECTIONS_DIR / f"{safe_name}__{h}.json"


@dataclass
class DocCorrections:
    source_path: str = ""
    # field_corrections[str(page_idx)][field_key] = {"value": str, "bbox": [x,y,w,h] or null}
    field_corrections: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    # manifest_rows: full row dicts from ManifestParser (when applicable)
    # Each row may carry "_user_edited": True when modified.
    manifest_rows: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DocCorrections":
        return cls(
            source_path=d.get("source_path", ""),
            field_corrections=d.get("field_corrections", {}) or {},
            manifest_rows=d.get("manifest_rows", []) or [],
        )


class CorrectionStore:
    """Per-document correction persistence."""

    def __init__(self, source_path: str | Path):
        self.source_path = Path(source_path)
        self.path = _sidecar_path(self.source_path)
        self.data = self._load()

    # ---------- IO ----------
    def _load(self) -> DocCorrections:
        if not self.path.exists():
            return DocCorrections(source_path=str(self.source_path))
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return DocCorrections.from_dict(raw)
        except Exception:
            return DocCorrections(source_path=str(self.source_path))

    def _flush(self) -> None:
        """Atomic write of the JSON sidecar."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.data.to_dict(), indent=2, ensure_ascii=False)
        # Write to temp then replace
        fd, tmp = tempfile.mkstemp(prefix=".corr_", suffix=".json", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ---------- Field corrections (page-by-page UI mode) ----------
    def set_field(
        self,
        page_idx: int,
        field_key: str,
        value: str,
        bbox: Optional[Tuple[int, int, int, int]] = None,
    ) -> None:
        page_key = str(page_idx)
        self.data.field_corrections.setdefault(page_key, {})[field_key] = {
            "value": value,
            "bbox": list(bbox) if bbox else None,
        }
        self._flush()

    def clear_field(self, page_idx: int, field_key: str) -> None:
        page_key = str(page_idx)
        page = self.data.field_corrections.get(page_key)
        if page and field_key in page:
            page.pop(field_key, None)
            if not page:
                self.data.field_corrections.pop(page_key, None)
            self._flush()

    def get_page_corrections(self, page_idx: int) -> Dict[str, Dict[str, Any]]:
        return self.data.field_corrections.get(str(page_idx), {}) or {}

    def apply_to_extraction(
        self,
        page_idx: int,
        extraction: Dict[str, dict],
    ) -> Dict[str, dict]:
        """Override extraction values with stored corrections for this page.

        Mutates and returns the extraction dict.
        """
        for field_key, corr in self.get_page_corrections(page_idx).items():
            existing = extraction.get(field_key) or {}
            existing["value"] = corr.get("value", "")
            existing["lines"] = (corr.get("value", "") or "").splitlines() or [""]
            if corr.get("bbox"):
                existing["bbox"] = tuple(corr["bbox"])
            existing["page"] = page_idx
            existing["_user_corrected"] = True
            extraction[field_key] = existing
        return extraction

    # ---------- Manifest rows ----------
    def save_manifest_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Replace the stored manifest rows entirely (e.g. after a fresh parse)."""
        self.data.manifest_rows = list(rows)
        self._flush()

    def update_manifest_row(self, row_idx: int, updates: Dict[str, Any]) -> None:
        if not (0 <= row_idx < len(self.data.manifest_rows)):
            return
        self.data.manifest_rows[row_idx].update(updates)
        self.data.manifest_rows[row_idx]["_user_edited"] = True
        self._flush()

    def get_manifest_rows(self) -> List[Dict[str, Any]]:
        return list(self.data.manifest_rows)

    def has_manifest_rows(self) -> bool:
        return bool(self.data.manifest_rows)

    # ---------- Lifecycle ----------
    def reset(self) -> None:
        self.data = DocCorrections(source_path=str(self.source_path))
        if self.path.exists():
            try:
                self.path.unlink()
            except OSError:
                pass
