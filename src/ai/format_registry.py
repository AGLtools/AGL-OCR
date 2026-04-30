"""Persistence of AI-learned manifest formats.

Each learned format is a JSON file under data/learned_formats/ with:
    {
      "name": "MSC",                       # short display name
      "carrier": "Mediterranean Shipping Company",
      "signature": ["MSC GENEVA", "B/L No.", "Mediterranean Shipping"],
      "is_scanned": false,
      "model": "gemini-2.0-flash",
      "extraction_hints": "free text guidance for the LLM next time",
      "created_at": "2026-04-29T...",
      "samples": 1
    }

Detection: a learned format matches if ALL of its signature tokens appear
(case-insensitive) in the first ~1000 words of the PDF.
"""
from __future__ import annotations
import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from ..paths import app_data_dir


def _dir() -> Path:
    p = app_data_dir() / "data" / "learned_formats"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "format"


def list_learned() -> List[Dict]:
    out = []
    for f in sorted(_dir().glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            data["_path"] = str(f)
            out.append(data)
        except Exception:
            continue
    return out


def save_learned(
    name: str,
    signature: List[str],
    *,
    carrier: str = "",
    is_scanned: bool = False,
    model: str = "",
    extraction_hints: str = "",
    example_rows: Optional[List[Dict]] = None,
    parse_template: Optional[Dict] = None,
) -> Path:
    """Create or update a learned format. Returns the file path."""
    path = _dir() / f"{_slug(name)}.json"
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    existing_template = existing.get("parse_template", {})
    incoming_template = parse_template if parse_template is not None else None

    def _template_usable(tpl) -> bool:
        if not isinstance(tpl, dict):
            return False
        if (tpl.get("parse_code") or "").strip():
            return True
        return bool(tpl.get("row_patterns") or [])

    if incoming_template is None:
        final_template = existing_template
    elif _template_usable(incoming_template):
        # Keep the template that has the higher row_count (more rows = better parser).
        # Also never overwrite a "handcrafted" model with an AI-generated one.
        existing_model = existing.get("model", "")
        if existing_model == "handcrafted":
            final_template = existing_template
        else:
            existing_rows = int((existing_template or {}).get("row_count", 0))
            incoming_rows = int(incoming_template.get("row_count", 0))
            # Keep existing if it's strictly better; replace on tie (newer = fresher)
            if existing_rows > incoming_rows and _template_usable(existing_template):
                final_template = existing_template
            else:
                final_template = incoming_template
    else:
        # Do not degrade an existing good local parser with an empty/invalid one.
        final_template = existing_template

    data = {
        "name": name,
        "carrier": carrier or existing.get("carrier", ""),
        "signature": [s for s in (signature or []) if s and s.strip()],
        "is_scanned": bool(is_scanned),
        "model": model or existing.get("model", ""),
        "extraction_hints": extraction_hints or existing.get("extraction_hints", ""),
        "example_rows": example_rows if example_rows is not None else existing.get("example_rows", []),
        "parse_template": final_template,
        "created_at": existing.get("created_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "samples": int(existing.get("samples", 0)) + 1,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def delete_learned(name: str) -> bool:
    path = _dir() / f"{_slug(name)}.json"
    if path.exists():
        path.unlink()
        return True
    return False


def detect_learned(text: str) -> Optional[Dict]:
    """Return the best-matching learned format dict, or None.

    Matching strategy (whitespace-tolerant, partial):
      - Normalize both sides to single-space uppercase.
      - A format is a candidate if at least 60% of its signature tokens are
        found in the document (minimum 2 tokens).
      - Among candidates, the one with the highest match RATIO wins; ties
        broken by absolute number of matched tokens (more specific = better).

    Partial matching is necessary because PDF text extraction often splits
    multi-word headers across non-contiguous column blocks, so requiring 100%
    of literal tokens caused false negatives.
    """
    if not text:
        return None
    text_up = re.sub(r"\s+", " ", text.upper())

    def _has_token(doc: str, token: str) -> bool:
        # Word-boundary match for simple alnum tokens (e.g. MSC),
        # substring fallback for tokens containing punctuation/spaces.
        if re.fullmatch(r"[A-Z0-9_]+", token or ""):
            return re.search(rf"(?<![A-Z0-9_]){re.escape(token)}(?![A-Z0-9_])", doc) is not None
        return token in doc

    candidates = []
    for fmt in list_learned():
        sig = fmt.get("signature") or []
        norm_sig = [re.sub(r"\s+", " ", s.upper()).strip() for s in sig if s and s.strip()]
        if not norm_sig:
            continue
        matched = sum(1 for s in norm_sig if _has_token(text_up, s))
        ratio = matched / len(norm_sig)
        min_matched = 1 if len(norm_sig) == 1 else 2
        min_ratio = 1.0 if len(norm_sig) == 1 else 0.6
        if matched >= min_matched and ratio >= min_ratio:
            candidates.append((ratio, matched, fmt))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]
