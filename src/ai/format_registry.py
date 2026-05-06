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
        # Preserve any user feedback already attached to this format.
        "feedback": existing.get("feedback", []),
        "created_at": existing.get("created_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "samples": int(existing.get("samples", 0)) + 1,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def add_feedback(
    name: str,
    text: str,
    *,
    doc_name: str = "",
    rows_snapshot: Optional[List[Dict]] = None,
    problem_indexes: Optional[List[int]] = None,
    image_paths: Optional[List[str]] = None,
    diffs: Optional[List[Dict]] = None,
) -> bool:
    """Append a feedback entry to a learned format.

    Beyond a free-text comment, callers can attach:
      * ``diffs``: list of :class:`SpatialDiff` dicts — STRUCTURED evidence
        that replaces prose + screenshots. Cheap to send back to the LLM.
      * ``rows_snapshot``: full extraction (legacy).
      * ``problem_indexes``: indexes into ``rows_snapshot`` (legacy).
      * ``image_paths``: PNG snapshots (legacy, deprecated — kept for
        backward compatibility but no longer produced by the UI).
    """
    if not (text and text.strip()) and not diffs:
        return False
    path = _dir() / f"{_slug(name)}.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    feedback = data.get("feedback") or []
    entry: Dict = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "doc_name": doc_name or "",
        "text": (text or "").strip(),
    }
    if diffs:
        # Persist structured spatial evidence. This is the new primary
        # signal — re-learning calls only need this, not the screenshots.
        entry["diffs"] = list(diffs)
    if rows_snapshot:
        # Strip internal flags before persisting
        clean = [
            {k: v for k, v in r.items() if not str(k).startswith("_")}
            for r in rows_snapshot
        ]
        entry["rows_snapshot"] = clean
    if problem_indexes:
        entry["problem_indexes"] = list(problem_indexes)
    if image_paths:
        entry["image_paths"] = [str(p) for p in image_paths]
    feedback.append(entry)
    # Keep only the last 10 entries, deduplicated by the first 80 chars
    # of the comment text. Most-recent wins.
    seen = set()
    kept: List[Dict] = []
    for fb in reversed(feedback):
        key = (fb.get("text", "") or "")[:80]
        if key in seen:
            continue
        seen.add(key)
        kept.append(fb)
        if len(kept) >= 10:
            break
    data["feedback"] = list(reversed(kept))
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return True


def save_feedback_image(name: str, image_bytes: bytes, label: str = "") -> str:
    """Persist a PNG snapshot under ``<learned_formats>/<slug>_attachments/``.

    Returns the absolute file path (string). Used by the feedback dialog
    to attach annotated page snapshots to a feedback entry.
    """
    slug = _slug(name)
    folder = _dir() / f"{slug}_attachments"
    folder.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    safe_label = re.sub(r"[^A-Za-z0-9_-]+", "_", label or "page").strip("_") or "page"
    p = folder / f"{ts}_{safe_label}.png"
    p.write_bytes(image_bytes)
    return str(p)


def get_feedback_text(name: str) -> str:
    """Return all feedback entries concatenated as a prompt-ready block.

    Includes the free-text comment, plus — if attached — a compact JSON
    rendering of the user-flagged problem rows and any extra metadata
    (page snapshots are passed separately to multimodal calls).
    """
    path = _dir() / f"{_slug(name)}.json"
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    fb = data.get("feedback") or []
    if not fb:
        return ""
    # Lazy import to avoid a circular dependency on startup.
    try:
        from .spatial_diff import SpatialDiff, format_diff_as_evidence_block
    except Exception:
        SpatialDiff = None  # type: ignore
        format_diff_as_evidence_block = None  # type: ignore
    lines = []
    for i, entry in enumerate(fb, 1):
        ts = entry.get("timestamp", "")
        doc = entry.get("doc_name", "")
        lines.append(f"[{i}] {ts} ({doc}): {entry.get('text', '').strip()}")
        # Preferred path : structured spatial diffs.
        diffs_raw = entry.get("diffs") or []
        if diffs_raw and SpatialDiff is not None and format_diff_as_evidence_block is not None:
            try:
                diffs = [SpatialDiff.from_dict(d) for d in diffs_raw]
                ev = format_diff_as_evidence_block(diffs, max_chars=1200)
                if ev:
                    lines.append(ev)
                continue
            except Exception:
                pass
        # Legacy fallback : flagged rows summary.
        rows = entry.get("rows_snapshot") or []
        idx = entry.get("problem_indexes") or []
        if rows and idx:
            problem_rows = [rows[k] for k in idx if 0 <= k < len(rows)]
            if problem_rows:
                shown = problem_rows[:8]
                lines.append(
                    f"    Lignes problematiques signalees ({len(problem_rows)}, "
                    f"max 8 affichees) :"
                )
                for pr in shown:
                    bits = ", ".join(
                        f"{k}={str(v)[:40]}" for k, v in pr.items()
                        if str(v).strip() and not str(k).startswith("_")
                    )
                    lines.append(f"      - {bits}")
        elif rows:
            lines.append(
                f"    (Extraction complete attachee : {len(rows)} ligne(s).)"
            )
        if entry.get("image_paths"):
            lines.append(
                f"    Captures jointes (legacy) : {len(entry['image_paths'])} image(s)."
            )
    return "\n".join(lines)


def get_feedback_entries(name: str) -> List[Dict]:
    """Return raw feedback list (for dialogs that need image_paths, etc.)."""
    path = _dir() / f"{_slug(name)}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return list(data.get("feedback") or [])


def update_format(name: str, **fields) -> bool:
    """Update arbitrary top-level fields of a learned format (dev tool)."""
    path = _dir() / f"{_slug(name)}.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    for k, v in fields.items():
        data[k] = v
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return True


def purge_old_attachments(max_age_days: int = 14) -> int:
    """Delete legacy *_attachments PNG files older than ``max_age_days``.

    Spatial diffs replaced screenshots, so the attachments folder shrinks
    over time. Returns the number of files removed.
    """
    import time
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    base = _dir()
    if not base.exists():
        return 0
    for folder in base.glob("*_attachments"):
        if not folder.is_dir():
            continue
        for f in folder.rglob("*"):
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink(missing_ok=True)
                    removed += 1
            except OSError:
                continue
        # Try to drop the folder if it's now empty.
        try:
            next(folder.iterdir())
        except StopIteration:
            try:
                folder.rmdir()
            except OSError:
                pass
        except OSError:
            pass
    return removed


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
