"""Spatial diffs : structured evidence of an extraction error.

Replaces prose feedback + page screenshots with a compact, structured
description of *why* a row is wrong. The diff carries the words around
the extracted value so the LLM can fix the rule without re-seeing the
full document.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Iterable

from ..spatial_extractor import load_pages
from ..spatial_index import SpatialPage, Word


# ────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────
# Fields a user typically flags as wrong. We only compute diffs for these.
_DIFFABLE_FIELDS = (
    "weight", "volume", "pack_qty",
    "shipper", "consignee", "notify",
    "port_of_loading", "port_of_discharge",
    "place_of_acceptance", "place_of_delivery",
    "container_type", "seal1", "seal2",
)

# Words within this radius of the extracted value are sent as context.
_NEARBY_RADIUS_X = 200.0
_NEARBY_RADIUS_Y = 60.0


# ────────────────────────────────────────────────────────────────────────
# Dataclass
# ────────────────────────────────────────────────────────────────────────
@dataclass
class SpatialDiff:
    field_name: str
    bl_number: str
    container_number: str
    extracted_value: str
    page: int
    nearby_words: List[Dict] = field(default_factory=list)   # {text,x0,top}
    candidate_value: str = ""
    candidate_anchor: str = ""
    rule_field: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "SpatialDiff":
        return cls(
            field_name=d.get("field_name", ""),
            bl_number=d.get("bl_number", ""),
            container_number=d.get("container_number", ""),
            extracted_value=str(d.get("extracted_value", "")),
            page=int(d.get("page", 1) or 1),
            nearby_words=list(d.get("nearby_words", [])),
            candidate_value=d.get("candidate_value", "") or "",
            candidate_anchor=d.get("candidate_anchor", "") or "",
            rule_field=d.get("rule_field", "") or d.get("field_name", ""),
        )


# ────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────
def compute_diffs(
    rows: List[Dict],
    flagged_idxs: Iterable[int],
    source_path: str | Path,
    *,
    fields: Iterable[str] = _DIFFABLE_FIELDS,
) -> List[SpatialDiff]:
    """Build a SpatialDiff list from flagged rows.

    For each flagged row × each diffable field, locate the extracted
    value's coordinates in the source PDF, capture nearby words, and
    propose a candidate replacement when one looks more plausible.

    Returns ``[]`` if the PDF cannot be re-opened or no flag is set.
    """
    flagged_idxs = sorted(set(int(i) for i in flagged_idxs))
    if not flagged_idxs:
        return []
    try:
        pages: List[SpatialPage] = load_pages(source_path)
    except Exception:
        pages = []
    if not pages:
        return []

    diffs: List[SpatialDiff] = []
    for idx in flagged_idxs:
        if not (0 <= idx < len(rows)):
            continue
        row = rows[idx]
        page_no = _row_page(row, len(pages))
        sp = pages[max(0, min(page_no - 1, len(pages) - 1))]
        for fld in fields:
            val = row.get(fld)
            if val in (None, "", 0):
                continue
            d = _diff_for_field(sp, row, fld, page_no)
            if d is not None:
                diffs.append(d)
    return diffs


def format_diff_as_evidence_block(diffs: List[SpatialDiff], *, max_chars: int = 1600) -> str:
    """Compact textual rendering for the LLM patch prompt."""
    if not diffs:
        return ""
    lines = ["## EVIDENCE SPATIALE (champs flagues par l'utilisateur) ##"]
    seen_fields: set = set()
    for d in diffs:
        # Group by field — one short evidence block per (field, BL).
        key = (d.field_name, d.bl_number, d.container_number)
        if key in seen_fields:
            continue
        seen_fields.add(key)
        lines.append("")
        lines.append(
            f"- champ='{d.field_name}'  bl='{d.bl_number}'  "
            f"conteneur='{d.container_number or '-'}'  page={d.page}"
        )
        lines.append(f"  extrait : {d.extracted_value!r}")
        if d.candidate_value:
            anchor = f" (apres '{d.candidate_anchor}')" if d.candidate_anchor else ""
            lines.append(f"  attendu : {d.candidate_value!r}{anchor}")
        if d.nearby_words:
            preview = " ".join(w["text"] for w in d.nearby_words[:12])
            lines.append(f"  voisins : {preview}")
        if d.rule_field:
            lines.append(f"  regle a corriger : SpatialRule(field_name='{d.rule_field}')")
        if sum(len(l) for l in lines) > max_chars:
            lines.append("  ... (tronque)")
            break
    return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────
# Internals
# ────────────────────────────────────────────────────────────────────────
_NUMBER_RX = re.compile(r"[\d]{1,3}(?:[ ,.]\d{3})*(?:[.,]\d{1,3})?")


def _row_page(row: Dict, total_pages: int) -> int:
    pg = row.get("page")
    try:
        n = int(pg) if pg is not None else 0
    except (TypeError, ValueError):
        n = 0
    if n <= 0:
        n = 1
    return max(1, min(n, total_pages))


def _normalise(s: str) -> str:
    return re.sub(r"[\s,]+", "", str(s)).lower()


def _find_word(sp: SpatialPage, value: str) -> Optional[Word]:
    """Try to locate a word whose text matches ``value`` (loose match)."""
    if not value:
        return None
    target = _normalise(value)
    if not target:
        return None
    # Exact prefix match first
    for w in sp.words:
        if _normalise(w.text) == target:
            return w
    # Sliding window of consecutive words on the same line — handles
    # numbers split by a thousand separator that pdfplumber kept as one
    # token but the row stored without separator.
    lines = sp._group_by_lines(y_tol=4.0)  # type: ignore[attr-defined]
    for line in lines:
        for i in range(len(line)):
            for j in range(i + 1, min(i + 4, len(line) + 1)):
                joined = "".join(_normalise(w.text) for w in line[i:j])
                if joined == target:
                    return line[i]
    # Fall back to substring match on token
    for w in sp.words:
        if target in _normalise(w.text):
            return w
    return None


def _nearby(sp: SpatialPage, anchor: Word) -> List[Dict]:
    """Words within ±_NEARBY_RADIUS around ``anchor``."""
    out: List[Dict] = []
    for w in sp.words:
        if abs(w.cy - anchor.cy) <= _NEARBY_RADIUS_Y and abs(w.cx - anchor.cx) <= _NEARBY_RADIUS_X:
            out.append({"text": w.text, "x0": round(w.x0, 1), "top": round(w.top, 1)})
    out.sort(key=lambda d: (d["top"], d["x0"]))
    return out[:24]


def _candidate_for(field_name: str, nearby_words: List[Dict]) -> tuple[str, str]:
    """Heuristic candidate inferred from neighbouring words.

    For numeric fields (weight/volume/pack_qty) : look for the largest
    plausible number on the same line — typically the BL total when the
    extracted value was a wrongly-divided per-container value.
    For textual fields : leave empty (the LLM decides from context).
    """
    if field_name not in ("weight", "volume", "pack_qty"):
        return "", ""
    candidates: List[tuple[float, str, str]] = []
    for i, w in enumerate(nearby_words):
        m = _NUMBER_RX.fullmatch(w["text"]) or _NUMBER_RX.search(w["text"])
        if not m:
            continue
        raw = m.group(0)
        try:
            value = float(raw.replace(" ", "").replace(",", "").rstrip("."))
        except ValueError:
            continue
        anchor = nearby_words[i - 1]["text"] if i > 0 else ""
        candidates.append((value, raw, anchor))
    if not candidates:
        return "", ""
    candidates.sort(reverse=True)
    return candidates[0][1], candidates[0][2]


def _diff_for_field(
    sp: SpatialPage, row: Dict, field_name: str, page_no: int,
) -> Optional[SpatialDiff]:
    raw_val = str(row.get(field_name, ""))
    anchor = _find_word(sp, raw_val)
    if anchor is None:
        # Cannot locate the value → no useful diff.
        return None
    nearby = _nearby(sp, anchor)
    cand_val, cand_anchor = _candidate_for(field_name, nearby)
    return SpatialDiff(
        field_name=field_name,
        bl_number=str(row.get("bl_number", "")),
        container_number=str(row.get("container_number", "")),
        extracted_value=raw_val,
        page=page_no,
        nearby_words=nearby,
        candidate_value=cand_val,
        candidate_anchor=cand_anchor,
        rule_field=field_name,
    )
