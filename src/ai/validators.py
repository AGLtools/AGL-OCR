"""Detection of likely extraction issues in ManifestRow dicts.

Used by:
- the review dialog (highlight rows + show issues column)
- the AI auto-fix pipeline (only rows with issues are sent for correction)

Validation is intentionally LENIENT: we flag obvious problems only, not
every missing optional field. The goal is to surface OCR/parsing failures,
not nag about empty seals.
"""
from __future__ import annotations
import re
from typing import Dict, List

# ISO 6346 container number: 4 letters + 7 digits (last digit is a check digit
# we don't validate strictly — accept format only).
_CONTAINER_RE = re.compile(r"^[A-Z]{4}\d{7}$")
_BL_RE = re.compile(r"^[A-Z0-9][A-Z0-9_/-]{3,}$")
_NUM_RE = re.compile(r"^\d+([.,]\d+)?$")
_DATE_RE = re.compile(r"^\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}$")


# Fields considered MANDATORY for a usable MIDAS line
_MANDATORY = ("vessel", "bl_number", "port_of_discharge")
# Fields important for cargo analytics (warning level)
_IMPORTANT = ("shipper", "consignee", "description", "weight")


def validate_row(row: Dict) -> List[str]:
    """Return a list of human-readable issue strings (FR). Empty = OK."""
    issues: List[str] = []

    # Mandatory presence
    for k in _MANDATORY:
        if not _nonempty(row.get(k)):
            issues.append(f"{_label(k)} manquant")

    # Format checks
    bl = _clean(row.get("bl_number"))
    if bl and not _BL_RE.match(bl):
        issues.append(f"BL au format suspect : '{bl}'")

    cn = _clean(row.get("container_number")).upper().replace(" ", "")
    if cn and not _CONTAINER_RE.match(cn):
        issues.append(f"N° conteneur non-ISO : '{cn}'")

    w = _clean(row.get("weight"))
    if w and not _NUM_RE.match(w):
        issues.append(f"Poids non numérique : '{w}'")

    d = _clean(row.get("date_of_arrival"))
    if d and not _DATE_RE.match(d):
        issues.append(f"Date d'arrivée mal formée : '{d}'")

    # Important but not blocking
    for k in _IMPORTANT:
        if not _nonempty(row.get(k)):
            issues.append(f"{_label(k)} vide")

    return issues


def validate_rows(rows: List[Dict]) -> Dict[int, List[str]]:
    """Return {row_index: [issues]} for rows with at least one issue."""
    out: Dict[int, List[str]] = {}
    for i, r in enumerate(rows):
        iss = validate_row(r)
        if iss:
            out[i] = iss
    return out


# ── helpers ────────────────────────────────────────────────────────────────
def _nonempty(v) -> bool:
    return bool((v or "").strip()) if isinstance(v, str) else v not in (None, "", 0)


def _clean(v) -> str:
    return (v or "").strip() if isinstance(v, str) else str(v or "").strip()


_LABELS = {
    "vessel": "Navire",
    "bl_number": "N° BL",
    "port_of_discharge": "Port de déchargement",
    "shipper": "Expéditeur",
    "consignee": "Destinataire",
    "description": "Description",
    "weight": "Poids",
    "container_number": "N° conteneur",
    "date_of_arrival": "Date d'arrivée",
}


def _label(key: str) -> str:
    return _LABELS.get(key, key)
