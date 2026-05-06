"""Declarative spatial-extraction template.

A SpatialTemplate is a pure-data description of HOW to extract fields from
a manifest, expressed as spatial relationships (anchor word + direction)
rather than as Python regex code. The extraction engine in
``spatial_parser.py`` consumes a SpatialTemplate to produce row dicts.

Why this exists
---------------
Generated regex parsers are brittle: pdfplumber merges columns into single
text lines, OCR introduces character noise, and weight totals end up being
"helpfully" divided across containers. Spatial rules are robust because:

- They use the exact (x, y) coordinates pdfplumber emits per word.
- The same descriptor works for text PDFs (exact coords) and scanned ones
  (coords estimated from line index).
- ``weight_scope`` makes the per-BL vs per-container semantics explicit,
  killing the "weight divided" bug at the schema level.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any


# ────────────────────────────────────────────────────────────────────────
# Field rule
# ────────────────────────────────────────────────────────────────────────
@dataclass
class SpatialRule:
    """One field extraction rule, defined by spatial relationship.

    Attributes
    ----------
    field_name
        Name of the row dict key (must be in the manifest schema, e.g.
        ``port_of_loading``, ``weight``, ``shipper``).
    anchor_text
        Tokens of the label to find (case-insensitive, fuzzy match).
        For example ``["Port", "of", "loading"]``.
    direction
        Where the value sits relative to the anchor :
        ``right`` | ``below`` | ``left`` | ``above``.
    max_distance
        Maximum search distance in pixels (or normalized units, see
        ``normalized``).
    x_tolerance / y_tolerance
        Cross-axis slack when collecting words.
    max_words
        How many tokens to concatenate as the value.
    stop_at
        Optional list of tokens that, if encountered while collecting,
        stop the value capture (e.g. another label).
    fuzzy_threshold
        Minimum rapidfuzz score to accept a token as the anchor (0-100).
    scope
        ``page`` (default) — search whole page.
        ``bl_block`` — restrict search to the current BL block when called
        from within a per-BL extraction loop.
    normalized
        If True, ``max_distance`` / ``x_tolerance`` / ``y_tolerance`` are
        fractions of page width / height (0-1) — useful for templates
        learned from a different DPI than the one being parsed.
    regex_clean
        Optional regex applied to the captured string with one group;
        the group's value replaces the raw capture.
    """

    field_name: str
    anchor_text: List[str]
    direction: str = "right"
    max_distance: float = 250.0
    x_tolerance: float = 40.0
    y_tolerance: float = 4.0
    max_words: int = 8
    stop_at: List[str] = field(default_factory=list)
    fuzzy_threshold: int = 80
    scope: str = "page"
    normalized: bool = False
    regex_clean: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SpatialRule":
        # Tolerate aliases used in older / hand-written templates.
        anchor = d.get("anchor_text") or d.get("anchor") or d.get("label")
        if isinstance(anchor, str):
            anchor = anchor.split()
        return cls(
            field_name=d["field_name"],
            anchor_text=list(anchor or []),
            direction=str(d.get("direction") or "right").lower(),
            max_distance=float(d.get("max_distance", 250.0)),
            x_tolerance=float(d.get("x_tolerance", 40.0)),
            y_tolerance=float(d.get("y_tolerance", 4.0)),
            max_words=int(d.get("max_words", 8)),
            stop_at=list(d.get("stop_at") or []),
            fuzzy_threshold=int(d.get("fuzzy_threshold", 80)),
            scope=str(d.get("scope") or "page").lower(),
            normalized=bool(d.get("normalized", False)),
            regex_clean=str(d.get("regex_clean") or ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ────────────────────────────────────────────────────────────────────────
# Template
# ────────────────────────────────────────────────────────────────────────
@dataclass
class SpatialTemplate:
    """Declarative descriptor of a manifest format.

    Attributes
    ----------
    format_name
        Short uppercase name (``MSC``, ``PIL``, ``CMA_CGM`` …).
    carrier
        Long form carrier name.
    signature_keywords
        Distinctive tokens used by the format detector to recognise this
        layout in a new document.
    is_scanned
        ``True`` if the format is typically a scanned image (impacts how
        coordinates are obtained — direct pdfplumber vs estimated).
    bl_marker_pattern
        Regex that, applied to the document text, captures one BL number
        per match. Used to slice the document into per-BL blocks. Group 1
        (or the whole match if no group) is taken as the BL number.
    bl_split_strategy
        How to delimit blocks :
        - ``marker`` : split on every occurrence of ``bl_marker_pattern``
          (each block runs from one BL number to the next).
        - ``gap`` : cluster on vertical whitespace gaps between text lines.
        - ``page`` : one block per PDF page.
    container_marker
        Regex for the container number with a single capture group. e.g.
        ``r'\\(\\s*CN\\s*\\)\\s*([A-Z]{4}\\d{7})'`` or
        ``r'CN:\\s*([A-Z]{4}\\d{7})'``. May be empty for non-containerised
        manifests.
    weight_scope
        ``per_bl`` (default) → the weight in the source text is the BL
        total ; the same value is assigned to every container row of that
        BL. NEVER divide.
        ``per_container`` → each container has its own weight value.
        ``per_page`` → one weight per page applies to all containers on it.
    field_rules
        Spatial rules for header / row fields.
    extra
        Free-form bag for format-specific hints (raw text patterns, hand
        notes, etc.) that the engine may consult.
    """

    format_name: str
    carrier: str = ""
    signature_keywords: List[str] = field(default_factory=list)
    is_scanned: bool = False
    bl_marker_pattern: str = ""
    bl_split_strategy: str = "marker"
    container_marker: str = ""
    weight_scope: str = "per_bl"
    field_rules: List[SpatialRule] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SpatialTemplate":
        rules_raw = d.get("field_rules") or d.get("spatial_rules") or []
        rules = [SpatialRule.from_dict(r) for r in rules_raw]
        return cls(
            format_name=str(d.get("format_name") or d.get("name") or ""),
            carrier=str(d.get("carrier") or d.get("carrier_name") or ""),
            signature_keywords=list(d.get("signature_keywords") or d.get("signature") or []),
            is_scanned=bool(d.get("is_scanned", False)),
            bl_marker_pattern=str(d.get("bl_marker_pattern") or ""),
            bl_split_strategy=str(d.get("bl_split_strategy") or "marker").lower(),
            container_marker=str(d.get("container_marker") or ""),
            weight_scope=str(d.get("weight_scope") or "per_bl").lower(),
            field_rules=rules,
            extra=dict(d.get("extra") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format_name": self.format_name,
            "carrier": self.carrier,
            "signature_keywords": self.signature_keywords,
            "is_scanned": self.is_scanned,
            "bl_marker_pattern": self.bl_marker_pattern,
            "bl_split_strategy": self.bl_split_strategy,
            "container_marker": self.container_marker,
            "weight_scope": self.weight_scope,
            "field_rules": [r.to_dict() for r in self.field_rules],
            "extra": self.extra,
        }

    def get_rule(self, field_name: str) -> Optional[SpatialRule]:
        return next((r for r in self.field_rules if r.field_name == field_name), None)


# ────────────────────────────────────────────────────────────────────────
# Validation
# ────────────────────────────────────────────────────────────────────────
def is_usable(template: Optional[SpatialTemplate]) -> bool:
    """A template is usable if it has either a BL marker or at least one
    extraction rule. Without a BL marker we can still extract header
    fields, so a few rules alone are enough.
    """
    if template is None:
        return False
    if template.bl_marker_pattern.strip():
        return True
    return bool(template.field_rules)
