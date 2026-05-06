"""High-level orchestrator that turns a SpatialTemplate + a PDF into rows.

Public API
----------
``parse_with_spatial_template(pdf_path, template, ocr_text=None) -> list[dict]``

Pipeline
--------
1. Load pages (text PDF → pdfplumber coords ; scanned → estimated coords
   from ``ocr_text``) via :mod:`spatial_extractor`.
2. Pull header fields once at the document level using rules with
   ``scope='page'``.
3. Slice the document into BL blocks using ``template.bl_marker_pattern``
   and ``template.bl_split_strategy``.
4. For each BL block:
       a. Apply rules with ``scope='bl_block'`` to get per-BL fields
          (shipper, consignee, weight, volume, …).
       b. Find every container number with ``template.container_marker``.
       c. Emit one row per container, applying the ``weight_scope``
          policy — for ``per_bl`` the SAME weight goes on every row,
          NEVER divided.
       d. If no container is found, emit a single row for the BL.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Iterable

from .spatial_template import SpatialTemplate, SpatialRule
from .spatial_extractor import (
    SpatialPage,
    load_pages,
    detect_bl_blocks,
    block_view,
    block_text,
    rebuild_text_from_pages,
)


# Recognised row field names — anything else gets dropped silently.
_ROW_FIELDS = {
    "bl_number", "bl_type", "shipper", "consignee", "notify",
    "freight_forwarder",
    "port_of_loading", "port_of_discharge",
    "place_of_delivery", "place_of_acceptance",
    "container_number", "container_type",
    "seal1", "seal2", "seal3",
    "weight", "weight_unit", "pack_qty", "pack_unit",
    "volume", "volume_unit", "description",
    "vessel", "voyage", "date_of_arrival",
}


# ────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────
def parse_with_spatial_template(
    pdf_path: str | Path,
    template: SpatialTemplate,
    *,
    ocr_text: Optional[str] = None,
    progress_cb=None,
) -> List[Dict]:
    """Extract all rows from a PDF using a SpatialTemplate.

    Parameters
    ----------
    pdf_path
        Path to the PDF file (text-native or scanned).
    template
        The SpatialTemplate to apply.
    ocr_text
        For scanned PDFs : the OCR'd text (with optional ``=== PAGE N ===``
        markers). Pages will be synthesised from it. If omitted on a scan,
        the parser will return ``[]``.
    """
    pages = load_pages(
        pdf_path,
        ocr_text=ocr_text,
        is_scanned=template.is_scanned,
    )
    if not pages:
        return []

    if progress_cb:
        progress_cb(f"Spatial parser : {len(pages)} page(s) chargee(s).")

    # 1) Header / document-level fields
    header = _extract_header_fields(pages, template)

    # 2) BL blocks
    blocks = detect_bl_blocks(
        pages,
        strategy=template.bl_split_strategy,
        bl_pattern=template.bl_marker_pattern,
    )
    if progress_cb:
        progress_cb(f"Spatial parser : {len(blocks)} bloc(s) BL detecte(s).")

    if not blocks:
        # Fall back: treat the whole document as one synthetic block.
        if pages:
            blocks = [{
                "bl_number": "",
                "page_idx": 0,
                "y_start": 0.0,
                "y_end": 1e9,
                "page": pages[0],
                "carry_over": False,
            }]

    # 3) Rows
    rows: List[Dict] = []
    container_rx = _compile(template.container_marker)
    for block in blocks:
        bl_rows = _emit_rows_for_block(
            block=block,
            template=template,
            container_rx=container_rx,
            header=header,
            pdf_path=str(pdf_path),
        )
        rows.extend(bl_rows)

    return rows


# ────────────────────────────────────────────────────────────────────────
# Internals
# ────────────────────────────────────────────────────────────────────────
def _compile(pattern: str) -> Optional[re.Pattern]:
    if not pattern:
        return None
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error:
        return None


def _extract_header_fields(
    pages: List[SpatialPage], template: SpatialTemplate,
) -> Dict[str, str]:
    """Apply page-scope rules across the document. First non-empty wins."""
    header: Dict[str, str] = {}
    page_rules = [r for r in template.field_rules if r.scope != "bl_block"]
    if not page_rules:
        return header
    for sp in pages:
        for rule in page_rules:
            if header.get(rule.field_name):
                continue
            val = _apply_rule(sp, rule)
            if val:
                header[rule.field_name] = val
    return header


def _emit_rows_for_block(
    *,
    block: Dict,
    template: SpatialTemplate,
    container_rx: Optional[re.Pattern],
    header: Dict[str, str],
    pdf_path: str,
) -> List[Dict]:
    """Produce one row per container in this BL block (or one row total
    if the BL has no containers).
    """
    sub_page = block_view(block)
    sub_text = block_text(block)

    # Per-BL fields
    bl_data: Dict = {}
    for rule in template.field_rules:
        if rule.scope != "bl_block":
            continue
        val = _apply_rule(sub_page, rule)
        if val:
            bl_data[rule.field_name] = val

    # Inject the BL number from the marker if the template didn't define
    # an explicit rule for it.
    if block.get("bl_number") and not bl_data.get("bl_number"):
        bl_data["bl_number"] = block["bl_number"]

    # Weight & numeric normalisation (string → number when obvious)
    _coerce_numeric(bl_data, "weight")
    _coerce_numeric(bl_data, "volume")
    _coerce_numeric(bl_data, "pack_qty", as_int=True)

    # Containers
    containers: List[str] = []
    if container_rx:
        for m in container_rx.finditer(sub_text):
            cn = (m.group(1) if m.groups() else m.group(0)).strip().upper()
            if cn and cn not in containers:
                containers.append(cn)

    rows: List[Dict] = []
    if containers:
        weight_scope = (template.weight_scope or "per_bl").lower()
        for cn in containers:
            row = {**header, **bl_data}
            row["container_number"] = cn
            row["page"] = block["page_idx"] + 1
            row.setdefault("source_file", pdf_path)
            # Weight policy
            if weight_scope == "per_container":
                # The bl_data weight is treated as the per-container value
                # already (template author's responsibility).
                pass
            else:
                # per_bl / per_page → keep the same value on each row.
                # Explicitly DO NOT divide.
                pass
            rows.append(_clean_row(row))
    elif bl_data:
        row = {**header, **bl_data}
        row["page"] = block["page_idx"] + 1
        row.setdefault("source_file", pdf_path)
        rows.append(_clean_row(row))
    return rows


def _apply_rule(sp: SpatialPage, rule: SpatialRule) -> str:
    """Resolve one SpatialRule against a SpatialPage.

    Falls back gracefully when the anchor isn't found.
    """
    if not rule.anchor_text:
        return ""

    label = " ".join(rule.anchor_text)
    # Try exact first, then fuzzy (handles OCR noise on scanned docs).
    anchor = sp.find_label(label)
    if not anchor:
        anchor = sp.find_label_fuzzy(label, threshold=rule.fuzzy_threshold)
    if not anchor:
        # For multi-token labels, try a fuzzy match on the longest token.
        if len(rule.anchor_text) > 1:
            longest = max(rule.anchor_text, key=len)
            anchor = sp.find_label_fuzzy(longest, threshold=rule.fuzzy_threshold)
    if not anchor:
        return ""

    direction = (rule.direction or "right").lower()
    if direction == "below":
        val = sp.value_below(
            anchor,
            max_dy=rule.max_distance,
            x_tolerance=rule.x_tolerance,
            max_words=rule.max_words,
        )
    elif direction == "right":
        val = sp.value_right(
            anchor,
            max_dx=rule.max_distance,
            y_tolerance=rule.y_tolerance,
            max_words=rule.max_words,
        )
    elif direction == "above":
        val = sp.value_above(
            anchor,
            max_dy=rule.max_distance,
            x_tolerance=rule.x_tolerance,
            max_words=rule.max_words,
        )
    else:
        val = ""

    if not val:
        return ""

    # Stop-at filter
    if rule.stop_at:
        upper = val.upper()
        cuts = [upper.find(s.upper()) for s in rule.stop_at]
        cuts = [c for c in cuts if c >= 0]
        if cuts:
            val = val[:min(cuts)].strip()

    # Optional regex_clean
    if rule.regex_clean:
        try:
            m = re.search(rule.regex_clean, val)
            if m:
                val = (m.group(1) if m.groups() else m.group(0)).strip()
        except re.error:
            pass

    return val.strip()


def _coerce_numeric(d: Dict, key: str, *, as_int: bool = False) -> None:
    """Convert ``d[key]`` from string ('22,754.000 KGS') to a number."""
    v = d.get(key)
    if v is None or isinstance(v, (int, float)):
        return
    s = str(v).strip()
    # Strip trailing unit tokens
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return
    # Normalise European decimals if there's a single comma and no dot
    if s.count(",") == 1 and s.count(".") == 0 and len(s.split(",")[1]) <= 3:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        n = float(s)
        d[key] = int(n) if as_int else n
    except ValueError:
        pass


def _clean_row(row: Dict) -> Dict:
    """Drop unknown keys and empty strings to match the canonical schema."""
    out: Dict = {}
    for k, v in row.items():
        if k == "page" or k == "source_file" or k == "_shipowner":
            out[k] = v
            continue
        if k not in _ROW_FIELDS:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out
