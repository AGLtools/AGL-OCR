"""Local PDF parser driven by an AI-generated `parse_template`.

Goal: once the AI has LEARNED a format and produced a parse template,
extraction of subsequent documents of the same format runs ENTIRELY LOCALLY
(no Gemini call) — fast, deterministic, free.

Template schema (saved on the learned format JSON under "parse_template") :
{
  "header_field_patterns": {
      "vessel": "regex with ONE capture group",
      "voyage": "regex with ONE capture group",
      "date_of_arrival": "regex with ONE capture group",
      "shipowner": "regex with ONE capture group"   // optional, can be a literal
  },
  "row_patterns": [                                  // ordered, first match wins
      "(?P<bl_number>MEDU\\d+)\\s+(?P<container_number>[A-Z]{4}\\d{7})\\s+(?P<container_type>\\d{2}[A-Z]{2})\\s+(?P<weight>[\\d,. ]+)\\s+(?P<weight_unit>KGS?|MT|TONS?)\\s+(?P<pack_qty>\\d+)\\s+(?P<pack_unit>[A-Z]+)"
  ],
  "shipowner": "MSC"                                 // literal default, overrides regex if present
}

Each row regex MUST use NAMED GROUPS with names matching the manifest schema
(bl_number, container_number, container_type, shipper, consignee, ...).

The parser:
  1. Reads the PDF text via pdfplumber (per page).
  2. Extracts header fields by running each header regex against the WHOLE document.
  3. Walks through every line and tries each row regex. First match → emits a row
     dict (header fields auto-merged in).
  4. Returns list[dict] in the same shape as ai_extractor.extract_rows_from_pdf.

If the template returns < 1 row the caller should propose AI fallback.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, List, Optional


# Recognised row-level field names (all optional in the template)
_ROW_FIELDS = {
    "bl_number", "bl_type", "shipper", "consignee", "notify",
    "freight_forwarder", "port_of_loading", "port_of_discharge",
    "place_of_delivery", "place_of_acceptance",
    "container_number", "container_type",
    "seal1", "seal2", "seal3",
    "weight", "weight_unit", "pack_qty", "pack_unit",
    "volume", "volume_unit", "description",
}


def parse_with_template(
    pdf_path: str | Path,
    template: Dict,
    *,
    progress_cb=None,
    text_override: Optional[str] = None,
) -> List[Dict]:
    """Parse a PDF locally using a learned template. Returns list of row dicts.

    If `text_override` is supplied, it is used directly INSTEAD of reading
    the PDF via pdfplumber. This is the path used for SCANNED documents:
    the caller (UI) OCRs the PDF first via Cloud Vision, then passes the
    resulting text here so the AI-generated parser can run on it.
    """
    pdf_path = Path(pdf_path)
    if not template:
        return []

    if text_override is not None:
        full_text = text_override
        pages_text = full_text.split("\f") if "\f" in full_text else [full_text]
    else:
        pages_text = _read_pages(pdf_path)
        if not pages_text:
            return []
        full_text = "\n".join(pages_text)

    # --- Header fields ---
    header = _extract_header(full_text, template)

    # --- Mode 0: SPATIAL TEMPLATE (preferred — robust, no regex code) ---
    spatial_rules = template.get("spatial_rules") or template.get("spatial_template")
    if spatial_rules:
        try:
            from ..spatial_template import SpatialTemplate
            from ..spatial_parser import parse_with_spatial_template
            # The spatial descriptor may live either at parse_template root
            # (legacy) or under the "spatial_template" key. Accept both.
            descriptor = (
                spatial_rules
                if isinstance(spatial_rules, dict) and "field_rules" in spatial_rules
                else {**template, "field_rules": spatial_rules}
            )
            st = SpatialTemplate.from_dict(descriptor)
            if progress_cb:
                progress_cb("Parser local (template spatial)…")
            rows = parse_with_spatial_template(
                pdf_path, st,
                ocr_text=text_override if st.is_scanned else None,
                progress_cb=progress_cb,
            )
            if rows:
                # Merge header (from regex header_field_patterns) as fallback
                # for any field the spatial rules left empty.
                for r in rows:
                    for k, v in header.items():
                        r.setdefault(k, v)
                    r.setdefault("source_file", str(pdf_path))
                return rows
            # Spatial parser returned 0 → fall through to legacy modes
            if progress_cb:
                progress_cb("Template spatial vide — bascule sur parse_code legacy.")
        except Exception as e:
            if progress_cb:
                progress_cb(f"Template spatial en echec ({e}) — fallback parse_code.")

    # --- Mode 1: AI-generated parse(text) Python function (legacy) ---
    parse_code = (template.get("parse_code") or "").strip()
    if parse_code:
        if progress_cb:
            progress_cb("Parser local (code IA)…")
        rows = run_parse_code(parse_code, full_text)
        for r in rows:
            for k, v in header.items():
                r.setdefault(k, v)
            r.setdefault("source_file", str(pdf_path))
        return rows

    # --- Mode 2 (legacy): row_patterns regex line-by-line ---
    row_patterns = _compile_row_patterns(template.get("row_patterns") or [])
    if not row_patterns:
        return []

    rows: List[Dict] = []
    for page_idx, page_text in enumerate(pages_text, 1):
        if progress_cb:
            progress_cb(f"Parser local — page {page_idx}/{len(pages_text)}…")
        for line in page_text.splitlines():
            line_clean = line.strip()
            if not line_clean:
                continue
            for rx in row_patterns:
                m = rx.search(line_clean)
                if not m:
                    continue
                gd = {k: (v.strip() if isinstance(v, str) else v)
                      for k, v in m.groupdict().items()
                      if v is not None and k in _ROW_FIELDS}
                if not gd:
                    continue
                row = {**header, **gd}
                # Stamp source for traceability
                row.setdefault("source_file", str(pdf_path))
                row["page"] = page_idx
                rows.append(row)
                break  # first matching pattern wins
    return rows


# ────────────────────────────────────────────────────────────────────────
# Internals
# ────────────────────────────────────────────────────────────────────────
def _read_pages(pdf_path: Path) -> List[str]:
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return []
    try:
        out: List[str] = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for p in pdf.pages:
                out.append(p.extract_text() or "")
        return out
    except Exception:
        return []


def _extract_header(text: str, template: Dict) -> Dict[str, str]:
    """Apply each header_field_patterns regex once on the full text."""
    header: Dict[str, str] = {}
    patterns = template.get("header_field_patterns") or {}
    for field, pat in patterns.items():
        if not pat:
            continue
        try:
            rx = re.compile(pat, re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        m = rx.search(text)
        if m and m.groups():
            val = (m.group(1) or "").strip()
            if val:
                header[field] = val
    # Literal shipowner override
    so = template.get("shipowner")
    if so:
        header.setdefault("_shipowner", so)
    return header


def _compile_row_patterns(patterns) -> List[re.Pattern]:
    out: List[re.Pattern] = []
    for p in patterns:
        if not p:
            continue
        try:
            out.append(re.compile(p, re.IGNORECASE))
        except re.error:
            # silently skip invalid patterns — parser remains usable with the rest
            continue
    return out


def template_is_usable(template: Optional[Dict]) -> bool:
    """True if the template has spatial rules, a parse_code function,
    OR at least one valid row pattern.
    """
    if not template:
        return False
    if template.get("spatial_rules") or template.get("spatial_template"):
        return True
    if (template.get("parse_code") or "").strip():
        return True
    return bool(_compile_row_patterns(template.get("row_patterns") or []))


# ────────────────────────────────────────────────────────────────────────
# AI-generated parse(text) execution (sandboxed)
# ────────────────────────────────────────────────────────────────────────
_SAFE_BUILTINS = {
    "abs", "all", "any", "bool", "dict", "enumerate", "filter", "float",
    "int", "isinstance", "len", "list", "map", "max", "min", "next",
    "range", "reversed", "set", "sorted", "str", "sum", "tuple", "zip",
    "True", "False", "None", "print",
}


def run_parse_code(code: str, text: str) -> List[Dict]:
    """Exec AI-generated `parse(text) -> list[dict]` in a restricted namespace.

    Returns [] on any error (compile, runtime, wrong return type).
    """
    import builtins as _b
    safe_builtins = {n: getattr(_b, n) for n in _SAFE_BUILTINS if hasattr(_b, n)}
    glb: Dict = {"__builtins__": safe_builtins, "re": re}
    try:
        exec(compile(code, "<learned_parser>", "exec"), glb)
    except Exception:
        return []
    fn = glb.get("parse")
    if not callable(fn):
        return []
    try:
        import threading
        result: list = []
        exc: list = []

        def _run():
            try:
                out = fn(text)
                if isinstance(out, list):
                    result.extend(out)
            except Exception as e:
                exc.append(e)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=10.0)  # max 10 s for the generated parser
        if t.is_alive() or exc:
            return []
    except Exception:
        return []
    rows: List[Dict] = []
    for item in result:
        if isinstance(item, dict):
            # keep only known row fields + page (if provided)
            clean = {k: v for k, v in item.items()
                     if k in _ROW_FIELDS or k == "page"}
            if clean:
                rows.append(clean)
    return rows
