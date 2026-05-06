"""Local structural analysis of a PDF — produced BEFORE any LLM call.

The fingerprint captures every fact about a manifest that we can derive
deterministically from the text + a single thumbnail page. Feeding this
to the LLM instead of the full document divides the prompt size by ~30
and the cost by ~10 while making the model's task strictly easier
(several decisions are made for it).
"""
from __future__ import annotations

import io
import re
import statistics
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..spatial_extractor import (
    load_pages,
    detect_bl_blocks,
    detect_bl_blocks_by_marker,
    block_text,
)


# ────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────
# Library of common BL number patterns. The detector keeps those that
# match at least ``_MIN_PATTERN_HITS`` times in the real document.
_BL_PATTERN_LIBRARY: List[str] = [
    r"\bABJ\d{9}\b",                  # CMA CGM Abidjan
    r"\bMEDU\w{6,}\b",                # MSC
    r"\bMAEU\w{6,}\b",                # Maersk
    r"\bONEY\w{6,}\b",                # ONE
    r"\bHLCU\w{6,}\b",                # Hapag-Lloyd
    r"\bCMDU\w{6,}\b",                # CMA
    r"\bCOSU\w{6,}\b",                # COSCO
    r"\bEGLV\w{6,}\b",                # Evergreen
    r"\bPABV\d{6,}\b",                # PIL
    r"\bGMSL\w{6,}\b",                # Grimaldi
    r"\b[A-Z]{4}\d{10}\b",            # Generic 4+10
    r"\b[A-Z]{4}\d{8,9}\b",           # Generic 4+8/9
    r"\b[A-Z]{2,3}\d{7,12}\b",        # Loose ABC123456
]

_CONTAINER_ISO_RX = re.compile(r"\b[A-Z]{4}\d{7}\b")
_CONTAINER_MARKERS: List[Tuple[str, str]] = [
    (r"\(\s*CN\s*\)\s*([A-Z]{4}\d{7})", "(CN)"),
    (r"\bCN[\s:]+([A-Z]{4}\d{7})", "CN:"),
    (r"\bCONT(?:AINER)?[\s.:N°#]+([A-Z]{4}\d{7})", "CONT:"),
]

_WEIGHT_RX = re.compile(
    r"([\d]{1,3}(?:[ ,.]\d{3})*(?:[.,]\d{1,3})?)\s*(?:KGS?|MT|TONS?|TONNES?)\b",
    re.IGNORECASE,
)

_MIN_PATTERN_HITS = 5
_MAX_SAMPLE_BLOCKS = 3
_MAX_SAMPLE_CHARS = 400
_MAX_IMAGE_BYTES = 80 * 1024
_MAX_IMAGE_WIDTH = 800


# ────────────────────────────────────────────────────────────────────────
# Dataclass
# ────────────────────────────────────────────────────────────────────────
@dataclass
class DocumentFingerprint:
    pdf_path: str
    is_scanned: bool
    format_hint: str
    bl_pattern_candidates: List[Tuple[str, int]]   # [(regex, hit_count)]
    container_marker: str                           # "(CN)" / "CN:" / "" / "ISO"
    weight_scope: str                               # "per_bl" / "per_container" / "per_page"
    sample_blocks: List[str]
    page1_image: Optional[bytes]
    total_bls: int
    total_containers: int
    full_text_chars: int

    # Convenience getters used by the prompt builder
    @property
    def best_bl_pattern(self) -> str:
        return self.bl_pattern_candidates[0][0] if self.bl_pattern_candidates else ""

    @property
    def best_bl_hits(self) -> int:
        return self.bl_pattern_candidates[0][1] if self.bl_pattern_candidates else 0

    def to_prompt_facts(self) -> str:
        """Render the pre-detected facts as a compact prompt block."""
        lines = ["## FAITS PRE-DETECTES (NE PAS REMETTRE EN CAUSE) ##"]
        if self.format_hint:
            lines.append(f"- carrier_hint     : {self.format_hint}")
        if self.best_bl_pattern:
            lines.append(
                f"- bl_pattern       : {self.best_bl_pattern} "
                f"(matche {self.best_bl_hits}x)"
            )
        if self.container_marker:
            lines.append(f"- container_marker : {self.container_marker}")
        lines.append(f"- weight_scope     : {self.weight_scope}")
        lines.append(
            f"- total_bls        : {self.total_bls}   "
            f"total_containers : {self.total_containers}"
        )
        return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────
def extract_fingerprint(
    pdf_path: str | Path,
    *,
    ocr_text: Optional[str] = None,
    is_scanned: Optional[bool] = None,
) -> DocumentFingerprint:
    """Build a DocumentFingerprint from a PDF.

    For text PDFs : reads pdfplumber directly.
    For scanned PDFs : caller must provide ``ocr_text`` (and set
    ``is_scanned=True``); page coordinates are synthesised.
    """
    pdf_path = Path(pdf_path)

    # Decide is_scanned if caller didn't.
    if is_scanned is None:
        try:
            import pdfplumber
            with pdfplumber.open(str(pdf_path)) as pdf:
                first = pdf.pages[0].extract_text() or "" if pdf.pages else ""
                is_scanned = len(first.strip()) < 200
        except Exception:
            is_scanned = False

    pages = load_pages(pdf_path, ocr_text=ocr_text, is_scanned=bool(is_scanned))
    full_text = "\n".join(
        block_text({"page": sp, "page_idx": i,
                    "y_start": min((w.top for w in sp.words), default=0),
                    "y_end":   max((w.bottom for w in sp.words), default=0)})
        for i, sp in enumerate(pages)
    ) if pages else (ocr_text or "")

    bl_candidates = _detect_bl_patterns(full_text)
    container_marker = _detect_container_marker(full_text)
    format_hint = _detect_format_hint(full_text)
    sample_blocks = _extract_sample_blocks(
        pages, bl_candidates, max_blocks=_MAX_SAMPLE_BLOCKS,
        max_chars=_MAX_SAMPLE_CHARS,
    )
    weight_scope = _detect_weight_scope(sample_blocks)
    total_bls, total_containers = _count_totals(full_text, bl_candidates)
    page1_image = _render_compact_page_image(pdf_path) if is_scanned else None

    return DocumentFingerprint(
        pdf_path=str(pdf_path),
        is_scanned=bool(is_scanned),
        format_hint=format_hint,
        bl_pattern_candidates=bl_candidates,
        container_marker=container_marker,
        weight_scope=weight_scope,
        sample_blocks=sample_blocks,
        page1_image=page1_image,
        total_bls=total_bls,
        total_containers=total_containers,
        full_text_chars=len(full_text),
    )


# ────────────────────────────────────────────────────────────────────────
# Detectors (all deterministic — no LLM)
# ────────────────────────────────────────────────────────────────────────
def _detect_bl_patterns(text: str) -> List[Tuple[str, int]]:
    """Test the BL pattern library against ``text``. Keep only patterns
    that match >= _MIN_PATTERN_HITS times. Sort by hits desc.
    """
    if not text:
        return []
    out: List[Tuple[str, int]] = []
    seen_hits: Dict[str, int] = {}
    for pat in _BL_PATTERN_LIBRARY:
        try:
            hits = len(re.findall(pat, text))
        except re.error:
            continue
        if hits >= _MIN_PATTERN_HITS:
            seen_hits[pat] = hits
    # Drop the loose generic pattern if a more specific one already
    # accounts for at least 80% of its matches.
    sorted_pats = sorted(seen_hits.items(), key=lambda x: x[1], reverse=True)
    if not sorted_pats:
        # Fallback: try a minimal generic pattern at >= 3 hits.
        loose = r"\b[A-Z]{2,4}\d{6,12}\b"
        try:
            hits = len(re.findall(loose, text))
            if hits >= 3:
                return [(loose, hits)]
        except re.error:
            pass
        return []
    out = sorted_pats[:3]
    return out


def _detect_container_marker(text: str) -> str:
    """Return the textual anchor used to introduce container numbers."""
    if not text:
        return ""
    best_label = ""
    best_hits = 0
    for pat, label in _CONTAINER_MARKERS:
        try:
            hits = len(re.findall(pat, text))
        except re.error:
            continue
        if hits > best_hits:
            best_hits = hits
            best_label = label
    if best_hits >= 3:
        return best_label
    # No anchored marker — fall back to ISO direct if many distinct
    # container numbers exist.
    iso_hits = len(set(_CONTAINER_ISO_RX.findall(text)))
    if iso_hits >= 3:
        return "ISO"
    return ""


def _detect_format_hint(text: str) -> str:
    """Best-effort carrier name guess from the first ~2000 chars."""
    head = (text or "")[:2000].upper()
    table = [
        ("MSC",  ["MEDITERRANEAN SHIPPING", "MSC GENEVA", "MSC ", "MEDU"]),
        ("CMA_CGM", ["CMA CGM", "CMA-CGM", "CMDU", "ABJ"]),
        ("MAERSK", ["MAERSK", "MAEU"]),
        ("ONE",  ["OCEAN NETWORK EXPRESS", " ONE ", "ONEY"]),
        ("HAPAG", ["HAPAG", "HLCU"]),
        ("EVERGREEN", ["EVERGREEN", "EGLV"]),
        ("COSCO", ["COSCO", "COSU"]),
        ("PIL",  [" PIL ", "PACIFIC INTERNATIONAL", "PABV"]),
        ("GRIMALDI", ["GRIMALDI", "GMSL"]),
    ]
    for name, tokens in table:
        if any(t in head for t in tokens):
            return name
    return ""


def _extract_sample_blocks(
    pages,
    bl_candidates: List[Tuple[str, int]],
    *,
    max_blocks: int = 3,
    max_chars: int = 400,
) -> List[str]:
    """Pick the first ``max_blocks`` real BL blocks, truncated to
    ``max_chars`` (cleanly cut at line boundary) each.
    """
    if not pages or not bl_candidates:
        return []
    bl_pattern = bl_candidates[0][0]
    blocks = detect_bl_blocks_by_marker(pages, bl_pattern)
    samples: List[str] = []
    for blk in blocks:
        if blk.get("carry_over"):
            continue
        text = block_text(blk).strip()
        if not text:
            continue
        if len(text) > max_chars:
            cut = text.rfind("\n", 0, max_chars)
            text = text[: cut if cut > 100 else max_chars]
        samples.append(text)
        if len(samples) >= max_blocks:
            break
    return samples


def _detect_weight_scope(sample_blocks: List[str]) -> str:
    """Decide if the weight in the document is a per-BL total or a
    per-container value. Strictly deterministic.

    Algorithm
    ---------
    For each sample block, count :
        n_containers = number of distinct ISO container numbers
        n_weights    = number of weight occurrences (NUMBER + KGS/MT/...)
    If most blocks have n_containers > 1 and n_weights == 1 → ``per_bl``.
    If n_weights ≈ n_containers across blocks                → ``per_container``.
    Default → ``per_bl`` (safer : never accidentally divides).
    """
    if not sample_blocks:
        return "per_bl"
    per_bl_votes = 0
    per_container_votes = 0
    for blk in sample_blocks:
        containers = set(_CONTAINER_ISO_RX.findall(blk))
        weights = _WEIGHT_RX.findall(blk)
        n_c = len(containers)
        n_w = len(weights)
        if n_c <= 1:
            # Ambiguous block, ignore.
            continue
        if n_w <= 1:
            per_bl_votes += 1
        elif n_c == n_w:
            per_container_votes += 1
        else:
            # More weights than containers (totals + per-container) — still
            # safer to treat as per_bl unless ratio is exactly 1.
            per_bl_votes += 1
    if per_container_votes > per_bl_votes:
        return "per_container"
    return "per_bl"


def _count_totals(
    text: str, bl_candidates: List[Tuple[str, int]],
) -> Tuple[int, int]:
    if not text:
        return 0, 0
    if bl_candidates:
        try:
            total_bls = len(set(re.findall(bl_candidates[0][0], text)))
        except re.error:
            total_bls = 0
    else:
        total_bls = 0
    total_containers = len(set(_CONTAINER_ISO_RX.findall(text)))
    return total_bls, total_containers


# ────────────────────────────────────────────────────────────────────────
# Page thumbnail (only for scanned docs)
# ────────────────────────────────────────────────────────────────────────
def _render_compact_page_image(
    pdf_path: Path, *, page_idx: int = 0,
) -> Optional[bytes]:
    """Render ``page_idx`` (0-based) at low DPI then resize+JPEG it down
    to ≤ 80 KB. Used only for scanned PDFs.
    """
    try:
        from pdf2image.pdf2image import convert_from_path
        from ..paths import poppler_bin
        from PIL import Image
    except Exception:
        return None
    try:
        imgs = convert_from_path(
            str(pdf_path),
            dpi=100,
            first_page=page_idx + 1,
            last_page=page_idx + 1,
            poppler_path=poppler_bin(),
        )
    except Exception:
        return None
    if not imgs:
        return None
    img = imgs[0]
    # Resize so width ≤ _MAX_IMAGE_WIDTH while keeping aspect.
    if img.width > _MAX_IMAGE_WIDTH:
        ratio = _MAX_IMAGE_WIDTH / float(img.width)
        new_size = (_MAX_IMAGE_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)
    # JPEG with progressive quality reduction until ≤ _MAX_IMAGE_BYTES.
    for q in (75, 60, 50, 40, 30):
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=q, optimize=True)
        data = buf.getvalue()
        if len(data) <= _MAX_IMAGE_BYTES:
            return data
    return data  # last attempt, may exceed budget but caller can decide
