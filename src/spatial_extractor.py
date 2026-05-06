"""Spatial extraction engine: builds a list of SpatialPage objects from a
PDF (text-native or scanned) and provides high-level operations like BL
block detection.

This module is the "physical layer" of the spatial extraction stack:

    spatial_template.py  ← what to extract  (declarative)
    spatial_extractor.py ← how to read the page  (this file)
    spatial_parser.py    ← orchestrates template + extractor  (next file)

For text PDFs we feed pdfplumber's exact word coordinates into the existing
``spatial_index.SpatialPage`` (which already has find_label / value_below /
value_right / words_in_box). For scanned PDFs we accept a pre-OCR'd text
string and synthesise approximate coordinates from the line index — this
lets the same template work in both regimes.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Iterable, Dict, Tuple

from .spatial_index import SpatialPage, Word


# ────────────────────────────────────────────────────────────────────────
# Page loaders
# ────────────────────────────────────────────────────────────────────────
def load_text_pdf_pages(pdf_path: str | Path) -> List[SpatialPage]:
    """Open a text-native PDF and return one SpatialPage per page.

    Empty pages (no extracted words) are skipped. Returns ``[]`` if the
    file can't be opened or produces no spatial words.
    """
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return []

    pages: List[SpatialPage] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for p in pdf.pages:
                sp = SpatialPage.from_pdfplumber_page(p, x_tolerance=3, y_tolerance=3)
                if sp.words:
                    pages.append(sp)
    except Exception:
        return []
    return pages


def synthesise_pages_from_text(text: str) -> List[SpatialPage]:
    """For SCANNED PDFs: build SpatialPage objects from OCR'd text.

    Coordinates are estimated from the line index and the word position
    within the line. The unit is unimportant as long as it's consistent —
    spatial relationships (right of, below, etc.) work on relative
    distances.

    The OCR text is split on the page-break markers our OCR helpers emit
    (``=== PAGE N ===``). If no markers are found, the whole text is a
    single page.

    Coordinate convention (matches pdfplumber):
        x : column index × 6  (≈ 6 pixels per character)
        y : line index  × 12  (≈ 12 pixels per line)
    """
    if not text:
        return []

    # Split on our standard OCR page markers.
    raw_pages: List[str]
    if "=== PAGE" in text:
        # Keep the content AFTER each marker.
        parts = re.split(r"=== PAGE \d+ ===\n?", text)
        raw_pages = [p for p in parts if p.strip()]
    else:
        raw_pages = [text]

    pages: List[SpatialPage] = []
    char_w = 6.0
    line_h = 12.0
    for raw in raw_pages:
        words: List[Word] = []
        for line_idx, line in enumerate(raw.splitlines()):
            top = line_idx * line_h
            bottom = top + line_h - 1
            # Walk char-by-char to find token x positions
            col = 0
            for tok in re.finditer(r"\S+", line):
                x0 = tok.start() * char_w
                x1 = tok.end() * char_w
                words.append(Word(
                    text=tok.group(0),
                    x0=x0, top=top, x1=x1, bottom=bottom,
                ))
                col = tok.end()
        if words:
            pages.append(SpatialPage(words))
    return pages


def load_pages(
    pdf_path: str | Path,
    *,
    ocr_text: Optional[str] = None,
    is_scanned: bool = False,
) -> List[SpatialPage]:
    """Smart loader: text-native first, OCR-synthesised fallback.

    - If ``ocr_text`` is provided AND ``is_scanned`` is True → synthesise
      pages from the OCR text (don't bother opening the PDF).
    - Else try pdfplumber; if it returns no words AND ``ocr_text`` was
      supplied, fall back to OCR synthesis.
    - Else return whatever pdfplumber produced.
    """
    if is_scanned and ocr_text:
        return synthesise_pages_from_text(ocr_text)
    pages = load_text_pdf_pages(pdf_path)
    if not pages and ocr_text:
        return synthesise_pages_from_text(ocr_text)
    return pages


# ────────────────────────────────────────────────────────────────────────
# Full-document text rebuild (for regex scopes like bl_marker_pattern)
# ────────────────────────────────────────────────────────────────────────
def rebuild_text_from_pages(pages: List[SpatialPage]) -> str:
    """Return the document's text in visual reading order (top→bottom,
    left→right per line). Pages are separated by ``\\f``.
    """
    out_pages: List[str] = []
    for sp in pages:
        lines = sp._group_by_lines(y_tol=4.0)  # type: ignore[attr-defined]
        out_pages.append(
            "\n".join(" ".join(w.text for w in line) for line in lines)
        )
    return "\f".join(out_pages)


# ────────────────────────────────────────────────────────────────────────
# BL block detection
# ────────────────────────────────────────────────────────────────────────
def detect_bl_blocks_by_marker(
    pages: List[SpatialPage],
    bl_pattern: str,
) -> List[Dict]:
    """Slice the document into per-BL spatial blocks using a regex marker.

    Each block is ``{"bl_number": str, "page_idx": int, "y_start": float,
    "y_end": float, "page": SpatialPage}``.

    Strategy
    --------
    1. Compile ``bl_pattern``. The first capture group (or whole match if
       no group) is the BL number.
    2. For each page, find every word whose text matches the pattern.
    3. Block i runs from match i's ``top`` to match i+1's ``top`` on the
       same page; on the last match of a page, it runs to the page bottom.
    4. The first block on a new page may inherit the BL number of the
       last block of the previous page if no new marker was found before
       data starts (this handles BL records spilling across pages — see
       ``carry_over``).
    """
    if not bl_pattern:
        return []
    try:
        rx = re.compile(bl_pattern, re.IGNORECASE)
    except re.error:
        return []

    blocks: List[Dict] = []
    last_bl: Optional[str] = None

    for page_idx, sp in enumerate(pages):
        # Find all anchor words on this page (sorted top→bottom)
        anchors: List[Tuple[Word, str]] = []
        for w in sorted(sp.words, key=lambda w: (w.top, w.x0)):
            m = rx.search(w.text)
            if m:
                bl = (m.group(1) if m.groups() else m.group(0)).strip()
                if bl:
                    anchors.append((w, bl))

        page_top = min((w.top for w in sp.words), default=0.0)
        page_bot = max((w.bottom for w in sp.words), default=0.0)

        if not anchors:
            # No marker on this page — extend the last block to cover this
            # page, if any.
            if last_bl and blocks:
                blocks.append({
                    "bl_number": last_bl,
                    "page_idx": page_idx,
                    "y_start": page_top,
                    "y_end": page_bot,
                    "page": sp,
                    "carry_over": True,
                })
            continue

        for i, (anchor, bl) in enumerate(anchors):
            y_start = anchor.top
            if i + 1 < len(anchors):
                y_end = anchors[i + 1][0].top
            else:
                y_end = page_bot + 1
            blocks.append({
                "bl_number": bl,
                "page_idx": page_idx,
                "y_start": y_start,
                "y_end": y_end,
                "page": sp,
                "carry_over": False,
            })
            last_bl = bl

    return blocks


def detect_bl_blocks_by_page(pages: List[SpatialPage]) -> List[Dict]:
    """One block per page (used when no marker is provided)."""
    out: List[Dict] = []
    for page_idx, sp in enumerate(pages):
        if not sp.words:
            continue
        out.append({
            "bl_number": "",
            "page_idx": page_idx,
            "y_start": min(w.top for w in sp.words),
            "y_end": max(w.bottom for w in sp.words),
            "page": sp,
            "carry_over": False,
        })
    return out


def detect_bl_blocks_by_gap(
    pages: List[SpatialPage],
    gap_multiplier: float = 2.5,
) -> List[Dict]:
    """Slice using vertical whitespace gaps between text lines.

    A "gap" greater than ``gap_multiplier × median_gap`` starts a new block.
    Useful when the document has no consistent BL marker but visually
    separates BL records with extra whitespace.
    """
    out: List[Dict] = []
    for page_idx, sp in enumerate(pages):
        lines = sp._group_by_lines(y_tol=4.0)  # type: ignore[attr-defined]
        if len(lines) < 2:
            if sp.words:
                out.append({
                    "bl_number": "",
                    "page_idx": page_idx,
                    "y_start": min(w.top for w in sp.words),
                    "y_end": max(w.bottom for w in sp.words),
                    "page": sp,
                    "carry_over": False,
                })
            continue
        tops = [line[0].top for line in lines]
        gaps = [tops[i + 1] - tops[i] for i in range(len(tops) - 1)]
        sorted_gaps = sorted(gaps)
        median = sorted_gaps[len(sorted_gaps) // 2] or 1.0
        threshold = median * gap_multiplier

        block_start_idx = 0
        for i, g in enumerate(gaps):
            if g >= threshold:
                # Close the current block
                y_start = lines[block_start_idx][0].top
                y_end = lines[i][0].bottom
                out.append({
                    "bl_number": "",
                    "page_idx": page_idx,
                    "y_start": y_start,
                    "y_end": y_end,
                    "page": sp,
                    "carry_over": False,
                })
                block_start_idx = i + 1
        # Trailing block
        if block_start_idx < len(lines):
            y_start = lines[block_start_idx][0].top
            y_end = lines[-1][-1].bottom
            out.append({
                "bl_number": "",
                "page_idx": page_idx,
                "y_start": y_start,
                "y_end": y_end,
                "page": sp,
                "carry_over": False,
            })
    return out


def detect_bl_blocks(
    pages: List[SpatialPage],
    *,
    strategy: str = "marker",
    bl_pattern: str = "",
) -> List[Dict]:
    """Dispatcher — pick the right block detector for the strategy."""
    s = (strategy or "marker").lower()
    if s == "marker" and bl_pattern:
        blocks = detect_bl_blocks_by_marker(pages, bl_pattern)
        if blocks:
            return blocks
        # Fall through to gap detection if marker found nothing
        return detect_bl_blocks_by_gap(pages)
    if s == "gap":
        return detect_bl_blocks_by_gap(pages)
    return detect_bl_blocks_by_page(pages)


# ────────────────────────────────────────────────────────────────────────
# Block-restricted SpatialPage view
# ────────────────────────────────────────────────────────────────────────
def block_view(block: Dict) -> SpatialPage:
    """Return a SpatialPage containing only the words inside this block's
    vertical range. Used to scope rule searches to one BL.
    """
    sp: SpatialPage = block["page"]
    y0 = block["y_start"] - 1
    y1 = block["y_end"] + 1
    words = [w for w in sp.words if w.cy >= y0 and w.cy <= y1]
    return SpatialPage(words)


def block_text(block: Dict) -> str:
    """Reconstructed reading-order text of a block (for regex on container
    markers, weight extraction, etc.)."""
    sp = block_view(block)
    lines = sp._group_by_lines(y_tol=4.0)  # type: ignore[attr-defined]
    return "\n".join(" ".join(w.text for w in line) for line in lines)
