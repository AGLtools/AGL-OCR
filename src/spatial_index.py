"""Spatial index over a pdfplumber page.

Provides label-anchored lookups so that field extraction is robust against
PDF text-flow ordering issues (the classic "Port of Loading" problem where
the value sits visually below the label but is far away in the PDF text
stream).

Core idea: every word has (x0, top, x1, bottom). To find a value:
  1. Locate the label word(s) by fuzzy text match.
  2. Search in a direction (below / right / above / left) within tolerances.
  3. Return the closest word(s) that are not themselves another label.

Designed to work both from pdfplumber word dicts and from OCR token streams
(any iterable of dicts with x0/top/x1/bottom/text fields).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Iterable


# Words that should never be returned as a "value" — they're labels themselves.
# Extended via parser config if needed.
DEFAULT_LABEL_BLOCKLIST = {
    "B/L", "PORT", "OF", "LOADING", "DISCHARGE", "PLACE", "DELIVERY",
    "VESSEL", "VOYAGE", "FLAG", "MASTER", "DATE", "ARRIVAL",
    "SHIPPER", "CONSIGNEE", "NOTIFY", "MARKS", "DESCRIPTION",
    "PACK", "WGT", "VOL", "CTR", "SEAL", "SEAL1", "SEAL2", "SEAL3",
    "TARE", "SZTP", "SHP", "STAT", "MOVEMENT", "BOARD",
}


@dataclass
class Word:
    text: str
    x0: float
    top: float
    x1: float
    bottom: float

    @property
    def cx(self) -> float:
        return (self.x0 + self.x1) / 2.0

    @property
    def cy(self) -> float:
        return (self.top + self.bottom) / 2.0

    @property
    def width(self) -> float:
        return self.x1 - self.x0

    @property
    def height(self) -> float:
        return self.bottom - self.top

    @classmethod
    def from_pdfplumber(cls, w: dict) -> "Word":
        return cls(
            text=w["text"],
            x0=float(w["x0"]),
            top=float(w["top"]),
            x1=float(w["x1"]),
            bottom=float(w["bottom"]),
        )


class SpatialPage:
    """Wraps a flat list of words with spatial query helpers.

    Coordinates use pdfplumber convention: origin top-left, top < bottom.
    """

    def __init__(self, words: List[Word]):
        self.words = words

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    @classmethod
    def from_pdfplumber_page(
        cls,
        page,
        x_tolerance: float = 3,
        y_tolerance: float = 3,
    ) -> "SpatialPage":
        raw = page.extract_words(
            x_tolerance=x_tolerance,
            y_tolerance=y_tolerance,
            keep_blank_chars=False,
        )
        return cls([Word.from_pdfplumber(w) for w in raw])

    # ------------------------------------------------------------------
    # Label location
    # ------------------------------------------------------------------
    def find_label(
        self,
        label: str,
        case_sensitive: bool = False,
        first_only: bool = True,
    ) -> Optional[Word | List[Word]]:
        """Find the word(s) matching a (possibly multi-word) label.

        For multi-word labels we look for adjacent words on the same line that
        together spell the label.
        """
        target = label if case_sensitive else label.upper()
        target_tokens = target.split()

        if len(target_tokens) == 1:
            matches = [
                w for w in self.words
                if (w.text if case_sensitive else w.text.upper()) == target
            ]
            if not matches:
                return None
            return matches[0] if first_only else matches

        # Multi-word: scan lines (group by Y) for consecutive matching words
        lines = self._group_by_lines()
        all_matches: List[List[Word]] = []
        for line in lines:
            for i in range(len(line) - len(target_tokens) + 1):
                slice_ = line[i : i + len(target_tokens)]
                texts = [
                    (w.text if case_sensitive else w.text.upper())
                    for w in slice_
                ]
                if texts == target_tokens:
                    all_matches.append(slice_)
        if not all_matches:
            return None
        # Treat a multi-word match as a single virtual word spanning the slice
        merged = [self._merge(slice_) for slice_ in all_matches]
        return merged[0] if first_only else merged

    def find_label_fuzzy(
        self, label: str, threshold: int = 85
    ) -> Optional[Word]:
        """Fuzzy single-word match (for OCR tolerance)."""
        try:
            from rapidfuzz import fuzz
        except ImportError:
            return self.find_label(label)
        target = label.upper()
        best = None
        best_score = 0
        for w in self.words:
            score = fuzz.ratio(w.text.upper(), target)
            if score >= threshold and score > best_score:
                best = w
                best_score = score
        return best

    # ------------------------------------------------------------------
    # Directional value lookup
    # ------------------------------------------------------------------
    def value_below(
        self,
        anchor: Word,
        max_dy: float = 25.0,
        x_tolerance: float = 40.0,
        max_words: int = 6,
        stop_blocklist: Optional[set] = None,
    ) -> str:
        """Return text directly below the anchor (next visual line)."""
        blocklist = stop_blocklist or DEFAULT_LABEL_BLOCKLIST
        # Candidates: words whose top is below anchor.bottom and within max_dy,
        # and whose horizontal center overlaps anchor's x range (± x_tolerance)
        x_min = anchor.x0 - x_tolerance
        x_max = anchor.x1 + x_tolerance
        cand = [
            w for w in self.words
            if w.top > anchor.bottom - 1
            and w.top - anchor.bottom < max_dy
            and w.cx >= x_min
            and w.cx <= x_max
            and w.text.upper() not in blocklist
        ]
        if not cand:
            return ""
        # Group by closest line
        cand.sort(key=lambda w: (w.top, w.x0))
        first_y = cand[0].top
        same_line = [w for w in cand if abs(w.top - first_y) < 4.0]
        same_line.sort(key=lambda w: w.x0)
        return " ".join(w.text for w in same_line[:max_words])

    def value_right(
        self,
        anchor: Word,
        max_dx: float = 250.0,
        y_tolerance: float = 4.0,
        max_words: int = 8,
        stop_blocklist: Optional[set] = None,
    ) -> str:
        """Return text immediately to the right of the anchor on same line."""
        blocklist = stop_blocklist or DEFAULT_LABEL_BLOCKLIST
        cand = [
            w for w in self.words
            if w.x0 > anchor.x1 - 1
            and w.x0 - anchor.x1 < max_dx
            and abs(w.cy - anchor.cy) < y_tolerance
            and w.text.upper() not in blocklist
        ]
        cand.sort(key=lambda w: w.x0)
        return " ".join(w.text for w in cand[:max_words])

    def value_above(
        self,
        anchor: Word,
        max_dy: float = 25.0,
        x_tolerance: float = 40.0,
        max_words: int = 6,
    ) -> str:
        x_min = anchor.x0 - x_tolerance
        x_max = anchor.x1 + x_tolerance
        cand = [
            w for w in self.words
            if w.bottom < anchor.top + 1
            and anchor.top - w.bottom < max_dy
            and w.cx >= x_min
            and w.cx <= x_max
        ]
        if not cand:
            return ""
        cand.sort(key=lambda w: (-w.top, w.x0))
        first_y = cand[0].top
        same_line = sorted(
            [w for w in cand if abs(w.top - first_y) < 4.0],
            key=lambda w: w.x0,
        )
        return " ".join(w.text for w in same_line[:max_words])

    # ------------------------------------------------------------------
    # Box / region lookup
    # ------------------------------------------------------------------
    def words_in_box(
        self, x0: float, top: float, x1: float, bottom: float
    ) -> List[Word]:
        return [
            w for w in self.words
            if w.cx >= x0 and w.cx <= x1
            and w.cy >= top and w.cy <= bottom
        ]

    def text_in_box(self, x0: float, top: float, x1: float, bottom: float) -> str:
        ws = self.words_in_box(x0, top, x1, bottom)
        ws.sort(key=lambda w: (w.top, w.x0))
        return " ".join(w.text for w in ws)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _group_by_lines(self, y_tol: float = 3.0) -> List[List[Word]]:
        if not self.words:
            return []
        sorted_w = sorted(self.words, key=lambda w: (w.top, w.x0))
        lines: List[List[Word]] = []
        current: List[Word] = [sorted_w[0]]
        for w in sorted_w[1:]:
            if abs(w.top - current[0].top) <= y_tol:
                current.append(w)
            else:
                lines.append(sorted(current, key=lambda x: x.x0))
                current = [w]
        if current:
            lines.append(sorted(current, key=lambda x: x.x0))
        return lines

    @staticmethod
    def _merge(words: List[Word]) -> Word:
        return Word(
            text=" ".join(w.text for w in words),
            x0=min(w.x0 for w in words),
            top=min(w.top for w in words),
            x1=max(w.x1 for w in words),
            bottom=max(w.bottom for w in words),
        )


# ============================================================
# Generic field extractor — driven by YAML rule dict
# ============================================================
def extract_field(spatial: SpatialPage, rule: dict) -> str:
    """Extract a single field value using a YAML rule dict.

    Supported rule shapes:
      { "regex": "VESSEL:\\s*(.*?)FLAG:" }
      { "label_anchor": "Port of Loading", "direction": "below",
        "max_distance": 25, "x_tolerance": 40, "max_words": 6 }
      { "label_anchor": "WGT:", "direction": "right",
        "max_distance": 200, "max_words": 2 }
      { "bbox": [x0, top, x1, bottom] }
      { "fuzzy_label": "Port of Loading", "direction": "below" }
    """
    if "regex" in rule:
        # Reconstruct text in visual reading order: group by lines, then
        # left-to-right within each line. Plain (top, x0) sort fails when
        # words on the same visual line have slightly different `top` values.
        lines = spatial._group_by_lines(y_tol=4.0)
        full = " ".join(" ".join(w.text for w in line) for line in lines)
        m = re.search(rule["regex"], full, re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        return (m.group(1) if m.groups() else m.group(0)).strip()

    if "bbox" in rule:
        x0, top, x1, bottom = rule["bbox"]
        return spatial.text_in_box(x0, top, x1, bottom).strip()

    label = rule.get("label_anchor") or rule.get("fuzzy_label")
    if not label:
        return ""
    if "fuzzy_label" in rule:
        anchor = spatial.find_label_fuzzy(label, threshold=rule.get("fuzzy_threshold", 85))
    else:
        anchor = spatial.find_label(label)
    if not anchor:
        return ""
    direction = rule.get("direction", "below")
    max_dist = rule.get("max_distance", 25.0)
    x_tol = rule.get("x_tolerance", 40.0)
    y_tol = rule.get("y_tolerance", 4.0)
    max_words = rule.get("max_words", 6)

    if direction == "below":
        val = spatial.value_below(anchor, max_dy=max_dist,
                                  x_tolerance=x_tol, max_words=max_words)
    elif direction == "right":
        val = spatial.value_right(anchor, max_dx=max_dist,
                                  y_tolerance=y_tol, max_words=max_words)
    elif direction == "above":
        val = spatial.value_above(anchor, max_dy=max_dist,
                                  x_tolerance=x_tol, max_words=max_words)
    else:
        val = ""

    # Optional post-processing
    if "stop_at" in rule and val:
        idx = val.upper().find(rule["stop_at"].upper())
        if idx >= 0:
            val = val[:idx].strip()
    if "regex_clean" in rule and val:
        m = re.search(rule["regex_clean"], val)
        if m:
            val = m.group(1) if m.groups() else m.group(0)
    return val.strip()
