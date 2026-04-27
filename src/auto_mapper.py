"""Keyword-based auto-mapping for first-pass extraction when no template matches."""
from __future__ import annotations
from typing import Dict, List, Optional
import re

from rapidfuzz import fuzz

from .ocr_engine import Page, Token
from .config import load_fields


class AutoMapper:
    def __init__(self):
        self.fields = load_fields()

    def map(self, page: Page) -> Dict[str, dict]:
        """Return {field_key: {'value': str, 'confidence': float, 'bbox': (x,y,w,h)}}.

        Strategy: for each field, find token whose text best matches one of the field's
        keywords; then read the value as text directly to the right (same line) or just below.
        """
        results: Dict[str, dict] = {}
        # Index tokens by line for quick "same line" lookups
        lines: dict[int, List[Token]] = {}
        for t in page.tokens:
            lines.setdefault(t.line_id, []).append(t)
        for ln in lines.values():
            ln.sort(key=lambda t: t.x)

        for f in self.fields:
            best = self._find_label(page, f["keywords"])
            if best is None:
                continue
            label_token, label_conf = best
            value_tokens = self._value_after_label(label_token, page, lines)
            if not value_tokens:
                continue
            value_text = " ".join(t.text for t in value_tokens).strip()
            value_text = self._postprocess(f.get("type", "string"), value_text)
            if not value_text:
                continue
            x = min(t.x for t in value_tokens)
            y = min(t.y for t in value_tokens)
            x2 = max(t.x + t.w for t in value_tokens)
            y2 = max(t.y + t.h for t in value_tokens)
            results[f["key"]] = {
                "value": value_text,
                "confidence": label_conf,
                "bbox": (x, y, x2 - x, y2 - y),
                "page": page.index,
            }
        return results

    # ---------- helpers ----------
    def _find_label(self, page: Page, keywords: list[str]) -> Optional[tuple[Token, float]]:
        """Find token (or token sequence) on the page that best matches any keyword."""
        best_token, best_score = None, 0.0
        # Try multi-word keywords by sliding a window over tokens of same line
        lines: dict[int, List[Token]] = {}
        for t in page.tokens:
            lines.setdefault(t.line_id, []).append(t)
        for line_tokens in lines.values():
            line_tokens.sort(key=lambda t: t.x)
            for kw in keywords:
                kw_words = kw.split()
                n = len(kw_words)
                for i in range(0, len(line_tokens) - n + 1):
                    window = line_tokens[i:i + n]
                    txt = " ".join(t.text for t in window).lower().strip(":")
                    score = fuzz.ratio(kw.lower(), txt) / 100.0
                    if score > best_score:
                        best_score = score
                        # use the LAST token of the label (closest to value)
                        best_token = window[-1]
        if best_token is None or best_score < 0.75:
            return None
        return best_token, best_score

    def _value_after_label(self, label: Token, page: Page,
                           lines: dict[int, List[Token]]) -> List[Token]:
        """Tokens to the right of label on same line, or first line below if right is empty."""
        same = [t for t in lines.get(label.line_id, [])
                if t.x > label.x + label.w and t is not label]
        if same:
            # stop at obvious next-label punctuation (colon)
            value = []
            for t in same:
                value.append(t)
                if len(value) >= 8:
                    break
            return value
        # try line just below within label's horizontal band
        candidates = [
            t for t in page.tokens
            if t.y > label.y + label.h
            and t.y < label.y + label.h + label.h * 3
            and abs(t.cx - label.cx) < label.w * 4
        ]
        candidates.sort(key=lambda t: (t.y, t.x))
        return candidates[:8]

    @staticmethod
    def _postprocess(ftype: str, value: str) -> str:
        value = value.strip(" :;-|")
        if ftype == "number":
            m = re.search(r"[\d][\d\s.,]*", value)
            return m.group(0).strip() if m else value
        if ftype == "date":
            m = re.search(
                r"\d{1,2}[\-/.\s][A-Za-z0-9]{1,9}[\-/.\s]\d{2,4}|\d{4}-\d{2}-\d{2}",
                value,
            )
            return m.group(0) if m else value
        return value
