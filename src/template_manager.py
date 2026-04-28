"""Template manager: persist/load cartographic templates per shipowner format.

Two intelligence upgrades vs. v1:

1. **Anchor-relative coordinates.** Each FieldBox stores, in addition to its
   absolute normalized position, an *anchor token* (a stable text label found
   on the same page near the field) plus the offset from that anchor to the
   field box. At apply-time we re-find the anchor on the new page (fuzzy text
   match) and translate the field box by the anchor delta — so vertical shifts
   (extra/missing rows above) no longer break alignment.

2. **Table mode.** A template can be flagged `table_mode=True`. The user maps
   ONE row (the first one) and on apply we cluster page tokens by Y inside the
   table band to find every subsequent row, producing one Excel row per
   detected line.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional
import json
import re

from rapidfuzz import fuzz

from .config import TEMPLATES_DIR
from .ocr_engine import Page, Token


@dataclass
class FieldBox:
    field_key: str
    # Absolute normalized coords (0..1) — fallback if anchor is not found
    x: float
    y: float
    w: float
    h: float
    page: int = 0
    # Anchor-relative positioning (preferred when anchor_text is set)
    anchor_text: str = ""
    anchor_dx: float = 0.0   # box.x - anchor.cx, normalized to page width
    anchor_dy: float = 0.0   # box.y - anchor.cy, normalized to page height


@dataclass
class Anchor:
    text: str
    x: float
    y: float


@dataclass
class Template:
    name: str
    shipowner: str
    field_boxes: List[FieldBox] = field(default_factory=list)
    anchors: List[Anchor] = field(default_factory=list)
    page_index: int = 0
    # NEW: when True, treat the user-mapped boxes as ONE table row and replicate
    # them down the page by clustering tokens into rows.
    table_mode: bool = False

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "shipowner": self.shipowner,
            "page_index": self.page_index,
            "table_mode": self.table_mode,
            "field_boxes": [asdict(b) for b in self.field_boxes],
            "anchors": [asdict(a) for a in self.anchors],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Template":
        return cls(
            name=d["name"],
            shipowner=d.get("shipowner", ""),
            page_index=d.get("page_index", 0),
            table_mode=d.get("table_mode", False),
            field_boxes=[FieldBox(**b) for b in d.get("field_boxes", [])],
            anchors=[Anchor(**a) for a in d.get("anchors", [])],
        )


class TemplateManager:
    def __init__(self, directory: Path = TEMPLATES_DIR):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)

    # ---------- CRUD ----------
    def list_templates(self) -> List[Template]:
        out = []
        for p in sorted(self.dir.glob("*.json")):
            try:
                out.append(Template.from_dict(json.loads(p.read_text(encoding="utf-8"))))
            except Exception:
                continue
        return out

    def save(self, template: Template) -> Path:
        path = self.dir / f"{_sanitize(template.name)}.json"
        path.write_text(json.dumps(template.to_dict(), indent=2, ensure_ascii=False),
                        encoding="utf-8")
        return path

    def delete(self, name: str) -> bool:
        path = self.dir / f"{_sanitize(name)}.json"
        if path.exists():
            path.unlink()
            return True
        return False

    # ============================================================
    # Anchor utilities
    # ============================================================
    @staticmethod
    def build_anchors(page: Page, max_anchors: int = 8) -> List[Anchor]:
        candidates = [
            t for t in page.tokens
            if len(t.text) >= 4 and re.search(r"[A-Za-z]", t.text)
        ]
        candidates.sort(key=lambda t: (-t.conf, -len(t.text), t.y))
        anchors: list[Anchor] = []
        for t in candidates[: max_anchors * 3]:
            nx = t.cx / page.width
            ny = t.cy / page.height
            if any(a.text.lower() == t.text.lower() for a in anchors):
                continue
            anchors.append(Anchor(text=t.text, x=nx, y=ny))
            if len(anchors) >= max_anchors:
                break
        return anchors

    @staticmethod
    def find_field_anchor(page: Page,
                          bbox: tuple[int, int, int, int]) -> Optional[Token]:
        """Pick the best stable text token near `bbox` to use as a positional anchor.

        Prefers high-confidence alphabetic tokens located ABOVE-LEFT of the box
        (typical for column headers / labels). Falls back to nearest alpha token.
        """
        bx, by, bw, bh = bbox
        candidates = [
            t for t in page.tokens
            if len(t.text) >= 3
            and re.search(r"[A-Za-z]", t.text)
            and t.conf >= 60
            and not _is_garbage(t.text)
        ]
        if not candidates:
            return None

        def score(t: Token) -> float:
            dx = bx - t.cx
            dy = by - t.cy
            dist = (dx * dx + dy * dy) ** 0.5
            bonus = 0.0
            if dy > 0:
                bonus -= 80   # strongly prefer anchors ABOVE the box
            if dx > 0:
                bonus -= 30   # mildly prefer anchors to the LEFT
            bonus -= len(t.text) * 2  # prefer longer / more distinctive labels
            return dist + bonus

        return min(candidates, key=score)

    @staticmethod
    def locate_anchor_on_page(page: Page,
                              anchor_text: str,
                              min_score: int = 80) -> Optional[Token]:
        if not anchor_text:
            return None
        best, best_score = None, 0
        target = anchor_text.lower()
        for t in page.tokens:
            s = fuzz.ratio(target, t.text.lower())
            if s > best_score:
                best, best_score = t, s
        return best if best_score >= min_score else None

    # ============================================================
    # Template matching
    # ============================================================
    def find_matching_template(self, page: Page,
                               threshold: float = 0.55) -> Optional[Template]:
        best, best_score = None, 0.0
        for tpl in self.list_templates():
            score = self._score(tpl, page)
            if score > best_score:
                best, best_score = tpl, score
        if best and best_score >= threshold:
            return best
        return None

    @staticmethod
    def _score(tpl: Template, page: Page) -> float:
        if not tpl.anchors:
            return 0.0
        scores = []
        for a in tpl.anchors:
            ax_px = a.x * page.width
            ay_px = a.y * page.height
            best_local = 0.0
            for t in page.tokens:
                sim = fuzz.ratio(a.text.lower(), t.text.lower()) / 100.0
                if sim < 0.6:
                    continue
                dx = (t.cx - ax_px) / page.width
                dy = (t.cy - ay_px) / page.height
                dist = (dx * dx + dy * dy) ** 0.5
                pos = max(0.0, 1.0 - dist * 2.5)
                local = sim * pos
                if local > best_local:
                    best_local = local
            scores.append(best_local)
        return sum(scores) / len(scores)


# ---------- helpers ----------
def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", name).strip("_") or "template"


def _is_garbage(text: str) -> bool:
    if len(text) < 3:
        return True
    alpha = sum(1 for c in text if c.isalpha())
    return alpha / len(text) < 0.5
