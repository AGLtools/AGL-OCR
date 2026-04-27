"""Template manager: persist/load cartographic templates per shipowner format.

A template stores normalized bounding boxes (0-1 range) for each field, along with
'anchor' tokens — distinctive text strings & their normalized positions used as a
fingerprint to auto-recognize the same document layout in the future.
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
    # Normalized coords (0..1) relative to page width/height
    x: float
    y: float
    w: float
    h: float
    page: int = 0


@dataclass
class Anchor:
    text: str
    x: float  # normalized center
    y: float


@dataclass
class Template:
    name: str                          # e.g. "MAERSK_BL_v1"
    shipowner: str
    field_boxes: List[FieldBox] = field(default_factory=list)
    anchors: List[Anchor] = field(default_factory=list)
    page_index: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "shipowner": self.shipowner,
            "page_index": self.page_index,
            "field_boxes": [asdict(b) for b in self.field_boxes],
            "anchors": [asdict(a) for a in self.anchors],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Template":
        return cls(
            name=d["name"],
            shipowner=d.get("shipowner", ""),
            page_index=d.get("page_index", 0),
            field_boxes=[FieldBox(**b) for b in d.get("field_boxes", [])],
            anchors=[Anchor(**a) for a in d.get("anchors", [])],
        )

    # --- denormalize a field box for a specific page size ---
    def pixel_box(self, fb: FieldBox, page_w: int, page_h: int) -> tuple[int, int, int, int]:
        return (
            int(fb.x * page_w),
            int(fb.y * page_h),
            int(fb.w * page_w),
            int(fb.h * page_h),
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

    # ---------- anchor extraction ----------
    @staticmethod
    def build_anchors(page: Page, max_anchors: int = 8) -> List[Anchor]:
        """Pick distinctive long alpha tokens spread across the page as fingerprint."""
        candidates = [
            t for t in page.tokens
            if len(t.text) >= 4 and re.search(r"[A-Za-z]", t.text)
        ]
        # prefer high-confidence & longer tokens, prefer top of page (header usually has shipowner name)
        candidates.sort(key=lambda t: (-t.conf, -len(t.text), t.y))
        anchors: list[Anchor] = []
        for t in candidates[: max_anchors * 3]:
            nx = t.cx / page.width
            ny = t.cy / page.height
            # avoid near-duplicates of same word
            if any(a.text.lower() == t.text.lower() for a in anchors):
                continue
            anchors.append(Anchor(text=t.text, x=nx, y=ny))
            if len(anchors) >= max_anchors:
                break
        return anchors

    # ---------- matching ----------
    def find_matching_template(self, page: Page, threshold: float = 0.55) -> Optional[Template]:
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
        """Score = average of (fuzzy text match * positional proximity) over anchors."""
        if not tpl.anchors:
            return 0.0
        scores = []
        for a in tpl.anchors:
            ax_px = a.x * page.width
            ay_px = a.y * page.height
            best_local = 0.0
            for t in page.tokens:
                # text similarity
                sim = fuzz.ratio(a.text.lower(), t.text.lower()) / 100.0
                if sim < 0.6:
                    continue
                # positional similarity (distance normalized by page diagonal)
                dx = (t.cx - ax_px) / page.width
                dy = (t.cy - ay_px) / page.height
                dist = (dx * dx + dy * dy) ** 0.5
                pos = max(0.0, 1.0 - dist * 2.5)  # within ~40% of page = good
                local = sim * pos
                if local > best_local:
                    best_local = local
            scores.append(best_local)
        return sum(scores) / len(scores)


def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", name).strip("_") or "template"
