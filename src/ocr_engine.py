"""OCR engine: convert PDFs/images to page images and extract token-level OCR data.

Optimizations:
- OpenCV preprocessing (grayscale + denoise + adaptive threshold) for better OCR.
- Lazy OCR: pages are rendered up front but OCR runs on demand.
- Focused per-field OCR with type-aware char whitelist (fixes O/0 confusion
  on numeric fields).
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List
import hashlib
import os
import re

import numpy as np
import cv2
from PIL import Image
import pytesseract
from pdf2image import convert_from_path

from .config import load_settings, CACHE_DIR
from .paths import poppler_bin, tesseract_exe, tessdata_dir


# ---------- data classes ----------
@dataclass
class Token:
    text: str
    x: int
    y: int
    w: int
    h: int
    conf: float
    line_id: int = -1
    block_id: int = -1

    def to_dict(self):
        return asdict(self)

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2


@dataclass
class Page:
    index: int
    image_path: Path
    width: int
    height: int
    tokens: List[Token] = field(default_factory=list)
    ocr_done: bool = False  # True once full-page OCR has run
    # Pixel crop applied on the original scan (x, y, w, h) — stored for info only.
    crop_box: tuple = field(default_factory=lambda: (0, 0, 0, 0))

    def text(self) -> str:
        return " ".join(t.text for t in self.tokens)


# ---------- engine ----------
class OCREngine:
    # Per-type Tesseract config strings used for FOCUSED field re-OCR.
    _FIELD_CONFIGS = {
        "number": '--psm 6 -c tessedit_char_whitelist="0123456789.,- "',
        "date":   '--psm 6 -c tessedit_char_whitelist="0123456789/-.: ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"',
        "string": '--psm 6',  # assume single uniform block (multi-line cells)
    }

    def __init__(self):
        s = load_settings()
        self.lang = s.get("ocr_lang", "eng")
        self.dpi = int(s.get("pdf_dpi", 250))

        self.poppler_path = poppler_bin() or s.get("poppler_path") or None

        bundled_tess = tesseract_exe()
        if bundled_tess:
            pytesseract.pytesseract.tesseract_cmd = bundled_tess
        else:
            tess = s.get("tesseract_cmd")
            if tess and Path(tess).exists():
                pytesseract.pytesseract.tesseract_cmd = tess

        td = tessdata_dir()
        if td:
            os.environ["TESSDATA_PREFIX"] = td

    # ============================================================
    # Document loading (rendering only — OCR is lazy)
    # ============================================================
    def load_document(self, file_path: str | Path) -> List[Page]:
        """Render every page to PNG (preprocessed). Does NOT OCR yet."""
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()
        doc_hash = hashlib.md5(file_path.read_bytes()).hexdigest()[:12]
        cache_dir = CACHE_DIR / f"{file_path.stem}_{doc_hash}"
        cache_dir.mkdir(parents=True, exist_ok=True)

        if suffix == ".pdf":
            kwargs = {"dpi": self.dpi}
            if self.poppler_path:
                kwargs["poppler_path"] = self.poppler_path
            images = convert_from_path(str(file_path), **kwargs)
        elif suffix in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}:
            images = [Image.open(file_path).convert("RGB")]
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        pages: List[Page] = []
        for i, img in enumerate(images):
            img_path = cache_dir / f"page_{i + 1}.png"
            crop_path = cache_dir / f"page_{i + 1}.crop"
            if not img_path.exists():
                img = self._auto_rotate(img)
                img = self._preprocess_for_display(img)
                # Detect & crop to the main content zone so template coords
                # are stable regardless of scan margin shifts.
                arr = np.array(img)
                cx, cy, cw, ch = self._detect_content_region(arr)
                img = img.crop((cx, cy, cx + cw, cy + ch))
                img.save(img_path, "PNG")
                crop_path.write_text(f"{cx},{cy},{cw},{ch}")
            else:
                img = Image.open(img_path)
                if crop_path.exists():
                    cx, cy, cw, ch = map(int, crop_path.read_text().split(","))
                else:
                    cx = cy = 0; cw = img.width; ch = img.height
            pages.append(Page(
                index=i,
                image_path=img_path,
                width=img.width,
                height=img.height,
                crop_box=(cx, cy, cw, ch),
            ))
        return pages

    def ensure_page_ocr(self, page: Page) -> Page:
        """Run full-page OCR on demand; subsequent calls are no-ops."""
        if page.ocr_done:
            return page
        img = Image.open(page.image_path)
        page.tokens = self._ocr_image(img)
        page.ocr_done = True
        return page

    # ============================================================
    # Pre-processing
    # ============================================================
    # ============================================================
    # Content-zone detection
    # ============================================================
    @staticmethod
    def _detect_content_region(arr: np.ndarray, pad_frac: float = 0.015) -> tuple:
        """Return (x, y, w, h) bounding box of the main content area (table / text blocks).

        Uses morphological dilation to merge nearby text into blobs, then
        takes the union bbox of all significant blobs.  The result is padded
        slightly so no character touches the border.
        """
        gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY) if len(arr.shape) == 3 else arr
        h, w = gray.shape

        # Invert: content = white blobs on black background
        _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)

        # Dilate to merge text tokens into line-level and then block-level blobs
        kw = max(1, w // 20)   # horizontal kernel — connects tokens on same line
        kh = max(1, h // 40)   # vertical kernel  — connects lines into blocks
        dilated = cv2.dilate(thresh,
                             cv2.getStructuringElement(cv2.MORPH_RECT, (kw, 1)))
        dilated = cv2.dilate(dilated,
                             cv2.getStructuringElement(cv2.MORPH_RECT, (1, kh)))

        contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL,
                                       cv2.CHAIN_APPROX_SIMPLE)

        min_area = h * w * 0.003  # ignore tiny noise blobs (< 0.3 % of page)
        significant = [c for c in contours if cv2.contourArea(c) > min_area]

        if not significant:
            return 0, 0, w, h  # nothing found — keep the full page

        # Union of all significant bounding rects
        xs, ys, x2s, y2s = [], [], [], []
        for c in significant:
            bx, by, bw, bh = cv2.boundingRect(c)
            xs.append(bx);  ys.append(by)
            x2s.append(bx + bw);  y2s.append(by + bh)

        pad_x = int(w * pad_frac)
        pad_y = int(h * pad_frac)
        x1 = max(0, min(xs)  - pad_x)
        y1 = max(0, min(ys)  - pad_y)
        x2 = min(w, max(x2s) + pad_x)
        y2 = min(h, max(y2s) + pad_y)
        return x1, y1, x2 - x1, y2 - y1

    # ============================================================
    # Orientation
    # ============================================================
    @staticmethod
    def _auto_rotate(img: Image.Image) -> Image.Image:
        try:
            osd = pytesseract.image_to_osd(img, output_type=pytesseract.Output.DICT)
            rotate = int(osd.get("rotate", 0))
        except Exception:
            return img
        if rotate == 0:
            return img
        return img.rotate(-rotate, expand=True, fillcolor="white")

    @staticmethod
    def _preprocess_for_display(img: Image.Image) -> Image.Image:
        """Light enhancement: grayscale + CLAHE contrast + edge-preserving denoise.

        Kept as 3-channel so the canvas displays a normal-looking page.
        """
        arr = np.array(img.convert("RGB"))
        gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(16, 16))
        enhanced = clahe.apply(gray)
        denoised = cv2.fastNlMeansDenoising(enhanced, None, h=10,
                                            templateWindowSize=7,
                                            searchWindowSize=21)
        rgb = cv2.cvtColor(denoised, cv2.COLOR_GRAY2RGB)
        return Image.fromarray(rgb)

    @staticmethod
    def _binarize_for_ocr(img: Image.Image) -> Image.Image:
        """Aggressive binarization for FOCUSED field crops (boosts OCR accuracy)."""
        arr = np.array(img.convert("L"))
        h, w = arr.shape
        # Upscale small crops so Tesseract has enough pixels per glyph.
        if h < 60:
            scale = max(2, 60 // max(h, 1))
            arr = cv2.resize(arr, (w * scale, h * scale),
                             interpolation=cv2.INTER_CUBIC)
        # Otsu binarization
        _, bw = cv2.threshold(arr, 0, 255,
                              cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        # White border so chars touching the edge are not clipped.
        bw = cv2.copyMakeBorder(bw, 10, 10, 10, 10,
                                cv2.BORDER_CONSTANT, value=255)
        return Image.fromarray(bw)

    # ============================================================
    # Full-page OCR (used for the page being interactively mapped)
    # ============================================================
    def _ocr_image(self, img: Image.Image) -> List[Token]:
        data = pytesseract.image_to_data(
            img, lang=self.lang, output_type=pytesseract.Output.DICT,
        )
        tokens: List[Token] = []
        n = len(data["text"])
        for i in range(n):
            txt = (data["text"][i] or "").strip()
            if not txt:
                continue
            try:
                conf = float(data["conf"][i])
            except (TypeError, ValueError):
                conf = -1.0
            if conf < 30:
                continue
            tokens.append(Token(
                text=txt,
                x=int(data["left"][i]),
                y=int(data["top"][i]),
                w=int(data["width"][i]),
                h=int(data["height"][i]),
                conf=conf,
                line_id=int(data["line_num"][i]),
                block_id=int(data["block_num"][i]),
            ))
        return tokens

    # ============================================================
    # FOCUSED field extraction — used by templates on every page
    # ============================================================
    def extract_field(self, page: Page, bbox: tuple[int, int, int, int],
                      field_type: str = "string") -> tuple[str, list[str]]:
        """Crop the bbox, binarize, run a type-aware focused OCR pass.

        Returns (joined_text, list_of_lines). The line list lets the caller
        produce multiple Excel rows when a single page contains several data lines.
        """
        img = Image.open(page.image_path)
        x, y, w, h = bbox
        x = max(0, x); y = max(0, y)
        x2 = min(img.width, x + w)
        y2 = min(img.height, y + h)
        if x2 <= x or y2 <= y:
            return "", []
        crop = img.crop((x, y, x2, y2))
        crop = self._binarize_for_ocr(crop)

        config = self._FIELD_CONFIGS.get(field_type, self._FIELD_CONFIGS["string"])
        try:
            raw = pytesseract.image_to_string(crop, lang=self.lang, config=config)
        except Exception:
            raw = ""
        lines = [ln.strip(" \t|;:-") for ln in raw.splitlines()]
        lines = [ln for ln in lines if ln]
        if field_type == "number":
            lines = [self._clean_number(ln) for ln in lines]
            lines = [ln for ln in lines if ln]
        return "\n".join(lines), lines

    @staticmethod
    def _clean_number(s: str) -> str:
        # Common OCR digit confusions
        s = (s.replace("O", "0").replace("o", "0")
              .replace("l", "1").replace("I", "1")
              .replace("S", "5").replace("B", "8"))
        m = re.findall(r"[\d][\d\s.,]*", s)
        return m[0].strip() if m else ""

    # ============================================================
    # Helpers
    # ============================================================
    @staticmethod
    def text_in_bbox(tokens: List[Token], bbox: tuple[int, int, int, int]) -> str:
        x, y, w, h = bbox
        x2, y2 = x + w, y + h
        inside = [t for t in tokens if x <= t.cx <= x2 and y <= t.cy <= y2]
        inside.sort(key=lambda t: (t.y, t.x))
        lines: dict[int, list[Token]] = {}
        for t in inside:
            lines.setdefault(t.line_id, []).append(t)
        out_lines = []
        for _, group in sorted(lines.items(), key=lambda kv: min(t.y for t in kv[1])):
            group.sort(key=lambda t: t.x)
            out_lines.append(" ".join(t.text for t in group))
        return "\n".join(out_lines).strip()
