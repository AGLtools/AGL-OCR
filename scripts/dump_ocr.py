"""Dump raw OCR text for all pages to diagnose extraction."""
import sys
sys.path.insert(0, r"c:\AGL_OCR")
from pathlib import Path
from src.ocr_engine import OCREngine

e = OCREngine()
pdf = list(Path(r"c:\AGL_OCR\testing_pdf").glob("*.pdf"))[0]
pages = e.load_document(pdf)
for p in pages:
    e.ensure_page_ocr(p)
    print(f"=== PAGE {p.index+1} ({p.width}x{p.height}, {len(p.tokens)} tokens) ===")
    print(p.text())
    print()
