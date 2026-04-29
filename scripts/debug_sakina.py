"""Debug the desc_weight_map to trace what descriptions are extracted."""
import sys, re
sys.path.insert(0, r"c:\AGL_OCR")
from pathlib import Path
from src.ocr_engine import OCREngine
from src.extractors.sakina_extractor import SakinaExtractor, _normalize_ocr, _CARGO_DESCS

e = OCREngine()
pdf = list(Path(r"c:\AGL_OCR\testing_pdf").glob("*.pdf"))[0]
pages = e.load_document(pdf)
for p in pages: e.ensure_page_ocr(p)

extractor = SakinaExtractor()
# Simulate page classification
from src.extractors.sakina_extractor import PAGE_CARGO_TABLE, PAGE_CUSTOMS, PAGE_UNKNOWN
page_types = [extractor._classify_page(p) for p in pages]
print("Page types:", page_types)

cargo_texts = [
    _normalize_ocr(p.text())
    for p, pt in zip(pages, page_types)
    if pt in (PAGE_CARGO_TABLE, PAGE_CUSTOMS, PAGE_UNKNOWN)
]
corpus = " ".join(cargo_texts)

print("\n--- BL refs in corpus ---")
bls = re.findall(r"EAIF\d{3,4}", corpus, re.I)
print(bls)

print("\n--- All EAIF positions and 200-char context after ---")
for m in re.finditer(r"EAIF\d{3,4}", corpus, re.I):
    bl = m.group(0)
    after = corpus[m.end(): m.end() + 200]
    print(f"\n{bl} (pos {m.start()}):")
    print("  AFTER:", repr(after[:80]))
    # Try patterns
    for i, pat in enumerate(_CARGO_DESCS):
        dm = re.search(pat, after, re.I)
        if dm:
            print(f"  MATCHES pattern {i}: {dm.group(1)!r} at dist {dm.start()}")
            break

print("\n--- _build_desc_weight_map ---")
dw = extractor._build_desc_weight_map(corpus)
for bl, data in sorted(dw.items()):
    print(f"  {bl}: desc={data.get('description')!r}, weight={data.get('weight')!r}")
