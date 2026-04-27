# AGL OCR — Maritime Document Intelligence

Smart OCR tool for AGL (Africa Global Logistics) that extracts structured fields
from Bills of Lading, invoices and shipping forms, and **memorizes** each shipowner's
layout so future documents are processed automatically.

## Architecture

```
AGL_OCR/
├── app.py                    # Launcher (PyQt5)
├── requirements.txt
├── config/
│   ├── settings.yaml         # Tesseract / Poppler paths, OCR language, DPI
│   └── fields.yaml           # Standard AGL fields & auto-mapping keywords
├── data/
│   ├── templates/            # Saved cartographic templates (one JSON per shipowner)
│   ├── exports/              # Generated .xlsx files
│   └── cache/                # Rendered page PNGs (per document hash)
├── src/
│   ├── config.py             # Paths & YAML loaders
│   ├── ocr_engine.py         # PDF/image → page images → Tesseract token boxes
│   ├── auto_mapper.py        # Keyword-based first-pass field extraction
│   ├── template_manager.py   # Persist/load/match cartographic templates
│   ├── exporter.py           # Excel writer (openpyxl)
│   └── ui/
│       ├── canvas.py         # Interactive QGraphicsView (zoom, pan, draw bbox)
│       └── main_window.py    # Main PyQt5 window & workflow
└── testing_pdf/              # Sample documents
```

### Pipeline

1. **OCR Engine** (`ocr_engine.py`) — uses `pdf2image` + Poppler to rasterize PDFs,
   then `pytesseract.image_to_data` to get every word with its bounding box & confidence.
2. **Template Matching** (`template_manager.py`) — at load time, the first page is
   fingerprinted using ~8 distinctive "anchor" tokens (text + normalized position).
   Each saved template is scored against the page (fuzzy text × positional proximity);
   the best match above 0.55 is auto-applied.
3. **Auto-Mapping** (`auto_mapper.py`) — fallback when no template matches: searches
   for label keywords (e.g. "Container No"), then reads the value to the right or
   below the label.
4. **Human-in-the-Loop UI** (`ui/main_window.py`) — every OCR token is shown as a
   clickable box on the page. The user picks a field on the right panel and either
   clicks a token or drags a rectangle to map it. Boxes are colour-coded per field.
5. **Template Memorization** — clicking *Save as template* normalizes all drawn
   boxes to (0..1) page-relative coords and stores them as a JSON template tied to
   the shipowner. Next time a document with the same layout is opened, extraction
   is automatic.
6. **Export** — *Validate & queue* batches the current document; *Export queue to
   Excel* writes one row per document to `.xlsx` (one column per standard field).

## Setup (Windows)

1. Install Tesseract: https://github.com/UB-Mannheim/tesseract/wiki
   (default install path `C:\Program Files\Tesseract-OCR\` is already configured
   in `config/settings.yaml`). Install the **French** language pack as well
   (`fra.traineddata`) — config uses `eng+fra`.
2. Install Poppler (needed by `pdf2image`): https://github.com/oschwartz10612/poppler-windows/releases
   Set `poppler_path` in `config/settings.yaml` to the `Library/bin` folder, e.g.:
   ```yaml
   poppler_path: "C:\\poppler-24.02.0\\Library\\bin"
   ```
3. Create a virtualenv and install dependencies:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
4. Launch the app:
   ```powershell
   python app.py
   ```

## Workflow in the app

1. **📂 Open document** → PDF/image is OCR'd in background.
2. If a template matches, fields auto-fill; otherwise auto-mapping runs.
3. Verify each field on the right panel. To correct/add a field:
   - click the field name on the right,
   - then either click any blue token-box on the page, or toggle
     **✏ Draw box** and drag a rectangle.
4. **💾 Save as template** the first time you process a new shipowner format.
5. **✓ Validate & queue** to add this document to the export queue.
6. Open more documents (templates auto-apply), then **⬇ Export queue to Excel**.

## Standard fields

Defined in `config/fields.yaml`. Edit that file to add/remove fields or tune
auto-mapping keywords; the UI and Excel columns update automatically.
