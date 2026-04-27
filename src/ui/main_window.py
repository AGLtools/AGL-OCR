"""Main window for the AGL OCR application."""
from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Dict

from PyQt5.QtCore import Qt, QRectF, QThread, pyqtSignal
from PyQt5.QtGui import QIcon, QColor
from PyQt5.QtWidgets import (
    QMainWindow, QFileDialog, QAction, QToolBar, QStatusBar, QLabel,
    QDockWidget, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget,
    QListWidgetItem, QLineEdit, QMessageBox, QInputDialog, QComboBox,
    QGroupBox, QFormLayout, QSplitter, QApplication,
)

from ..ocr_engine import OCREngine, Page, Token
from ..template_manager import TemplateManager, Template, FieldBox
from ..auto_mapper import AutoMapper
from ..exporter import ExcelExporter
from ..config import load_fields, EXPORTS_DIR
from .canvas import ImageCanvas


# -------------------- background OCR worker --------------------
class OCRWorker(QThread):
    finished_ok = pyqtSignal(list, str)   # pages, source_path
    failed = pyqtSignal(str)

    def __init__(self, engine: OCREngine, file_path: str):
        super().__init__()
        self.engine = engine
        self.file_path = file_path

    def run(self):
        try:
            pages = self.engine.load_document(self.file_path)
            self.finished_ok.emit(pages, self.file_path)
        except Exception as e:
            self.failed.emit(str(e))


# -------------------- Main window --------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AGL OCR — Maritime Document Intelligence")
        self.resize(1500, 950)

        # core services
        self.engine = OCREngine()
        self.tpl_mgr = TemplateManager()
        self.auto_mapper = AutoMapper()
        self.fields_def = load_fields()
        self.field_type = {f["key"]: f.get("type", "string") for f in self.fields_def}

        # state
        self.pages: List[Page] = []
        self.current_page_idx: int = 0
        self.source_path: Optional[Path] = None
        self.active_template: Optional[Template] = None
        self.selected_field_key: Optional[str] = None
        # extracted_rows: one dict per processed document, accumulated across the session
        self.extracted_rows: List[Dict] = []
        # current document's per-field {value, bbox, page}
        self.current_extraction: Dict[str, dict] = {}

        self._build_ui()
        self._refresh_template_list()
        self.statusBar().showMessage("Ready. Open a document to start.")

    # ============================================================
    # UI construction
    # ============================================================
    def _build_ui(self):
        # Canvas (center)
        self.canvas = ImageCanvas(self)
        self.canvas.box_drawn.connect(self._on_box_drawn)

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.canvas)

        # Right side: fields + templates
        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.setContentsMargins(8, 8, 8, 8)

        # --- Field panel ---
        field_group = QGroupBox("Fields  (click a field, then click a token or draw a box)")
        flay = QVBoxLayout(field_group)
        self.field_list = QListWidget()
        self.field_list.itemClicked.connect(self._on_field_selected)
        for f in self.fields_def:
            it = QListWidgetItem(f"○  {f['label']}")
            it.setData(Qt.UserRole, f["key"])
            self.field_list.addItem(it)
        flay.addWidget(self.field_list)

        btn_row = QHBoxLayout()
        self.btn_draw = QPushButton("✏ Draw box for selected field")
        self.btn_draw.setCheckable(True)
        self.btn_draw.toggled.connect(self.canvas.set_drawing_mode)
        self.btn_clear_field = QPushButton("✖ Clear")
        self.btn_clear_field.clicked.connect(self._clear_selected_field)
        btn_row.addWidget(self.btn_draw)
        btn_row.addWidget(self.btn_clear_field)
        flay.addLayout(btn_row)

        # value preview
        self.value_preview = QLineEdit()
        self.value_preview.setPlaceholderText("Selected field value preview…")
        self.value_preview.setReadOnly(True)
        flay.addWidget(self.value_preview)

        rlay.addWidget(field_group, 3)

        # --- Template panel ---
        tpl_group = QGroupBox("Templates  (cartographic memory)")
        tlay = QVBoxLayout(tpl_group)
        self.tpl_status = QLabel("No template active.")
        self.tpl_status.setStyleSheet("color:#555;")
        tlay.addWidget(self.tpl_status)

        self.tpl_list = QListWidget()
        self.tpl_list.itemDoubleClicked.connect(self._apply_selected_template)
        tlay.addWidget(self.tpl_list)

        tbtns = QHBoxLayout()
        self.btn_save_tpl = QPushButton("💾 Save as template")
        self.btn_save_tpl.clicked.connect(self._save_template)
        self.btn_apply_tpl = QPushButton("▶ Apply selected")
        self.btn_apply_tpl.clicked.connect(self._apply_selected_template)
        self.btn_del_tpl = QPushButton("🗑 Delete")
        self.btn_del_tpl.clicked.connect(self._delete_selected_template)
        tbtns.addWidget(self.btn_save_tpl)
        tbtns.addWidget(self.btn_apply_tpl)
        tbtns.addWidget(self.btn_del_tpl)
        tlay.addLayout(tbtns)

        rlay.addWidget(tpl_group, 2)

        # --- Action buttons ---
        action_group = QGroupBox("Document actions")
        alay = QVBoxLayout(action_group)
        self.btn_validate = QPushButton("✓ Validate & queue current page")
        self.btn_validate.clicked.connect(self._validate_document)
        self.btn_process_all = QPushButton("▶▶ Process ALL pages with active template")
        self.btn_process_all.setStyleSheet("font-weight: bold; background-color: #cce5ff;")
        self.btn_process_all.clicked.connect(self._process_all_pages)
        self.btn_export = QPushButton("⬇ Export queue to Excel")
        self.btn_export.clicked.connect(self._export_excel)
        self.queue_label = QLabel("Queue: 0 rows")
        alay.addWidget(self.btn_validate)
        alay.addWidget(self.btn_process_all)
        alay.addWidget(self.btn_export)
        alay.addWidget(self.queue_label)
        rlay.addWidget(action_group, 1)

        right.setMinimumWidth(420)
        right.setMaximumWidth(560)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        # Toolbar
        tb = QToolBar("Main")
        tb.setMovable(False)
        self.addToolBar(tb)
        act_open = QAction("📂  Open document", self)
        act_open.triggered.connect(self._open_document)
        tb.addAction(act_open)
        tb.addSeparator()
        act_auto = QAction("🪄  Run auto-mapping", self)
        act_auto.triggered.connect(self._run_auto_mapping)
        tb.addAction(act_auto)
        tb.addSeparator()
        self.page_combo = QComboBox()
        self.page_combo.currentIndexChanged.connect(self._switch_page)
        tb.addWidget(QLabel("  Page: "))
        tb.addWidget(self.page_combo)

        self.setStatusBar(QStatusBar())

    # ============================================================
    # Document loading
    # ============================================================
    def _open_document(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open document", "",
            "Documents (*.pdf *.png *.jpg *.jpeg *.tif *.tiff *.bmp)",
        )
        if not path:
            return
        self.statusBar().showMessage(f"OCR running on {Path(path).name} … please wait")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.worker = OCRWorker(self.engine, path)
        self.worker.finished_ok.connect(self._on_ocr_done)
        self.worker.failed.connect(self._on_ocr_failed)
        self.worker.start()

    def _on_ocr_failed(self, msg: str):
        QApplication.restoreOverrideCursor()
        self.statusBar().showMessage("OCR failed.")
        QMessageBox.critical(self, "OCR error", msg)

    def _on_ocr_done(self, pages: List[Page], source_path: str):
        QApplication.restoreOverrideCursor()
        self.pages = pages
        self.source_path = Path(source_path)
        self.current_extraction = {}
        self._reset_field_list()

        self.page_combo.blockSignals(True)
        self.page_combo.clear()
        for i, _p in enumerate(pages):
            self.page_combo.addItem(f"Page {i + 1} / {len(pages)}")
        self.page_combo.blockSignals(False)
        self.current_page_idx = 0
        # Lazy OCR: only OCR page 0 now; other pages OCR on view or via Process All.
        self.engine.ensure_page_ocr(pages[0])
        self._show_page(0)

        # Try to find a matching template automatically
        match = self.tpl_mgr.find_matching_template(pages[0])
        if match:
            self.active_template = match
            self.tpl_status.setText(f"✓ Auto-matched template: <b>{match.name}</b>")
            self._apply_template(match)
            self.statusBar().showMessage(
                f"Template '{match.name}' applied automatically. Verify and validate."
            )
        else:
            self.active_template = None
            self.tpl_status.setText(
                "⚠ No matching template — running auto-mapping. "
                "Correct/draw boxes, then save as template."
            )
            self._run_auto_mapping()

    def _show_page(self, idx: int):
        if not self.pages:
            return
        self.current_page_idx = idx
        page = self.pages[idx]
        # Lazy OCR for any newly-displayed page
        if not page.ocr_done:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self.statusBar().showMessage(f"OCR page {idx + 1}…")
            self.engine.ensure_page_ocr(page)
            QApplication.restoreOverrideCursor()
            self.statusBar().showMessage(f"Page {idx + 1} ready.")
        self.canvas.load_page(page, self._on_token_clicked)
        # If a template is active, re-apply on this page (covers multi-page templates)
        if self.active_template:
            self._apply_template_to_page(self.active_template, idx)
        # re-draw any field boxes already extracted on this page
        for key, info in self.current_extraction.items():
            if info.get("page") == idx and info.get("bbox"):
                x, y, w, h = info["bbox"]
                self.canvas.add_field_box(QRectF(x, y, w, h), key)
        self._refresh_field_indicators()

    def _switch_page(self, idx: int):
        if idx < 0 or idx >= len(self.pages):
            return
        self._show_page(idx)

    # ============================================================
    # Field interactions
    # ============================================================
    def _reset_field_list(self):
        for i in range(self.field_list.count()):
            it = self.field_list.item(i)
            key = it.data(Qt.UserRole)
            label = next(f["label"] for f in self.fields_def if f["key"] == key)
            it.setText(f"○  {label}")
            it.setForeground(QColor("#222"))

    def _refresh_field_indicators(self):
        for i in range(self.field_list.count()):
            it = self.field_list.item(i)
            key = it.data(Qt.UserRole)
            label = next(f["label"] for f in self.fields_def if f["key"] == key)
            if key in self.current_extraction and self.current_extraction[key].get("value"):
                val = self.current_extraction[key]["value"]
                short = (val[:25] + "…") if len(val) > 25 else val
                it.setText(f"●  {label}  →  {short}")
                it.setForeground(QColor("#0a7d2c"))
            else:
                it.setText(f"○  {label}")
                it.setForeground(QColor("#222"))

    def _on_field_selected(self, item: QListWidgetItem):
        self.selected_field_key = item.data(Qt.UserRole)
        info = self.current_extraction.get(self.selected_field_key)
        self.value_preview.setText(info["value"] if info else "")
        self.statusBar().showMessage(
            f"Field '{self.selected_field_key}' selected. "
            f"Click a token, or toggle 'Draw box' to draw a region."
        )

    def _on_token_clicked(self, token: Token):
        if not self.selected_field_key:
            self.statusBar().showMessage("Select a field on the right first.")
            return
        # Use the token's bbox as the field box (single-token mapping).
        rect = QRectF(token.x, token.y, token.w, token.h)
        self._assign_box(self.selected_field_key, rect)

    def _on_box_drawn(self, rect: QRectF):
        if not self.selected_field_key:
            QMessageBox.information(self, "Pick a field",
                                    "Select a field on the right before drawing a box.")
            return
        self._assign_box(self.selected_field_key, rect)
        self.btn_draw.setChecked(False)

    def _assign_box(self, field_key: str, rect: QRectF):
        page = self.pages[self.current_page_idx]
        bbox = (int(rect.x()), int(rect.y()), int(rect.width()), int(rect.height()))
        # Use focused, type-aware OCR on the cropped region (more accurate than
        # picking tokens from full-page OCR).
        ftype = self.field_type.get(field_key, "string")
        text, lines = self.engine.extract_field(page, bbox, ftype)
        if not text:
            text = self.engine.text_in_bbox(page.tokens, bbox)
            lines = text.splitlines()
        self.current_extraction[field_key] = {
            "value": text,
            "lines": lines,
            "bbox": bbox,
            "page": self.current_page_idx,
            "confidence": 1.0,
        }
        self.canvas.add_field_box(rect, field_key)
        self.value_preview.setText(text)
        self._refresh_field_indicators()

    def _clear_selected_field(self):
        if not self.selected_field_key:
            return
        self.current_extraction.pop(self.selected_field_key, None)
        self.canvas.remove_field_box(self.selected_field_key)
        self.value_preview.clear()
        self._refresh_field_indicators()

    # ============================================================
    # Auto-mapping
    # ============================================================
    def _run_auto_mapping(self):
        if not self.pages:
            return
        page = self.pages[self.current_page_idx]
        results = self.auto_mapper.map(page)
        for key, info in results.items():
            if key in self.current_extraction:
                continue  # don't overwrite human/template data
            self.current_extraction[key] = {**info, "page": self.current_page_idx}
            x, y, w, h = info["bbox"]
            self.canvas.add_field_box(QRectF(x, y, w, h), key)
        self._refresh_field_indicators()
        self.statusBar().showMessage(
            f"Auto-mapping found {len(results)} field(s). Review & correct as needed."
        )

    # ============================================================
    # Templates
    # ============================================================
    def _refresh_template_list(self):
        self.tpl_list.clear()
        for tpl in self.tpl_mgr.list_templates():
            it = QListWidgetItem(f"{tpl.name}   ({tpl.shipowner})")
            it.setData(Qt.UserRole, tpl.name)
            self.tpl_list.addItem(it)

    def _save_template(self):
        if not self.pages or not self.current_extraction:
            QMessageBox.information(self, "Nothing to save",
                                    "Map at least one field before saving a template.")
            return
        name, ok = QInputDialog.getText(self, "Template name",
                                        "Name (e.g. MAERSK_BL_v1):")
        if not ok or not name.strip():
            return
        shipowner, _ = QInputDialog.getText(self, "Shipowner",
                                            "Shipowner / format owner:")
        page = self.pages[self.current_page_idx]
        boxes: list[FieldBox] = []
        for key, info in self.current_extraction.items():
            if not info.get("bbox"):
                continue
            x, y, w, h = info["bbox"]
            p = info.get("page", self.current_page_idx)
            ref_page = self.pages[p]
            boxes.append(FieldBox(
                field_key=key,
                x=x / ref_page.width,
                y=y / ref_page.height,
                w=w / ref_page.width,
                h=h / ref_page.height,
                page=p,
            ))
        anchors = self.tpl_mgr.build_anchors(page)
        tpl = Template(
            name=name.strip(),
            shipowner=shipowner.strip(),
            field_boxes=boxes,
            anchors=anchors,
            page_index=self.current_page_idx,
        )
        self.tpl_mgr.save(tpl)
        self.active_template = tpl
        self.tpl_status.setText(f"✓ Template '<b>{tpl.name}</b>' saved & active.")
        self._refresh_template_list()
        QMessageBox.information(self, "Template saved",
                                f"Template '{tpl.name}' saved.\n"
                                f"Future documents matching this layout will be "
                                f"extracted automatically.")

    def _apply_selected_template(self, *_):
        item = self.tpl_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        tpl = next((t for t in self.tpl_mgr.list_templates() if t.name == name), None)
        if not tpl:
            return
        self.active_template = tpl
        self._apply_template(tpl)
        self.tpl_status.setText(f"✓ Template '<b>{tpl.name}</b>' applied manually.")

    def _apply_template(self, tpl: Template):
        """Apply template to the CURRENT page (re-OCRs each field with focused OCR)."""
        if not self.pages:
            return
        # Clear existing field boxes drawn on canvas (current page)
        for key in list(self.current_extraction.keys()):
            self.canvas.remove_field_box(key)
        self.current_extraction = {}
        self._apply_template_to_page(tpl, self.current_page_idx)
        self._refresh_field_indicators()

    def _apply_template_to_page(self, tpl: Template, page_idx: int) -> dict[str, dict]:
        """Apply template's normalized boxes to a specific page; return {key: info}.

        Uses focused per-field OCR (type-aware). Updates current_extraction for the
        current page; for other pages, returns the dict for the caller to handle.
        """
        if page_idx >= len(self.pages):
            return {}
        page = self.pages[page_idx]
        # Page must be loaded for its width/height; OCR not strictly needed for
        # focused crops but ensure it for the canvas tokens display.
        if page_idx == self.current_page_idx and not page.ocr_done:
            self.engine.ensure_page_ocr(page)

        out: dict[str, dict] = {}
        for fb in tpl.field_boxes:
            x = int(fb.x * page.width)
            y = int(fb.y * page.height)
            w = int(fb.w * page.width)
            h = int(fb.h * page.height)
            ftype = self.field_type.get(fb.field_key, "string")
            text, lines = self.engine.extract_field(page, (x, y, w, h), ftype)
            info = {
                "value": text,
                "lines": lines,
                "bbox": (x, y, w, h),
                "page": page_idx,
                "confidence": 0.9,
            }
            out[fb.field_key] = info
            if page_idx == self.current_page_idx:
                self.current_extraction[fb.field_key] = info
                self.canvas.add_field_box(QRectF(x, y, w, h), fb.field_key)
        return out

    def _delete_selected_template(self):
        item = self.tpl_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        if QMessageBox.question(self, "Delete template",
                                f"Delete template '{name}'?") != QMessageBox.Yes:
            return
        self.tpl_mgr.delete(name)
        self._refresh_template_list()

    # ============================================================
    # Validate / Export
    # ============================================================
    def _validate_document(self):
        """Queue ONE row from the current page (splits into N rows if multi-line)."""
        if not self.pages:
            return
        rows = self._extraction_to_rows(self.current_extraction, self.current_page_idx)
        self.extracted_rows.extend(rows)
        self.queue_label.setText(f"Queue: {len(self.extracted_rows)} rows")
        self.statusBar().showMessage(
            f"Queued {len(rows)} row(s) from page {self.current_page_idx + 1}. "
            f"Total queue: {len(self.extracted_rows)}."
        )

    def _process_all_pages(self):
        """Apply the active template to EVERY page of the current document and queue rows."""
        if not self.pages:
            return
        if not self.active_template:
            QMessageBox.information(
                self, "No template",
                "Map the fields, save them as a template first, then use this button.",
            )
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        added = 0
        for idx in range(len(self.pages)):
            self.statusBar().showMessage(
                f"Processing page {idx + 1} / {len(self.pages)}\u2026"
            )
            QApplication.processEvents()
            extraction = self._apply_template_to_page(self.active_template, idx)
            rows = self._extraction_to_rows(extraction, idx)
            self.extracted_rows.extend(rows)
            added += len(rows)
        QApplication.restoreOverrideCursor()
        self.queue_label.setText(f"Queue: {len(self.extracted_rows)} rows")
        QMessageBox.information(
            self, "Processed",
            f"Added {added} row(s) from {len(self.pages)} page(s) "
            f"using template '{self.active_template.name}'.",
        )
        # Refresh current page display (it may have been overwritten above)
        self._show_page(self.current_page_idx)

    def _extraction_to_rows(self, extraction: dict[str, dict], page_idx: int) -> list[dict]:
        """Convert an extraction dict to one OR MORE rows.

        If multiple fields in this page have multiple text lines (e.g. 2 shipments
        on the same page), produce that many rows by zipping line-by-line.
        """
        if not extraction:
            return []
        # Determine how many lines we should split into
        line_counts = []
        for info in extraction.values():
            lines = info.get("lines") or info.get("value", "").splitlines()
            if len(lines) > 1:
                line_counts.append(len(lines))
        n_rows = max(line_counts) if line_counts else 1
        n_rows = min(n_rows, 10)  # safety cap

        base = {
            "Source File": self.source_path.name if self.source_path else "",
            "Template": self.active_template.name if self.active_template else "(auto)",
            "Page": page_idx + 1,
        }
        rows = []
        for r in range(n_rows):
            row = dict(base)
            for f in self.fields_def:
                key = f["key"]
                info = extraction.get(key)
                if not info:
                    row[key] = ""
                    continue
                lines = info.get("lines") or info.get("value", "").splitlines() or [""]
                if n_rows > 1 and len(lines) == n_rows:
                    row[key] = lines[r]
                elif n_rows > 1 and len(lines) == 1:
                    row[key] = lines[0]  # repeat single value across rows
                else:
                    row[key] = info.get("value", "")
            rows.append(row)
        return rows

    def _export_excel(self):
        if not self.extracted_rows:
            QMessageBox.information(self, "Nothing to export",
                                    "Validate at least one document first.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export to Excel",
            str(EXPORTS_DIR / "AGL_OCR_export.xlsx"),
            "Excel files (*.xlsx)",
        )
        if not path:
            return
        exporter = ExcelExporter(output_path=Path(path))
        out = exporter.export(self.extracted_rows)
        QMessageBox.information(self, "Exported",
                                f"Exported {len(self.extracted_rows)} row(s) to:\n{out}")
        self.extracted_rows.clear()
        self.queue_label.setText("Queue: 0 rows")
