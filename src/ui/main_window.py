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
from ..manifest_parser import ManifestParser
from ..corrections import CorrectionStore
from ..config import load_fields, EXPORTS_DIR
from .canvas import ImageCanvas
from .format_trainer import FormatTrainerDialog
from .manifest_review import ManifestReviewDialog


# ============================================================
# AGL brand theme (matches the AGL logo: navy + bright blue)
# ============================================================
AGL_NAVY      = "#1A4076"
AGL_BLUE      = "#1E90E0"
AGL_BLUE_DARK = "#1670B0"
AGL_BG        = "#F4F7FB"
AGL_TEXT      = "#1B2A3D"

AGL_STYLE = f"""
QMainWindow, QDialog {{ background: {AGL_BG}; color: {AGL_TEXT}; }}
QStatusBar {{
    background: {AGL_NAVY}; color: white;
    font-size: 12px; padding: 4px;
}}
QToolBar {{
    background: {AGL_NAVY}; spacing: 6px; padding: 6px;
    border-bottom: 2px solid {AGL_BLUE};
}}
QToolBar QToolButton {{
    color: white; background: transparent; padding: 6px 12px;
    border-radius: 4px; font-weight: 600;
}}
QToolBar QToolButton:hover {{ background: {AGL_BLUE_DARK}; }}
QToolBar QToolButton:pressed {{ background: {AGL_BLUE}; }}
QToolBar QLabel {{ color: white; }}
QGroupBox {{
    background: white; border: 1px solid #D8E0EC; border-radius: 6px;
    margin-top: 14px; padding: 10px; font-weight: bold; color: {AGL_NAVY};
}}
QGroupBox::title {{
    subcontrol-origin: margin; left: 12px; padding: 0 6px;
    background: {AGL_BG};
}}
QPushButton {{
    background: white; border: 1px solid #C8D2E0; border-radius: 4px;
    padding: 6px 12px; color: {AGL_TEXT};
}}
QPushButton:hover {{ background: #EAF1FA; border-color: {AGL_BLUE}; }}
QPushButton:pressed {{ background: #D5E5F5; }}
QPushButton:disabled {{ color: #999; background: #F0F0F0; }}
QListWidget, QLineEdit, QComboBox {{
    background: white; border: 1px solid #C8D2E0; border-radius: 4px; padding: 4px;
}}
QLineEdit:focus, QComboBox:focus, QListWidget:focus {{
    border-color: {AGL_BLUE};
}}
QLabel {{ color: {AGL_TEXT}; }}
QDockWidget::title {{
    background: {AGL_NAVY}; color: white; padding: 6px; font-weight: bold;
}}
QSplitter::handle {{ background: #C8D2E0; }}
QMessageBox {{ background: white; }}
"""


def _agl_icon_path() -> Optional[Path]:
    """Locate the AGL icon: PyInstaller bundle, install dir, or repo."""
    import sys
    candidates = []
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.append(Path(meipass) / "icon.ico")
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).parent / "icon.ico")
    here = Path(__file__).resolve()
    candidates.extend([
        here.parents[2] / "Forcompilation" / "icon.ico",
        here.parents[2] / "icon.ico",
        here.parents[2] / "Forcompilation" / "AGL_logo.png",
    ])
    for c in candidates:
        if c.exists():
            return c
    return None


def _apply_agl_theme(window: QMainWindow) -> None:
    window.setStyleSheet(AGL_STYLE)


def _set_app_icon(window: QMainWindow) -> None:
    icon_path = _agl_icon_path()
    if icon_path:
        window.setWindowIcon(QIcon(str(icon_path)))


# ============================================================
# AGL brand splash colours used by the canvas placeholder text
# ============================================================



# -------------------- background manifest parse worker --------------------
class ManifestParseWorker(QThread):
    finished_ok = pyqtSignal(list, str)   # rows, source_path
    progress = pyqtSignal(int, int)       # current_page, total
    failed = pyqtSignal(str)

    def __init__(self, parser: ManifestParser, config_name: str, file_path: str):
        super().__init__()
        self.parser = parser
        self.config_name = config_name
        self.file_path = file_path

    def run(self):
        try:
            rows = self.parser.parse(
                self.file_path,
                progress_callback=lambda cur, tot: self.progress.emit(cur, tot),
            )
            self.finished_ok.emit(rows, self.file_path)
        except Exception as e:
            self.failed.emit(str(e))


# -------------------- background OCR extraction worker --------------------
class ScannedExtractWorker(QThread):
    finished_ok = pyqtSignal(list, str)   # rows, source_path
    failed = pyqtSignal(str)

    def __init__(self, file_path: str, fmt: str):
        super().__init__()
        self.file_path = file_path
        self.fmt = fmt

    def run(self):
        try:
            from ..manifest_parser import ManifestParser
            parser = ManifestParser()
            rows = parser.parse_scanned(self.file_path)
            self.finished_ok.emit(rows, self.file_path)
        except Exception as e:
            import traceback
            self.failed.emit(traceback.format_exc())


# -------------------- background OCR worker --------------------
class OCRWorker(QThread):
    finished_ok = pyqtSignal(list, str)   # pages, source_path
    failed = pyqtSignal(str)

    def __init__(self, engine: OCREngine, file_path: str, max_pages: int | None = None):
        super().__init__()
        self.engine = engine
        self.file_path = file_path
        self.max_pages = max_pages

    def run(self):
        try:
            pages = self.engine.load_document(self.file_path, max_pages=self.max_pages)
            self.finished_ok.emit(pages, self.file_path)
        except Exception as e:
            self.failed.emit(str(e))


# -------------------- Main window --------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AGL OCR — Maritime Document Intelligence")
        self.resize(1500, 950)
        # Apply AGL brand theme + window icon
        _apply_agl_theme(self)
        _set_app_icon(self)

        # core services
        self.engine = OCREngine()
        self.tpl_mgr = TemplateManager()
        self.auto_mapper = AutoMapper()
        self.manifest_parser = ManifestParser()
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
        # Per-document persistent correction store (set on _open_document)
        self.corrections: Optional[CorrectionStore] = None
        # manifest_mode: True when document was opened via Smart Parse (state-machine).
        # In this mode only page 1 is rendered for preview and template/OCR
        # interactions are disabled.
        self.manifest_mode: bool = False

        self._build_ui()
        self._refresh_template_list()
        self.statusBar().showMessage("Prêt. Ouvrez un document pour commencer.")

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
        field_group = QGroupBox("Champs  (cliquez un champ, puis un mot ou dessinez une zone)")
        flay = QVBoxLayout(field_group)
        self.field_list = QListWidget()
        self.field_list.itemClicked.connect(self._on_field_selected)
        for f in self.fields_def:
            it = QListWidgetItem(f"○  {f['label']}")
            it.setData(Qt.UserRole, f["key"])
            self.field_list.addItem(it)
        flay.addWidget(self.field_list)

        btn_row = QHBoxLayout()
        self.btn_draw = QPushButton("✏ Dessiner une zone pour le champ")
        self.btn_draw.setCheckable(True)
        self.btn_draw.toggled.connect(self.canvas.set_drawing_mode)
        self.btn_clear_field = QPushButton("✖ Effacer")
        self.btn_clear_field.clicked.connect(self._clear_selected_field)
        btn_row.addWidget(self.btn_draw)
        btn_row.addWidget(self.btn_clear_field)
        flay.addLayout(btn_row)

        # value preview / editor (editable: edits are saved to the per-doc correction store)
        self.value_preview = QLineEdit()
        self.value_preview.setPlaceholderText(
            "Valeur du champ sélectionné — modifiable, enregistrement automatique…"
        )
        self.value_preview.setReadOnly(False)
        self.value_preview.editingFinished.connect(self._on_value_edited)
        flay.addWidget(self.value_preview)

        rlay.addWidget(field_group, 3)

        # --- Template panel ---
        tpl_group = QGroupBox("Modèles  (mémoire cartographique)")
        tlay = QVBoxLayout(tpl_group)
        self.tpl_status = QLabel("Aucun modèle actif.")
        self.tpl_status.setStyleSheet("color:#555;")
        tlay.addWidget(self.tpl_status)

        self.tpl_list = QListWidget()
        self.tpl_list.itemDoubleClicked.connect(self._apply_selected_template)
        tlay.addWidget(self.tpl_list)

        tbtns = QHBoxLayout()
        self.btn_save_tpl = QPushButton("💾 Enregistrer comme modèle")
        self.btn_save_tpl.clicked.connect(self._save_template)
        self.btn_apply_tpl = QPushButton("▶ Appliquer la sélection")
        self.btn_apply_tpl.clicked.connect(self._apply_selected_template)
        self.btn_del_tpl = QPushButton("🗑 Supprimer")
        self.btn_del_tpl.clicked.connect(self._delete_selected_template)
        tbtns.addWidget(self.btn_save_tpl)
        tbtns.addWidget(self.btn_apply_tpl)
        tbtns.addWidget(self.btn_del_tpl)
        tlay.addLayout(tbtns)

        rlay.addWidget(tpl_group, 2)

        # --- Action buttons ---
        action_group = QGroupBox("Actions sur le document")
        alay = QVBoxLayout(action_group)
        self.btn_validate = QPushButton("✓ Valider et ajouter cette page à la file")
        self.btn_validate.clicked.connect(self._validate_document)
        self.btn_process_all = QPushButton("▶▶ Traiter TOUTES les pages avec le modèle actif")
        self.btn_process_all.setStyleSheet("font-weight: bold; background-color: #cce5ff;")
        self.btn_process_all.clicked.connect(self._process_all_pages)
        self.btn_parse_manifest = QPushButton("🔍 Analyse intelligente du manifeste")
        self.btn_parse_manifest.setStyleSheet("font-weight: bold; background-color: #d4edda;")
        self.btn_parse_manifest.setToolTip(
            "Détecte automatiquement le format (CMA CGM, Maersk…) et extrait\n"
            "tous les BL/conteneurs via une machine à états. Fonctionne sur les\n"
            "PDF avec texte intégré. Aucun modèle requis — 360 pages en < 5 s."
        )
        self.btn_parse_manifest.clicked.connect(self._parse_manifest)
        self.btn_export = QPushButton("⬇ Exporter la file vers Excel")
        self.btn_export.clicked.connect(self._export_excel)
        self.btn_export_midas = QPushButton("📊 Exporter au format MIDAS (43 colonnes)")
        self.btn_export_midas.setToolTip(
            "Exporte la file au format MIDAS prêt pour saisie : 42 colonnes plates\n"
            "(Numéro escale et Index laissés vides — équipe d'intégration)."
        )
        self.btn_export_midas.clicked.connect(self._export_midas)
        self.queue_label = QLabel("File : 0 lignes")
        alay.addWidget(self.btn_validate)
        alay.addWidget(self.btn_process_all)
        alay.addWidget(self.btn_parse_manifest)
        alay.addWidget(self.btn_export)
        alay.addWidget(self.btn_export_midas)
        alay.addWidget(self.queue_label)
        rlay.addWidget(action_group, 1)

        right.setMinimumWidth(420)
        right.setMaximumWidth(560)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        # Toolbar
        tb = QToolBar("Principale")
        tb.setMovable(False)
        self.addToolBar(tb)
        act_open = QAction("📂  Ouvrir un document", self)
        act_open.triggered.connect(self._open_document)
        tb.addAction(act_open)
        tb.addSeparator()
        act_auto = QAction("🪄  Lancer la cartographie automatique", self)
        act_auto.triggered.connect(self._run_auto_mapping)
        tb.addAction(act_auto)
        tb.addSeparator()
        act_teach = QAction("🎓  Apprendre un nouveau format", self)
        act_teach.setToolTip(
            "Assistant : affichez le document, cliquez sur chaque LIBELLÉ\n"
            "puis sa VALEUR. L'application apprend la règle spatiale et\n"
            "sauvegarde une configuration réutilisable. Fonctionne sur les\n"
            "PDF avec texte intégré."
        )
        act_teach.triggered.connect(self._teach_format)
        tb.addAction(act_teach)
        tb.addSeparator()
        self.page_combo = QComboBox()
        self.page_combo.currentIndexChanged.connect(self._switch_page)
        tb.addWidget(QLabel("  Page : "))
        tb.addWidget(self.page_combo)

        self.setStatusBar(QStatusBar())

    # ============================================================
    # Document loading
    # ============================================================
    def _open_document(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Ouvrir un document", "",
            "Documents (*.pdf *.png *.jpg *.jpeg *.tif *.tiff *.bmp)",
        )
        if not path:
            return

        # ---- Step 1: SNIFF format BEFORE any OCR/rendering ----
        # pdfplumber sniff is ~50ms; full 360-page render is many minutes.
        path_obj = Path(path)
        fmt = None
        if path_obj.suffix.lower() == ".pdf" and self.manifest_parser.available:
            try:
                fmt = ManifestParser.detect_format(path_obj)
            except Exception:
                fmt = None

        # ---- Step 2: known manifest -> propose Smart Parse (no OCR) ----
        if fmt is not None:
            reply = QMessageBox.question(
                self, "Manifeste détecté",
                f"Format détecté : <b>{fmt.upper().replace('_', ' ')}</b><br><br>"
                f"Lancer l'<b>analyse intelligente</b> sur tout le document ?<br>"
                f"<i>(Utilise le texte intégré du PDF — sans OCR. Quelques secondes<br>"
                f"au lieu de plusieurs minutes pour un OCR de scan.)</i><br><br>"
                f"Choisir <b>Non</b> pour ouvrir en mode normal (modèles manuels / OCR).",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            if reply == QMessageBox.Yes:
                self._open_in_manifest_mode(str(path_obj), fmt)
                return

        # ---- Step 3: fallback -> normal OCR pipeline ----
        self.manifest_mode = False
        self.statusBar().showMessage(
            f"Rendu des pages de {path_obj.name}… (peut prendre du temps pour les gros PDF)"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.worker = OCRWorker(self.engine, str(path_obj))
        self.worker.finished_ok.connect(self._on_ocr_done)
        self.worker.failed.connect(self._on_ocr_failed)
        self.worker.start()

    def _open_in_manifest_mode(self, path: str, fmt: str):
        """Open a manifest: render only page 1 for preview, then run state-machine
        in background. No full OCR pass."""
        self.manifest_mode = True
        self.source_path = Path(path)
        self.statusBar().showMessage(
            f"Mode manifeste ({fmt}) — rendu de l'aperçu page 1…"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        # Quick preview: only page 1
        self._preview_worker = OCRWorker(self.engine, path, max_pages=1)
        self._preview_worker.finished_ok.connect(
            lambda pages, src: self._on_manifest_preview_ready(pages, src, fmt)
        )
        self._preview_worker.failed.connect(self._on_ocr_failed)
        self._preview_worker.start()

    def _on_manifest_preview_ready(self, pages: List[Page], source_path: str, fmt: str):
        """Page-1 preview is rendered; show it and launch the parser."""
        QApplication.restoreOverrideCursor()
        self.pages = pages  # only 1 page in manifest mode
        self.source_path = Path(source_path)
        # Init correction store for this document (will hold parsed + edited rows)
        self.corrections = CorrectionStore(self.source_path)
        self.current_extraction = {}
        self._reset_field_list()

        self.page_combo.blockSignals(True)
        self.page_combo.clear()
        self.page_combo.addItem("Page 1 (aperçu uniquement — mode manifeste)")
        self.page_combo.blockSignals(False)
        self.current_page_idx = 0
        self.canvas.load_page(pages[0], self._on_token_clicked)
        self.tpl_status.setText(
            f"📄 Mode manifeste — format : <b>{fmt.upper().replace('_', ' ')}</b>. "
            f"Analyseur en cours d'exécution…"
        )
        self.active_template = None

        # Launch the state-machine parser in background
        self.manifest_parser = ManifestParser(config_name=fmt)
        self.statusBar().showMessage(f"Analyse intelligente du manifeste ({fmt})…")
        self._parse_worker = ManifestParseWorker(
            self.manifest_parser, fmt, source_path
        )
        self._parse_worker.finished_ok.connect(self._on_parse_done)
        self._parse_worker.failed.connect(self._on_parse_failed)
        self._parse_worker.progress.connect(self._on_parse_progress)
        self._parse_worker.start()

    def _on_ocr_failed(self, msg: str):
        QApplication.restoreOverrideCursor()
        self.statusBar().showMessage("Échec de l'OCR.")
        QMessageBox.critical(self, "Erreur OCR", msg)

    def _on_ocr_done(self, pages: List[Page], source_path: str):
        QApplication.restoreOverrideCursor()
        self.manifest_mode = False
        self.pages = pages
        self.source_path = Path(source_path)
        # Init per-document correction store (loads any existing sidecar)
        self.corrections = CorrectionStore(self.source_path)
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
            badge = " <b>[TABLE]</b>" if match.table_mode else ""
            self.tpl_status.setText(f"✓ Modèle auto-détecté : <b>{match.name}</b>{badge}")
            self._apply_template(match)
            self.statusBar().showMessage(
                f"Modèle « {match.name} » appliqué automatiquement. Vérifiez et validez."
            )
        else:
            self.active_template = None
            self.tpl_status.setText(
                "⚠ Aucun modèle correspondant — cartographie automatique en cours. "
                "Corrigez/dessinez les zones, puis enregistrez comme modèle."
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
            self.statusBar().showMessage(f"Page {idx + 1} prête.")
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
        label = next((f["label"] for f in self.fields_def if f["key"] == self.selected_field_key), self.selected_field_key)
        self.statusBar().showMessage(
            f"Champ « {label} » sélectionné. Cliquez un mot ou activez « Dessiner » pour tracer une zone."
        )

    def _on_token_clicked(self, token: Token):
        if not self.selected_field_key:
            self.statusBar().showMessage("Sélectionnez d'abord un champ à droite.")
            return
        # Use the token's bbox as the field box (single-token mapping).
        rect = QRectF(token.x, token.y, token.w, token.h)
        self._assign_box(self.selected_field_key, rect)

    def _on_box_drawn(self, rect: QRectF):
        if not self.selected_field_key:
            QMessageBox.information(self, "Choisissez un champ",
                                    "Sélectionnez un champ à droite avant de dessiner une zone.")
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
        # Persist as a user correction (manual mapping = explicit user action)
        if self.corrections:
            self.corrections.set_field(self.current_page_idx, field_key, text, bbox)
        self._refresh_field_indicators()

    def _clear_selected_field(self):
        if not self.selected_field_key:
            return
        self.current_extraction.pop(self.selected_field_key, None)
        self.canvas.remove_field_box(self.selected_field_key)
        self.value_preview.clear()
        if self.corrections:
            self.corrections.clear_field(self.current_page_idx, self.selected_field_key)
        self._refresh_field_indicators()

    def _on_value_edited(self):
        """User edited the value preview field directly — persist as correction."""
        if not self.selected_field_key:
            return
        new_val = self.value_preview.text()
        info = self.current_extraction.get(self.selected_field_key) or {
            "bbox": None, "page": self.current_page_idx,
        }
        if info.get("value") == new_val:
            return
        info["value"] = new_val
        info["lines"] = new_val.splitlines() or [""]
        info["page"] = self.current_page_idx
        info["_user_corrected"] = True
        self.current_extraction[self.selected_field_key] = info
        if self.corrections:
            self.corrections.set_field(
                self.current_page_idx,
                self.selected_field_key,
                new_val,
                info.get("bbox"),
            )
        self._refresh_field_indicators()
        self.statusBar().showMessage(
            f"Correction enregistrée pour « {self.selected_field_key} » sur la page {self.current_page_idx + 1}."
        )

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
            QMessageBox.information(self, "Rien à enregistrer",
                                    "Cartographiez au moins un champ avant d'enregistrer un modèle.")
            return
        name, ok = QInputDialog.getText(self, "Nom du modèle",
                                        "Nom (ex : MAERSK_BL_v1) :")
        if not ok or not name.strip():
            return
        shipowner, _ = QInputDialog.getText(self, "Armateur",
                                            "Armateur / propriétaire du format :")
        # Ask whether this template is a TABLE that repeats per row
        table_mode = QMessageBox.question(
            self, "Type de modèle",
            "Ce modèle décrit-il un TABLEAU qui se répète ligne par ligne ?\n\n"
            "OUI → les zones décrivent UNE ligne ; à l'application l'outil\n"
            "          détectera chaque autre ligne dans la même bande\n"
            "          tabulaire et produira une ligne Excel par ligne détectée.\n\n"
            "NON → modèle standard à enregistrement unique (une ligne par page).",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) == QMessageBox.Yes

        page = self.pages[self.current_page_idx]
        boxes: list[FieldBox] = []
        for key, info in self.current_extraction.items():
            if not info.get("bbox"):
                continue
            x, y, w, h = info["bbox"]
            p = info.get("page", self.current_page_idx)
            ref_page = self.pages[p]
            # Find a stable text anchor near this box
            anchor_tok = self.tpl_mgr.find_field_anchor(ref_page, (x, y, w, h))
            anchor_text = anchor_tok.text if anchor_tok else ""
            anchor_dx = (x - anchor_tok.cx) / ref_page.width if anchor_tok else 0.0
            anchor_dy = (y - anchor_tok.cy) / ref_page.height if anchor_tok else 0.0
            boxes.append(FieldBox(
                field_key=key,
                x=x / ref_page.width,
                y=y / ref_page.height,
                w=w / ref_page.width,
                h=h / ref_page.height,
                page=p,
                anchor_text=anchor_text,
                anchor_dx=anchor_dx,
                anchor_dy=anchor_dy,
            ))
        anchors = self.tpl_mgr.build_anchors(page)
        tpl = Template(
            name=name.strip(),
            shipowner=shipowner.strip(),
            field_boxes=boxes,
            anchors=anchors,
            page_index=self.current_page_idx,
            table_mode=table_mode,
        )
        self.tpl_mgr.save(tpl)
        self.active_template = tpl
        self.tpl_status.setText(f"✓ Modèle « <b>{tpl.name}</b> » enregistré et actif.")
        self._refresh_template_list()
        QMessageBox.information(self, "Modèle enregistré",
                                f"Modèle « {tpl.name} » enregistré.\n"
                                f"Les futurs documents correspondant à cette mise en page\n"
                                f"seront extraits automatiquement.")

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
        badge = " [TABLEAU]" if tpl.table_mode else ""
        self.tpl_status.setText(f"✓ Modèle « <b>{tpl.name}</b> »{badge} appliqué manuellement.")

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

    def _resolve_field_box(self, fb: FieldBox, page: Page) -> tuple[int, int, int, int]:
        """Compute pixel bbox for a FieldBox, preferring anchor-relative position.

        Falls back to absolute normalized coords when no anchor was stored or
        the anchor cannot be re-located on this page.
        """
        w = int(fb.w * page.width)
        h = int(fb.h * page.height)
        if fb.anchor_text and page.ocr_done:
            anchor_tok = self.tpl_mgr.locate_anchor_on_page(page, fb.anchor_text)
            if anchor_tok:
                x = int(anchor_tok.cx + fb.anchor_dx * page.width)
                y = int(anchor_tok.cy + fb.anchor_dy * page.height)
                # Clamp to page
                x = max(0, min(x, page.width - 1))
                y = max(0, min(y, page.height - 1))
                return (x, y, w, h)
        # Fallback: absolute normalized
        return (int(fb.x * page.width), int(fb.y * page.height), w, h)

    def _apply_template_to_page(self, tpl: Template, page_idx: int) -> dict[str, dict]:
        """Apply template's boxes to a specific page; return {key: info}.

        - Uses anchor-relative repositioning when available.
        - If template.table_mode, expands ONE-row mapping into N rows by
          clustering tokens vertically inside the table band; in that case
          ``info['lines']`` will hold one OCR string per detected row.
        """
        if page_idx >= len(self.pages):
            return {}
        page = self.pages[page_idx]
        # OCR is required for anchor lookup AND for table-mode row clustering
        if not page.ocr_done:
            self.engine.ensure_page_ocr(page)

        # 1) Resolve every field box for THIS page (row 1 if table mode)
        resolved: dict[str, tuple[int, int, int, int]] = {}
        for fb in tpl.field_boxes:
            resolved[fb.field_key] = self._resolve_field_box(fb, page)

        out: dict[str, dict] = {}

        if tpl.table_mode and resolved:
            # ---- TABLE MODE ----
            # Compute the table band: union of resolved boxes (row-1 footprint)
            xs = [b[0] for b in resolved.values()]
            ys = [b[1] for b in resolved.values()]
            x2s = [b[0] + b[2] for b in resolved.values()]
            y2s = [b[1] + b[3] for b in resolved.values()]
            row1_top = min(ys)
            row1_bottom = max(y2s)
            row1_h = max(row1_bottom - row1_top, 12)
            zone = (
                min(xs),
                row1_top,
                max(x2s) - min(xs),
                page.height - row1_top,  # extend down to bottom of page
            )
            row_y_centers = self.engine.detect_row_y_centers(
                page.tokens, zone, row1_top, row1_h,
            )
            row1_cy = (row1_top + row1_bottom) // 2

            for fb in tpl.field_boxes:
                bx, by, bw, bh = resolved[fb.field_key]
                ftype = self.field_type.get(fb.field_key, "string")
                row_values: list[str] = []
                for cy in row_y_centers:
                    dy = cy - row1_cy
                    rb = (bx, by + dy, bw, bh)
                    text, _ = self.engine.extract_field(page, rb, ftype)
                    row_values.append(text)
                joined = "\n".join(v for v in row_values if v)
                # bbox displayed = original first-row box (visual reference)
                out[fb.field_key] = {
                    "value": joined,
                    "lines": row_values,
                    "bbox": (bx, by, bw, bh),
                    "page": page_idx,
                    "confidence": 0.85,
                }
        else:
            # ---- STANDARD MODE ----
            for fb in tpl.field_boxes:
                x, y, w, h = resolved[fb.field_key]
                ftype = self.field_type.get(fb.field_key, "string")
                text, lines = self.engine.extract_field(page, (x, y, w, h), ftype)
                out[fb.field_key] = {
                    "value": text,
                    "lines": lines,
                    "bbox": (x, y, w, h),
                    "page": page_idx,
                    "confidence": 0.9,
                }

        # Sync UI for the currently displayed page
        if page_idx == self.current_page_idx:
            for key, info in out.items():
                self.current_extraction[key] = info
                bx, by, bw, bh = info["bbox"]
                self.canvas.add_field_box(QRectF(bx, by, bw, bh), key)
        # Apply persisted user corrections for this page (override OCR/template)
        if self.corrections:
            self.corrections.apply_to_extraction(page_idx, out)
            if page_idx == self.current_page_idx:
                self.corrections.apply_to_extraction(page_idx, self.current_extraction)
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
        self.queue_label.setText(f"File : {len(self.extracted_rows)} lignes")
        self.statusBar().showMessage(
            f"{len(rows)} ligne(s) ajoutée(s) depuis la page {self.current_page_idx + 1}. "
            f"File totale : {len(self.extracted_rows)}."
        )

    def _process_all_pages(self):
        """Apply the active template to EVERY page of the current document and queue rows."""
        if not self.pages:
            return
        if not self.active_template:
            QMessageBox.information(
                self, "Aucun modèle",
                "Cartographiez les champs, enregistrez-les comme modèle, puis utilisez ce bouton.",
            )
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        added = 0
        for idx in range(len(self.pages)):
            self.statusBar().showMessage(
                f"Traitement page {idx + 1} / {len(self.pages)}\u2026"
            )
            QApplication.processEvents()
            extraction = self._apply_template_to_page(self.active_template, idx)
            rows = self._extraction_to_rows(extraction, idx)
            self.extracted_rows.extend(rows)
            added += len(rows)
        QApplication.restoreOverrideCursor()
        self.queue_label.setText(f"File : {len(self.extracted_rows)} lignes")
        QMessageBox.information(
            self, "Traitement terminé",
            f"{added} ligne(s) ajoutée(s) depuis {len(self.pages)} page(s) "
            f"avec le modèle « {self.active_template.name} ».",
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
        # Higher cap for table-mode templates (cargo manifests can have ~30 rows/page)
        n_rows = min(n_rows, 50)

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

    # ============================================================
    # Format Trainer (interactive teach-by-clicking)
    # ============================================================
    def _teach_format(self):
        """Open the click-and-teach wizard on the current document."""
        if not self.source_path:
            # No doc yet — let the user pick one
            path, _ = QFileDialog.getOpenFileName(
                self, "Choisir un PDF d'exemple pour l'apprentissage de format", "",
                "PDF (*.pdf)",
            )
            if not path:
                return
            target = Path(path)
        else:
            target = self.source_path

        if target.suffix.lower() != ".pdf":
            QMessageBox.information(
                self, "PDF requis",
                "L'apprentissage de format nécessite un PDF avec texte intégré "
                "(utilise pdfplumber)."
            )
            return

        dlg = FormatTrainerDialog(target, parent=self)
        if dlg.exec_() == FormatTrainerDialog.Accepted:
            self.statusBar().showMessage(
                "✓ Format enregistré. Ouvrez un document de cet armateur — "
                "il sera détecté automatiquement."
            )

    # ============================================================
    # Smart Manifest Parsing (state-machine, no template needed)
    # ============================================================
    def _parse_manifest(self):
        """Auto-detect format and parse the current document with state machine."""
        if not self.source_path:
            QMessageBox.information(self, "Aucun document",
                                    "Ouvrez d'abord un document.")
            return
        if not self.manifest_parser.available:
            QMessageBox.critical(
                self, "Dépendance manquante",
                "pdfplumber est requis.\n\nExécutez :\n  pip install pdfplumber"
            )
            return

        # Auto-detect format
        fmt = ManifestParser.detect_format(self.source_path)

        # ── Scanned document (SAKINA, etc.) ─────────────────────────
        if fmt == "sakina" or (fmt is None and not self._has_embedded_text()):
            reply = QMessageBox.question(
                self, "Document scanné détecté",
                f"Ce document semble être un <b>scan</b> (texte non intégré).<br><br>"
                f"Lancer l'extraction OCR automatique "
                f"<b>({fmt.upper() if fmt else 'SAKINA / format inconnu'})</b> ?<br><br>"
                f"<i>⏱ Durée estimée : 15–60 s selon le nombre de pages.</i>",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            if reply != QMessageBox.Yes:
                return
            self._run_scanned_extraction(fmt or "sakina")
            return

        if fmt is None:
            # Ask user
            fmt, ok = QInputDialog.getItem(
                self, "Format de manifeste",
                "Détection automatique impossible. Sélectionner manuellement :",
                ["cma_cgm", "maersk", "msc", "generic"],
                0, False,
            )
            if not ok:
                return
        else:
            reply = QMessageBox.question(
                self, "Format de manifeste détecté",
                f"Format détecté : <b>{fmt.upper().replace('_', ' ')}</b><br><br>"
                f"Analyser les {len(self.pages)} pages avec l'extracteur à états ?<br>"
                f"<i>(Aucun modèle requis — fonctionne directement sur le texte intégré du PDF)</i>",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            if reply != QMessageBox.Yes:
                return

        self.manifest_parser = ManifestParser(config_name=fmt)
        self.statusBar().showMessage(f"Analyse du manifeste ({fmt})…")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._parse_worker = ManifestParseWorker(
            self.manifest_parser, fmt, str(self.source_path)
        )
        self._parse_worker.finished_ok.connect(self._on_parse_done)
        self._parse_worker.failed.connect(self._on_parse_failed)
        self._parse_worker.progress.connect(self._on_parse_progress)
        self._parse_worker.start()

    def _on_parse_progress(self, current: int, total: int):
        self.statusBar().showMessage(
            f"Analyse page {current + 1} / {total}…"
        )
        QApplication.processEvents()

    def _on_parse_done(self, rows: list, source_path: str):
        QApplication.restoreOverrideCursor()
        if not rows:
            QMessageBox.warning(self, "Aucune donnée extraite",
                                "L'analyseur n'a trouvé aucun enregistrement BL.\n\n"
                                "Vérifiez que le PDF contient du texte (pas un scan).\n"
                                "Essayez d'ouvrir le PDF dans un visualiseur et de sélectionner du texte.")
            self.statusBar().showMessage("Analyse terminée — 0 ligne trouvée.")
            return
        # Open the editable review dialog: corrections are auto-saved per document.
        # The user can fix any extraction errors; corrections persist across sessions.
        src_path = Path(source_path)
        dlg = ManifestReviewDialog(rows, src_path, parent=self)
        dlg.exec_()
        # Refresh corrections store reference (dialog uses its own store on the same path)
        self.corrections = CorrectionStore(src_path)
        # Use the (possibly edited) rows from the dialog for the queue
        edited_rows = dlg.rows
        self.extracted_rows.extend(edited_rows)
        self.queue_label.setText(f"File : {len(self.extracted_rows)} lignes")
        self.statusBar().showMessage(
            f"✓ {len(edited_rows)} conteneurs ajoutés à la file depuis "
            f"{src_path.name}. Cliquez sur « Exporter » pour générer l'Excel."
        )

    def _on_parse_failed(self, msg: str):
        QApplication.restoreOverrideCursor()
        self.statusBar().showMessage("Échec de l'analyse.")
        QMessageBox.critical(self, "Erreur d'analyse", msg)

    # ── Scanned document helpers ─────────────────────────────────────────────
    def _has_embedded_text(self) -> bool:
        """Quick check: does pdfplumber find any words on page 1?"""
        try:
            import pdfplumber
            with pdfplumber.open(str(self.source_path)) as pdf:
                words = pdf.pages[0].extract_words(x_tolerance=6)
                return len(words) >= 5
        except Exception:
            return False

    def _run_scanned_extraction(self, fmt: str):
        self.statusBar().showMessage(f"OCR + extraction ({fmt.upper()})…  ⏳")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._scanned_worker = ScannedExtractWorker(str(self.source_path), fmt)
        self._scanned_worker.finished_ok.connect(self._on_parse_done)
        self._scanned_worker.failed.connect(self._on_parse_failed)
        self._scanned_worker.start()



    def _export_excel(self):        
        if not self.extracted_rows:
            QMessageBox.information(self, "Rien à exporter",
                                    "Validez d'abord au moins un document.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Exporter vers Excel",
            str(EXPORTS_DIR / "AGL_OCR_export.xlsx"),
            "Fichiers Excel (*.xlsx)",
        )
        if not path:
            return
        exporter = ExcelExporter(output_path=Path(path))
        out = exporter.export(self.extracted_rows)
        QMessageBox.information(self, "Export terminé",
                                f"{len(self.extracted_rows)} ligne(s) exportée(s) vers :\n{out}")
        self.extracted_rows.clear()
        self.queue_label.setText("File : 0 lignes")

    def _export_midas(self):
        """Export queued rows in the MIDAS 42-column flat format."""
        if not self.extracted_rows:
            QMessageBox.information(self, "Rien à exporter",
                                    "Analysez et validez au moins un manifeste d'abord.")
            return
        # Only manifest-style rows (with 'bl_number' or 'vessel') are MIDAS-compatible
        manifest_rows = [r for r in self.extracted_rows
                         if "bl_number" in r or "vessel" in r]
        if not manifest_rows:
            QMessageBox.warning(
                self, "Format incompatible",
                "L'export MIDAS attend des lignes issues de l'analyse intelligente "
                "du manifeste (bouton « 🔍 Analyse intelligente »).\n\n"
                "Les lignes issues d'un modèle template ne contiennent pas tous les "
                "champs requis (Navire, Numéro BL, Conteneur…).",
            )
            return
        default_name = f"MIDAS_{Path(self.source_path).stem if self.source_path else 'export'}.xlsx"
        path, _ = QFileDialog.getSaveFileName(
            self, "Exporter au format MIDAS",
            str(EXPORTS_DIR / default_name),
            "Fichiers Excel (*.xlsx)",
        )
        if not path:
            return
        try:
            exporter = ExcelExporter(output_path=Path(path))
            out = exporter.export_midas(manifest_rows)
        except Exception as e:
            QMessageBox.critical(self, "Erreur d'export MIDAS", str(e))
            return
        QMessageBox.information(
            self, "Export MIDAS terminé",
            f"{len(manifest_rows)} ligne(s) exportée(s) au format MIDAS vers :\n{out}\n\n"
            f"Colonnes laissées vides (à remplir par l'équipe d'intégration) :\n"
            f"  • Numéro escale\n  • Index\n  • Range\n  • Code transitaire / chargeur / marchandise\n"
            f"  • Manutentionaire",
        )
