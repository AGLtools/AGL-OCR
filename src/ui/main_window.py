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
    QGroupBox, QFormLayout, QSplitter, QApplication, QDialog, QPlainTextEdit,
    QToolButton, QMenu,
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
from .ai_dialogs import (
    AIExtractWorker, AILearnWorker,
    GeminiConfigDialog, LearnedFormatsDialog, LearnedSummaryDialog,
    ensure_api_key,
)
from ..ai import has_api_key, detect_learned
from ..ai.template_parser import parse_with_template, template_is_usable
from ..ai.gemini_client import get_ocr_engine, set_ocr_engine
from ..ai.format_registry import list_learned


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


class PageLoadWorker(QThread):
    """Render remaining pages in batches of BATCH_SIZE after page 1 is shown."""
    pages_ready = pyqtSignal(list, int)   # new pages, first 0-based index they start at
    progress = pyqtSignal(int, int)       # rendered_so_far, total
    failed = pyqtSignal(str)

    BATCH_SIZE = 5

    def __init__(self, engine: OCREngine, file_path: str, first_page: int, total: int):
        super().__init__()
        self.engine = engine
        self.file_path = file_path
        self.first_page = first_page   # 1-based
        self.total = total

    def run(self):
        try:
            done = 0
            p = self.first_page
            while p <= self.total:
                batch = self.BATCH_SIZE
                pages = self.engine.load_document(
                    self.file_path, first_page=p, max_pages=batch
                )
                if not pages:
                    break
                self.pages_ready.emit(pages, p - 1)  # 0-based start
                done += len(pages)
                self.progress.emit(done, self.total - self.first_page + 1)
                p += batch
        except Exception as e:
            self.failed.emit(str(e))


# -------------------- Main window --------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AGL OCR — Maritime Document Intelligence")
        self.resize(1400, 860)
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
        # Cache of the last successfully extracted rows (any method).
        # Used by _reopen_review() so the user can re-inspect without re-running.
        self._last_extract_rows: List[Dict] = []
        self._last_extract_source: str = ""

        # When set, _on_ocr_done will auto-trigger _parse_manifest after rendering.
        # Used when a learned format with a usable parse_template is detected at open.
        self._auto_parse_after_open: Optional[Dict] = None
        # Background worker for progressive page loading (learned-format fast path)
        self._page_load_worker: Optional[PageLoadWorker] = None

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
        self.btn_parse_manifest = QToolButton()
        self.btn_parse_manifest.setText("🔍 Analyse intelligente du manifeste ▾")
        self.btn_parse_manifest.setStyleSheet("font-weight: bold; background-color: #d4edda; padding: 6px;")
        self.btn_parse_manifest.setToolTip(
            "Cliquez pour auto-détection, ou ouvrez le menu pour choisir\n"
            "un parseur spécifique parmi les formats déjà appris."
        )
        self.btn_parse_manifest.setPopupMode(QToolButton.MenuButtonPopup)
        self.btn_parse_manifest.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.btn_parse_manifest.clicked.connect(self._parse_manifest)
        # Menu rebuilt dynamically every time it opens (so newly learned formats appear)
        self._parse_menu = QMenu(self.btn_parse_manifest)
        self._parse_menu.aboutToShow.connect(self._rebuild_parse_menu)
        self.btn_parse_manifest.setMenu(self._parse_menu)
        # AI extraction — universal fallback for any unknown format
        self.btn_ai_extract = QPushButton("🤖 Extraction IA universelle (Gemini)")
        self.btn_ai_extract.setStyleSheet(
            "font-weight: bold; background-color: #e7d4ff; color: #4a148c;"
        )
        self.btn_ai_extract.setToolTip(
            "Extraction par IA Google Gemini — fonctionne sur N'IMPORTE QUEL format\n"
            "de manifeste, scanné ou non. Nécessite une clé API Gemini gratuite.\n\n"
            "⏱ Durée : 5-30 s par document."
        )
        self.btn_ai_extract.clicked.connect(self._ai_extract)
        self.btn_ai_learn = QPushButton("🎓 Apprendre ce format à l'IA")
        self.btn_ai_learn.setToolTip(
            "Demande à l'IA d'analyser ce document et d'enregistrer son format\n"
            "pour reconnaissance automatique à l'avenir."
        )
        self.btn_ai_learn.clicked.connect(self._ai_learn_format)
        # Re-open last review without re-running extraction
        self.btn_reopen_review = QPushButton("📋 Ré-ouvrir la dernière revue")
        self.btn_reopen_review.setStyleSheet(
            "font-weight: bold; background-color: #fff3cd; color: #856404;"
        )
        self.btn_reopen_review.setToolTip(
            "Réouvre la fenêtre de révision du dernier résultat extrait\n"
            "sans relancer Gemini ni le parser."
        )
        self.btn_reopen_review.setEnabled(False)
        self.btn_reopen_review.clicked.connect(self._reopen_review)
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
        alay.addWidget(self.btn_ai_extract)
        alay.addWidget(self.btn_ai_learn)
        alay.addWidget(self.btn_reopen_review)
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
        # AI quick-action toolbar buttons
        act_ai = QAction("🤖  Extraction IA", self)
        act_ai.setToolTip("Extraction universelle par IA Gemini — fonctionne sur tout format.")
        act_ai.triggered.connect(self._ai_extract)
        tb.addAction(act_ai)
        tb.addSeparator()

        # Cancel button — only enabled while an AI/OCR worker is running
        self.act_cancel = QAction("🛑  Annuler IA", self)
        self.act_cancel.setToolTip("Interrompre l'extraction IA / l'apprentissage / l'OCR Cloud Vision en cours.")
        self.act_cancel.triggered.connect(self._cancel_ai)
        self.act_cancel.setEnabled(False)
        tb.addAction(self.act_cancel)
        tb.addSeparator()
        self.page_combo = QComboBox()
        self.page_combo.currentIndexChanged.connect(self._switch_page)
        tb.addWidget(QLabel("  Page : "))
        tb.addWidget(self.page_combo)

        # Menu bar — IA settings
        mb = self.menuBar()
        m_ai = mb.addMenu("&IA")
        a_cfg = QAction("⚙ Configurer la clé API Gemini…", self)
        a_cfg.triggered.connect(self._ai_configure)
        m_ai.addAction(a_cfg)
        a_formats = QAction("📚 Gérer les formats appris…", self)
        a_formats.triggered.connect(self._ai_manage_formats)
        m_ai.addAction(a_formats)
        m_ai.addSeparator()
        a_extract = QAction("🤖 Extraction IA du document courant", self)
        a_extract.triggered.connect(self._ai_extract)
        m_ai.addAction(a_extract)
        a_learn = QAction("🎓 Apprendre le format du document courant", self)
        a_learn.triggered.connect(self._ai_learn_format)
        m_ai.addAction(a_learn)
        m_ai.addSeparator()
        a_log = QAction("🐞 Voir le dernier log IA…", self)
        a_log.triggered.connect(self._ai_show_last_log)
        m_ai.addAction(a_log)

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
        learned = None
        sniff_text = ""
        if path_obj.suffix.lower() == ".pdf":
            # Read first 2 pages once for both detection paths
            try:
                import pdfplumber
                with pdfplumber.open(str(path_obj)) as pdf:
                    sniff_text = "\n".join(
                        (p.extract_text() or "") for p in pdf.pages[:2]
                    )
            except Exception:
                sniff_text = ""

            # Priority A: AI-LEARNED format (the user explicitly taught it)
            if sniff_text.strip():
                try:
                    learned = detect_learned(sniff_text)
                except Exception:
                    learned = None

            # Priority B: built-in deterministic parser (CMA CGM, etc.)
            if not learned and self.manifest_parser.available:
                try:
                    fmt = ManifestParser.detect_format(path_obj)
                except Exception:
                    fmt = None

        # ---- Step 2a: LEARNED format with usable template -> fast path ----
        # Render only page 1 immediately, run local parser, load rest in background.
        if learned and template_is_usable(learned.get("parse_template") or {}):
            try:
                import pdfplumber as _plb
                with _plb.open(str(path_obj)) as _pdf:
                    total_pages = len(_pdf.pages)
            except Exception:
                total_pages = None
            self._open_learned_fast(str(path_obj), learned, total_pages)
            return

        # ---- Step 2b: built-in manifest -> propose Smart Parse (no OCR) ----
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

        # ---- Step 2c / 3a: learned-without-parser OR unknown digital PDF ----
        # Both paths render page 1 first, THEN show the appropriate prompt.
        if path_obj.suffix.lower() == ".pdf" and sniff_text.strip():
            self.manifest_mode = False
            self.source_path = path_obj
            try:
                import pdfplumber as _plb
                with _plb.open(str(path_obj)) as _pdf:
                    total_pages = len(_pdf.pages)
            except Exception:
                total_pages = 1
            label = (f"Format appris : {learned.get('name')} — aperçu page 1…"
                     if learned else f"Aperçu de {path_obj.name} (page 1)…")
            self.statusBar().showMessage(label)
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._preview_worker = OCRWorker(self.engine, str(path_obj), max_pages=1)
            pending_learned = learned  # captured for the callback
            self._preview_worker.finished_ok.connect(
                lambda pages, src: self._on_unknown_preview_ready(
                    pages, src, total_pages, pending_learned
                )
            )
            self._preview_worker.failed.connect(self._on_ocr_failed)
            self._preview_worker.start()
            return

        # Scanned PDF (no embedded text) or image: full render + OCR pipeline
        self.manifest_mode = False
        self.statusBar().showMessage(
            f"Rendu des pages de {path_obj.name}… (peut prendre du temps pour les gros PDF)"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.worker = OCRWorker(self.engine, str(path_obj))
        self.worker.finished_ok.connect(self._on_ocr_done)
        self.worker.failed.connect(self._on_ocr_failed)
        self.worker.start()

    def _open_learned_fast(self, path: str, learned: Dict, total_pages: Optional[int]):
        """Fast open for AI-learned formats with a local parse template.

        1. Render only page 1 (~2 s) and show it immediately.
        2. Run parse_with_template (pure pdfplumber regex, < 1 s).
        3. Load remaining pages in background batches — user can navigate while they arrive.
        """
        self.manifest_mode = False
        self.source_path = Path(path)
        self._auto_parse_after_open = learned
        n_label = f" ({total_pages} pages)" if total_pages else ""
        self.statusBar().showMessage(
            f"Format appris : {learned['name']}{n_label} — chargement page 1…"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._learned_total_pages = total_pages or 0
        self._preview_worker = OCRWorker(self.engine, path, max_pages=1)
        self._preview_worker.finished_ok.connect(
            lambda pages, src: self._on_learned_preview_ready(pages, src, total_pages)
        )
        self._preview_worker.failed.connect(self._on_ocr_failed)
        self._preview_worker.start()

    def _on_learned_preview_ready(self, pages: List[Page], source_path: str, total_pages: Optional[int]):
        """Page 1 rendered — show it, run parser, start background load for remaining pages."""
        QApplication.restoreOverrideCursor()
        self.pages = pages
        self.source_path = Path(source_path)
        self.corrections = CorrectionStore(self.source_path)
        self.current_extraction = {}
        self._reset_field_list()

        total = total_pages or 1
        self.page_combo.blockSignals(True)
        self.page_combo.clear()
        self.page_combo.addItem(f"Page 1 / {total}")
        for i in range(2, total + 1):
            self.page_combo.addItem(f"Page {i} / {total} (chargement…)")
        self.page_combo.blockSignals(False)
        self.current_page_idx = 0

        learned = self._auto_parse_after_open
        self.tpl_status.setText(
            f"📄 Format appris : <b>{learned['name']}</b> — parser local en cours…"
        )
        self.active_template = None
        self.canvas.load_page(pages[0], self._on_token_clicked)

        # Start loading remaining pages in background
        if total > 1:
            self._page_load_worker = PageLoadWorker(
                self.engine, source_path, first_page=2, total=total
            )
            self._page_load_worker.pages_ready.connect(self._on_bg_pages_ready)
            self._page_load_worker.progress.connect(
                lambda done, n: self.statusBar().showMessage(
                    f"Chargement pages… {done + 1}/{n + 1}"
                )
            )
            self._page_load_worker.start()

        # Run local parser immediately (pdfplumber, very fast)
        QApplication.processEvents()
        self._parse_manifest()

    def _on_unknown_preview_ready(self, pages: List[Page], source_path: str,
                                   total_pages: int, learned: Optional[Dict] = None):
        """Show page 1 for unknown digital PDFs (or learned-without-parser),
        then ask the user what to do."""
        QApplication.restoreOverrideCursor()
        self.pages = pages
        self.source_path = Path(source_path)
        self.corrections = CorrectionStore(self.source_path)
        self.current_extraction = {}
        self._reset_field_list()

        total = max(total_pages or 1, 1)
        self.page_combo.blockSignals(True)
        self.page_combo.clear()
        self.page_combo.addItem(f"Page 1 / {total}")
        for i in range(2, total + 1):
            self.page_combo.addItem(f"Page {i} / {total} (chargement…)")
        self.page_combo.blockSignals(False)
        self.current_page_idx = 0

        # Show preview immediately
        self.canvas.load_page(pages[0], self._on_token_clicked)

        # Continue background rendering of remaining pages for smooth navigation
        if total > 1:
            self._page_load_worker = PageLoadWorker(
                self.engine, source_path, first_page=2, total=total
            )
            self._page_load_worker.pages_ready.connect(self._on_bg_pages_ready)
            self._page_load_worker.progress.connect(
                lambda done, n: self.statusBar().showMessage(
                    f"Chargement pages… {done + 1}/{n + 1}"
                )
            )
            self._page_load_worker.start()

        # Then ask action — different prompt depending on whether the format
        # is already learned (but parser-less) or completely unknown.
        if learned is not None:
            box = QMessageBox(self)
            box.setWindowTitle(f"Format appris : {learned.get('name', 'inconnu')}")
            box.setIcon(QMessageBox.Question)
            box.setText(
                f"Le format <b>{learned.get('name', '?')}</b> est reconnu, mais aucun parser local "
                f"n'est disponible.<br><br><b>Que faire ?</b>"
            )
            btn_ai = box.addButton("🤖 Extraction IA", QMessageBox.AcceptRole)
            btn_relearn = box.addButton("🎓 Ré-apprendre ce format", QMessageBox.ActionRole)
            box.addButton("Annuler", QMessageBox.RejectRole)
            box.exec_()
            clicked = box.clickedButton()
            if clicked is btn_ai:
                self._run_ai_extraction(
                    extra_hints=learned.get("extraction_hints", ""),
                    example_rows=learned.get("example_rows") or [],
                )
            elif clicked is btn_relearn:
                self._ai_learn_format()
            return

        box = QMessageBox(self)
        box.setWindowTitle("Format inconnu")
        box.setIcon(QMessageBox.Question)
        box.setText(
            "Aucun format intégré reconnu pour ce document.<br><br>"
            "<b>Que faire ?</b>"
        )
        btn_ai = box.addButton("🤖 Extraction IA (recommandé)", QMessageBox.AcceptRole)
        btn_learn = box.addButton("🎓 Apprendre ce format à l'IA", QMessageBox.ActionRole)
        btn_manual = box.addButton("Choisir manuellement…", QMessageBox.ActionRole)
        box.addButton("Annuler", QMessageBox.RejectRole)
        box.exec_()
        clicked = box.clickedButton()
        if clicked is btn_ai:
            self._run_ai_extraction()
            return
        if clicked is btn_learn:
            self._ai_learn_format()
            return
        if clicked is btn_manual:
            fmt, ok = QInputDialog.getItem(
                self, "Format de manifeste",
                "Sélectionner le parser à utiliser :",
                ["cma_cgm", "maersk", "msc", "generic"],
                0, False,
            )
            if ok:
                self._open_in_manifest_mode(str(self.source_path), fmt)

    def _on_bg_pages_ready(self, new_pages: List[Page], start_idx: int):
        """Background worker delivered a batch of rendered pages."""
        # Extend self.pages to accommodate
        needed = start_idx + len(new_pages)
        while len(self.pages) < needed:
            self.pages.append(None)  # type: ignore[arg-type]
        for i, p in enumerate(new_pages):
            self.pages[start_idx + i] = p
        # Update combo labels (remove "chargement…")
        self.page_combo.blockSignals(True)
        total = len(self.pages)
        for i, p in enumerate(new_pages):
            combo_idx = start_idx + i
            if combo_idx < self.page_combo.count():
                self.page_combo.setItemText(combo_idx, f"Page {combo_idx + 1} / {total}")
        self.page_combo.blockSignals(False)

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
        page = self.pages[idx] if idx < len(self.pages) else None
        if page is None:
            # Page not yet rendered by background worker — ask user to wait
            self.statusBar().showMessage(
                f"Page {idx + 1} en cours de chargement, veuillez patienter…"
            )
            return
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
    def _rebuild_parse_menu(self):
        """Rebuild the 'Analyse intelligente' dropdown each time it opens.

        Lists: auto-detect | every learned format (force apply) | learn new | AI extract.
        """
        menu = self._parse_menu
        menu.clear()

        a_auto = menu.addAction("⚡ Auto-détection (recommandé)")
        a_auto.triggered.connect(self._parse_manifest)

        # List all learned formats — user can FORCE one regardless of detection.
        try:
            learned_list = list_learned()
        except Exception:
            learned_list = []
        usable = [f for f in learned_list if template_is_usable(f.get("parse_template") or {})]

        if usable:
            menu.addSeparator()
            menu.addAction("Forcer un parseur spécifique :").setEnabled(False)
            for fmt in usable:
                tpl = fmt.get("parse_template") or {}
                rc = tpl.get("row_count")
                origin = "fait main" if fmt.get("model") == "handcrafted" else (fmt.get("model") or "ia")
                label = f"  ▸ {fmt.get('name', '?')}  ({origin}"
                if rc:
                    label += f", {rc} lignes échantillon"
                label += ")"
                act = menu.addAction(label)
                act.triggered.connect(lambda _checked=False, f=fmt: self._apply_specific_parser(f))

        menu.addSeparator()
        a_learn = menu.addAction("🎓 Apprendre / régénérer un parseur via IA…")
        a_learn.triggered.connect(self._ai_learn_format)
        a_ai = menu.addAction("🤖 Extraction IA universelle (sans parseur local)")
        a_ai.triggered.connect(self._ai_extract)

    def _apply_specific_parser(self, learned: Dict):
        """Force-apply a chosen learned format's parser to the current document."""
        if not self.source_path:
            QMessageBox.information(self, "Aucun document", "Ouvrez d'abord un document.")
            return
        tpl = learned.get("parse_template") or {}
        if not template_is_usable(tpl):
            QMessageBox.warning(
                self, "Parseur indisponible",
                f"Le format <b>{learned.get('name', '?')}</b> n'a pas de parseur local utilisable."
            )
            return
        self.statusBar().showMessage(
            f"Parseur forcé : {learned.get('name', '?')}…"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            rows = parse_with_template(str(self.source_path), tpl)
        except Exception as e:
            QApplication.restoreOverrideCursor()
            QMessageBox.warning(self, "Erreur parser local", str(e))
            return
        QApplication.restoreOverrideCursor()
        if not rows:
            QMessageBox.information(
                self, "Aucune ligne",
                f"Le parseur <b>{learned.get('name', '?')}</b> n'a extrait aucune ligne "
                f"de ce document. Le format est probablement différent."
            )
            return
        self._on_parse_done(rows, str(self.source_path))

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

        # ---- Priority 1: AI-LEARNED format WITH a usable local parse template ----
        # The user explicitly taught this format — trust it over generic built-ins.
        learned = self._detect_learned_format()
        if learned and template_is_usable(learned.get("parse_template") or {}):
            tpl = learned["parse_template"]
            self.statusBar().showMessage(
                f"Format appris détecté : {learned['name']} — parser local…"
            )
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                rows = parse_with_template(str(self.source_path), tpl)
            except Exception as e:
                QApplication.restoreOverrideCursor()
                QMessageBox.warning(self, "Erreur parser local", str(e))
                rows = []
            QApplication.restoreOverrideCursor()
            if rows:
                self._on_parse_done(rows, str(self.source_path))
                return
            # Empty result — propose AI fallback
            if QMessageBox.question(
                self, "Parser local sans résultat",
                f"Le parser local du format <b>{learned['name']}</b> n'a extrait "
                f"aucune ligne sur ce document.<br><br>"
                f"Lancer une extraction IA en repli ?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes,
            ) == QMessageBox.Yes:
                self._run_ai_extraction(
                    extra_hints=learned.get("extraction_hints", ""),
                    example_rows=learned.get("example_rows") or [],
                )
            return

        # ---- Priority 2: built-in deterministic parsers (CMA CGM, etc.) ----
        fmt = ManifestParser.detect_format(self.source_path)

        # ---- Priority 3: legacy learned format without template — ask user ----
        if learned and not fmt:
            if QMessageBox.question(
                self, f"Format appris : {learned['name']}",
                f"Le format <b>{learned['name']}</b> est reconnu mais aucun "
                f"parser local n'a été généré (ancien apprentissage).<br><br>"
                f"Lancer l'extraction <b>IA</b> ?<br>"
                f"<i>Astuce : utilisez « Apprendre le format » à nouveau pour "
                f"générer un parser local rapide.</i>",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes,
            ) == QMessageBox.Yes:
                self._run_ai_extraction(
                    extra_hints=learned.get("extraction_hints", ""),
                    example_rows=learned.get("example_rows") or [],
                )
            return

        # ── Scanned document (SAKINA, etc.) ─────────────────────────
        if fmt == "sakina" or (fmt is None and not self._has_embedded_text()):
            self._run_scanned_with_engine_choice(fmt)
            return

        if fmt is None:
            # No built-in format detected — propose AI fallback (works on any layout)
            box = QMessageBox(self)
            box.setWindowTitle("Format inconnu")
            box.setIcon(QMessageBox.Question)
            box.setText(
                "Aucun format intégré reconnu pour ce document.<br><br>"
                "<b>Que faire ?</b>"
            )
            btn_ai = box.addButton("🤖 Extraction IA (recommandé)", QMessageBox.AcceptRole)
            btn_learn = box.addButton("🎓 Apprendre ce format à l'IA", QMessageBox.ActionRole)
            btn_manual = box.addButton("Choisir manuellement…", QMessageBox.ActionRole)
            btn_cancel = box.addButton("Annuler", QMessageBox.RejectRole)
            box.exec_()
            clicked = box.clickedButton()
            if clicked is btn_ai:
                self._run_ai_extraction()
                return
            if clicked is btn_learn:
                self._ai_learn_format()
                return
            if clicked is btn_manual:
                fmt, ok = QInputDialog.getItem(
                    self, "Format de manifeste",
                    "Sélectionner le parser à utiliser :",
                    ["cma_cgm", "maersk", "msc", "generic"],
                    0, False,
                )
                if not ok:
                    return
            else:
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
        # Cache for re-open (user can come back without re-running)
        self._last_extract_rows = list(rows)
        self._last_extract_source = str(source_path)
        self.btn_reopen_review.setEnabled(True)
        # Open the editable review dialog
        src_path = Path(source_path)
        dlg = ManifestReviewDialog(rows, src_path, parent=self)
        dlg.exec_()
        self.corrections = CorrectionStore(src_path)
        edited_rows = dlg.rows
        self.extracted_rows.extend(edited_rows)
        self.queue_label.setText(f"File : {len(self.extracted_rows)} lignes")
        self.statusBar().showMessage(
            f"✓ {len(edited_rows)} conteneurs ajoutés à la file depuis "
            f"{src_path.name}. Cliquez sur « Exporter » pour générer l'Excel."
        )

    def _reopen_review(self):
        """Re-open the last review dialog without re-running extraction."""
        if not self._last_extract_rows:
            return
        src_path = Path(self._last_extract_source) if self._last_extract_source else None
        rows_copy = [dict(r) for r in self._last_extract_rows]
        dlg = ManifestReviewDialog(rows_copy, src_path, parent=self)
        dlg.exec_()
        if dlg.rows != rows_copy:
            reply = QMessageBox.question(
                self, "Mettre à jour la file ?",
                "Des modifications ont été apportées.<br><br>"
                "Voulez-vous <b>remplacer</b> le dernier lot de la file "
                f"({len(rows_copy)} lignes) par la version révisée ?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes,
            )
            if reply == QMessageBox.Yes:
                n = len(rows_copy)
                del self.extracted_rows[-n:]
                self.extracted_rows.extend(dlg.rows)
                self.queue_label.setText(f"File : {len(self.extracted_rows)} lignes")
                self.statusBar().showMessage(
                    f"✓ File mise à jour : {len(self.extracted_rows)} lignes."
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

    def _run_scanned_with_engine_choice(self, fmt):
        """Ask user which OCR engine to use for a scanned PDF (or auto from pref)."""
        from PyQt5.QtWidgets import QCheckBox
        pref = get_ocr_engine()

        if pref == "cloud_vision":
            engine = "cloud_vision"
        elif pref == "local":
            engine = "local"
        else:
            # No saved preference — ask
            box = QMessageBox(self)
            box.setWindowTitle("Document scanné détecté")
            box.setIcon(QMessageBox.Question)
            box.setText(
                "Ce document est un <b>scan</b> (texte non intégré).<br><br>"
                "<b>Quel moteur OCR utiliser ?</b>"
            )
            btn_cloud = box.addButton(
                "☁ Cloud Vision (rapide, qualité supérieure)", QMessageBox.AcceptRole
            )
            btn_local = box.addButton(
                "💻 OCR local pytesseract (hors-ligne)", QMessageBox.ActionRole
            )
            btn_cancel = box.addButton("Annuler", QMessageBox.RejectRole)

            # "Remember" checkbox inside message box
            cb_remember = QCheckBox("Retenir ce choix pour les prochains documents")
            box.setCheckBox(cb_remember)

            box.exec_()
            clicked = box.clickedButton()
            if clicked is btn_cloud:
                engine = "cloud_vision"
            elif clicked is btn_local:
                engine = "local"
            else:
                return
            if cb_remember.isChecked():
                set_ocr_engine(engine)

        if engine == "cloud_vision":
            if not ensure_api_key(self):
                return
            self._run_ai_extraction()
        else:
            self._run_scanned_extraction(fmt or "sakina")

    def _run_scanned_extraction(self, fmt: str):
        self.statusBar().showMessage(f"OCR + extraction ({fmt.upper()})…  ⏳")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._scanned_worker = ScannedExtractWorker(str(self.source_path), fmt)
        self._scanned_worker.finished_ok.connect(self._on_parse_done)
        self._scanned_worker.failed.connect(self._on_parse_failed)
        self._scanned_worker.start()

    # ============================================================
    # AI extraction (Google Gemini) — universal fallback
    # ============================================================
    def _detect_learned_format(self):
        """Return matching learned-format dict for the current document, or None."""
        if not self.source_path:
            return None
        try:
            import pdfplumber
            with pdfplumber.open(str(self.source_path)) as pdf:
                # Read up to first 2 pages for signature matching
                chunks = []
                for p in pdf.pages[:2]:
                    chunks.append(p.extract_text() or "")
                text = "\n".join(chunks)
        except Exception:
            text = ""
        if not text.strip():
            # Fallback: use OCR'd text from already-loaded pages, if any
            if self.pages:
                text = " ".join(t.text for t in (self.pages[0].tokens or []) if t.text)
        return detect_learned(text) if text else None

    def _ai_configure(self):
        GeminiConfigDialog(self).exec_()

    def _ai_manage_formats(self):
        LearnedFormatsDialog(self).exec_()

    def _ai_extract(self):
        """Toolbar/menu entry: launch universal AI extraction on the current PDF."""
        if not self.source_path:
            QMessageBox.information(self, "Aucun document", "Ouvrez d'abord un document.")
            return
        if not str(self.source_path).lower().endswith(".pdf"):
            QMessageBox.warning(self, "Format non supporté",
                                "L'extraction IA ne fonctionne que sur les fichiers PDF.")
            return
        # Auto-pick learned hints + few-shot examples if a format is known for this doc
        learned = self._detect_learned_format()
        hints = learned.get("extraction_hints", "") if learned else ""
        examples = learned.get("example_rows") or [] if learned else []
        self._run_ai_extraction(extra_hints=hints, example_rows=examples)

    def _run_ai_extraction(self, extra_hints: str = "", example_rows: list | None = None):
        if not ensure_api_key(self):
            return
        self.statusBar().showMessage("🤖 Extraction IA en cours…  ⏳")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._ai_worker = AIExtractWorker(
            str(self.source_path),
            extra_hints=extra_hints,
            example_rows=example_rows or [],
        )
        self._ai_worker.progress.connect(
            lambda msg: self.statusBar().showMessage(f"🤖 {msg}")
        )
        self._ai_worker.finished_ok.connect(self._on_ai_extract_done)
        self._ai_worker.failed.connect(self._on_parse_failed)
        self._ai_worker.cancelled.connect(self._on_ai_cancelled)
        self.act_cancel.setEnabled(True)
        self._ai_worker.start()

    def _on_ai_extract_done(self, rows: list, source_path: str):
        QApplication.restoreOverrideCursor()
        self.act_cancel.setEnabled(False)
        if not rows:
            QMessageBox.warning(
                self, "Aucune donnée extraite",
                "L'IA n'a renvoyé aucune ligne. Vérifiez que le document est bien un manifeste."
            )
            self.statusBar().showMessage("Extraction IA — 0 ligne.")
            return
        # Reuse the same review pipeline as the deterministic parser
        self._on_parse_done(rows, source_path)

    def _on_ai_cancelled(self):
        QApplication.restoreOverrideCursor()
        self.act_cancel.setEnabled(False)
        self.statusBar().showMessage("⏹ Extraction IA annulée par l'utilisateur.")

    def _cancel_ai(self):
        """Stop any running AI/OCR worker (extract or learn)."""
        stopped = False
        for attr in ("_ai_worker", "_learn_worker"):
            w = getattr(self, attr, None)
            if w is not None and w.isRunning():
                try:
                    w.cancel()
                    stopped = True
                except Exception:
                    pass
        if stopped:
            self.statusBar().showMessage("⏹ Demande d'annulation envoyée…")
        else:
            self.act_cancel.setEnabled(False)

    def _ai_learn_format(self):
        """Ask the AI to classify the current document and persist the format."""
        if not self.source_path:
            QMessageBox.information(self, "Aucun document", "Ouvrez d'abord un document.")
            return
        if not str(self.source_path).lower().endswith(".pdf"):
            QMessageBox.warning(self, "Format non supporté",
                                "L'apprentissage IA ne fonctionne que sur les PDF.")
            return
        if not ensure_api_key(self):
            return
        self.statusBar().showMessage("🎓 Analyse du format en cours…  ⏳")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._learn_worker = AILearnWorker(str(self.source_path))
        self._learn_worker.finished_ok.connect(self._on_ai_learn_done)
        self._learn_worker.failed.connect(self._on_ai_learn_failed)
        self._learn_worker.cancelled.connect(self._on_ai_cancelled)
        self.act_cancel.setEnabled(True)
        self._learn_worker.start()

    def _on_ai_learn_done(self, learned: dict):
        QApplication.restoreOverrideCursor()
        self.act_cancel.setEnabled(False)
        tpl = (learned or {}).get("parse_template") or {}
        has_name = bool((learned or {}).get("format_name"))
        has_sig = bool((learned or {}).get("signature_keywords"))
        has_code = bool((tpl.get("parse_code") or "").strip())
        if not (has_name or has_sig or has_code):
            self.statusBar().showMessage("Échec de l'analyse IA (réponse vide).")
            QMessageBox.warning(
                self, "Apprentissage IA incomplet",
                "L'IA a répondu avec un JSON incomplet/invalide.\n\n"
                "Ouvrez le dernier log IA, puis réessayez l'apprentissage."
            )
            return
        self.statusBar().showMessage("Format analysé — vérification utilisateur.")
        dlg = LearnedSummaryDialog(learned, parent=self)
        if dlg.exec_() == dlg.Accepted:
            # Offer to extract right away with the freshly-learned hints
            reply = QMessageBox.question(
                self, "Extraire maintenant ?",
                "Format enregistré.<br><br>Lancer l'extraction IA sur ce document maintenant ?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes,
            )
            if reply == QMessageBox.Yes:
                hints = learned.get("extraction_hints", "")
                examples = learned.get("example_rows") or []
                self._run_ai_extraction(extra_hints=hints, example_rows=examples)

    def _on_ai_learn_failed(self, msg: str):
        QApplication.restoreOverrideCursor()
        self.act_cancel.setEnabled(False)
        self.statusBar().showMessage("Échec de l'analyse IA.")
        # Self-heal: detect the "google-genai not installed" case and offer a
        # one-click install. Needed to recover users whose updater is too old
        # to refresh requirements properly (the updater itself is a shipped .exe).
        if "google-genai" in msg or "google.genai" in msg or "No module named 'google'" in msg:
            box = QMessageBox(self)
            box.setIcon(QMessageBox.Warning)
            box.setWindowTitle("Dépendance manquante")
            box.setText(
                "Le package <b>google-genai</b> requis pour l'apprentissage IA "
                "n'est pas installé dans cet environnement.<br><br>"
                "Voulez-vous l'installer maintenant ? (~30 s, connexion internet requise)"
            )
            box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            box.setDefaultButton(QMessageBox.Yes)
            if box.exec_() == QMessageBox.Yes:
                self._install_google_genai()
            return
        QMessageBox.critical(self, "Erreur IA", msg)

    def _install_google_genai(self):
        """One-shot pip install of google-genai using the current Python."""
        import sys, subprocess
        self.statusBar().showMessage("Installation de google-genai en cours…")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--upgrade",
                 "--disable-pip-version-check", "google-genai"],
                capture_output=True, text=True, timeout=180,
            )
        except Exception as e:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Installation échouée", str(e))
            return
        QApplication.restoreOverrideCursor()
        if proc.returncode == 0:
            QMessageBox.information(
                self, "Installation réussie",
                "google-genai a été installé.<br><br>"
                "<b>Veuillez redémarrer l'application</b> pour que la nouvelle "
                "bibliothèque soit chargée."
            )
            self.statusBar().showMessage("google-genai installé — redémarrez l'application.")
        else:
            QMessageBox.critical(
                self, "Installation échouée",
                f"pip a quitté avec le code {proc.returncode}.<br><br>"
                f"<pre>{(proc.stderr or proc.stdout)[-2000:]}</pre>"
            )

    def _ai_show_last_log(self):
        """Open the most recent AI debug log (text + prompt + Gemini reply)."""
        from ..ai.debug_log import get_last_log_path
        path = get_last_log_path()
        if not path:
            QMessageBox.information(
                self, "Aucun log",
                "Aucune interaction IA n'a encore été enregistrée."
            )
            return
        try:
            content = path.read_text(encoding="utf-8")
        except Exception as e:
            QMessageBox.critical(self, "Erreur lecture", str(e))
            return
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Log IA — {path.name}")
        dlg.resize(1000, 700)
        v = QVBoxLayout(dlg)
        v.addWidget(QLabel(f"<b>Fichier :</b> <code>{path}</code>"))
        ed = QPlainTextEdit(content)
        ed.setReadOnly(True)
        ed.setStyleSheet("font-family: Consolas, monospace; font-size: 11px;")
        v.addWidget(ed, 1)
        h = QHBoxLayout()
        btn_open = QPushButton("📂 Ouvrir le dossier des logs")
        def _open_folder():
            import os, subprocess
            subprocess.Popen(f'explorer "{path.parent}"')
        btn_open.clicked.connect(_open_folder)
        h.addWidget(btn_open)
        h.addStretch(1)
        btn_close = QPushButton("Fermer")
        btn_close.clicked.connect(dlg.accept)
        h.addWidget(btn_close)
        v.addLayout(h)
        dlg.exec_()



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
