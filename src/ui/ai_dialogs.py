"""AI-related Qt dialogs and workers — keeps main_window.py lean."""
from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Optional

from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QLineEdit, QPushButton,
    QLabel, QComboBox, QMessageBox, QListWidget, QListWidgetItem, QPlainTextEdit,
    QDialogButtonBox, QCheckBox, QGroupBox,
)

from ..ai import (
    get_api_key, set_api_key, has_api_key,
    AVAILABLE_MODELS, get_model_name, set_model_name,
    extract_rows_from_pdf, learn_format_from_pdf, ai_fix_row,
    list_learned, save_learned, delete_learned,
    get_ocr_engine, set_ocr_engine,
)
from ..ai.gemini_client import get_vision_api_key, set_vision_api_key


# ============================================================
# Workers
# ============================================================
class AIExtractWorker(QThread):
    """Run Gemini extraction off the UI thread."""
    finished_ok = pyqtSignal(list, str)        # rows, source_path
    failed = pyqtSignal(str)
    progress = pyqtSignal(str)                 # status text
    cancelled = pyqtSignal()                   # emitted when stopped by user

    def __init__(self, file_path: str, extra_hints: str = "", example_rows: Optional[List[Dict]] = None):
        super().__init__()
        self.file_path = file_path
        self.extra_hints = extra_hints
        self.example_rows = example_rows or []
        self._cancel = False

    def cancel(self):
        self._cancel = True
        self.requestInterruption()

    def run(self):
        from ..ai.ai_extractor import AICancelled
        try:
            rows = extract_rows_from_pdf(
                self.file_path,
                extra_hints=self.extra_hints,
                example_rows=self.example_rows,
                progress_cb=lambda msg: self.progress.emit(msg),
                cancel_check=lambda: self._cancel,
            )
            self.finished_ok.emit(rows, self.file_path)
        except AICancelled:
            self.cancelled.emit()
        except Exception as e:
            import traceback
            self.failed.emit(f"{e}\n\n{traceback.format_exc()}")


class ScannedTemplateWorker(QThread):
    """OCR a scanned PDF then run a learned template's `parse(text)` on the OCR text.

    Used when the user picks a learned format with `is_scanned=True`. Keeps
    the UI responsive during the (often long) Cloud Vision OCR pass.
    """
    finished_ok = pyqtSignal(list, str)        # rows, source_path
    failed = pyqtSignal(str)
    progress = pyqtSignal(str)
    cancelled = pyqtSignal()

    def __init__(self, file_path: str, template: Dict, format_name: str = ""):
        super().__init__()
        self.file_path = file_path
        self.template = template
        self.format_name = format_name
        self._cancel = False

    def cancel(self):
        self._cancel = True
        self.requestInterruption()

    def run(self):
        from ..ai import vision_client
        from ..ai.template_parser import parse_with_template
        from ..ai.ai_extractor import AICancelled
        try:
            self.progress.emit(f"OCR Cloud Vision pour le format {self.format_name}…")
            text = vision_client.ocr_pdf(
                self.file_path,
                progress_cb=lambda i, n: self.progress.emit(
                    f"OCR Cloud Vision page {i}/{n}…"
                ),
                cancel_check=lambda: self._cancel,
            )
            if self._cancel:
                self.cancelled.emit()
                return
            self.progress.emit(f"Parser local {self.format_name}…")
            rows = parse_with_template(self.file_path, self.template, text_override=text)
            self.finished_ok.emit(rows, self.file_path)
        except AICancelled:
            self.cancelled.emit()
        except Exception as e:
            import traceback
            self.failed.emit(f"{e}\n\n{traceback.format_exc()}")


class AILearnWorker(QThread):
    """Run format learning off the UI thread."""
    finished_ok = pyqtSignal(dict)
    failed = pyqtSignal(str)
    cancelled = pyqtSignal()

    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path
        self._cancel = False

    def cancel(self):
        self._cancel = True
        self.requestInterruption()

    def run(self):
        from ..ai.ai_extractor import AICancelled
        try:
            data = learn_format_from_pdf(self.file_path, cancel_check=lambda: self._cancel)
            self.finished_ok.emit(data)
        except AICancelled:
            self.cancelled.emit()
        except Exception as e:
            import traceback
            self.failed.emit(f"{e}\n\n{traceback.format_exc()}")


class AIFixWorker(QThread):
    """Run per-row AI correction off the UI thread."""
    finished_ok = pyqtSignal(int, dict)        # row_index, fixed_row
    failed = pyqtSignal(int, str)              # row_index, error

    def __init__(self, row_index: int, row: Dict, issues: List[str], context: str):
        super().__init__()
        self.row_index = row_index
        self.row = row
        self.issues = issues
        self.context = context

    def run(self):
        try:
            fixed = ai_fix_row(self.row, self.issues, self.context)
            self.finished_ok.emit(self.row_index, fixed)
        except Exception as e:
            self.failed.emit(self.row_index, str(e))


# ============================================================
# Dialogs
# ============================================================
class GeminiConfigDialog(QDialog):
    """Configure API key + model preference."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuration IA — Google Gemini & Cloud Vision")
        self.resize(620, 420)

        lay = QVBoxLayout(self)

        info = QLabel(
            "<b>Clés API Google</b><br>"
            "• <b>Gemini</b> : structuration des manifestes (texte). Clé gratuite sur "
            "<a href='https://aistudio.google.com/apikey'>aistudio.google.com/apikey</a>.<br>"
            "• <b>Cloud Vision</b> : OCR rapide pour gros volumes (PDF scannés). Activez "
            "<i>Cloud Vision API</i> sur <a href='https://console.cloud.google.com/apis/library/vision.googleapis.com'>console.cloud.google.com</a>. "
            "Si laissé vide, la clé Gemini sera réutilisée (même projet GCP).<br><br>"
            "Les clés sont stockées dans <code>data/ai_config.json</code> (gitignoré)."
        )
        info.setOpenExternalLinks(True)
        info.setWordWrap(True)
        lay.addWidget(info)

        form = QFormLayout()
        self.key_edit = QLineEdit()
        self.key_edit.setEchoMode(QLineEdit.Password)
        self.key_edit.setPlaceholderText("AIzaSy…")
        existing = get_api_key()
        if existing:
            self.key_edit.setText(existing)
        form.addRow("Clé Gemini :", self.key_edit)

        self.vision_edit = QLineEdit()
        self.vision_edit.setEchoMode(QLineEdit.Password)
        self.vision_edit.setPlaceholderText("(optionnelle — même clé que Gemini si vide)")
        existing_vision = get_vision_api_key()
        if existing_vision:
            self.vision_edit.setText(existing_vision)
        form.addRow("Clé Cloud Vision :", self.vision_edit)

        self.show_key = QCheckBox("Afficher les clés")
        def _toggle(c):
            mode = QLineEdit.Normal if c else QLineEdit.Password
            self.key_edit.setEchoMode(mode)
            self.vision_edit.setEchoMode(mode)
        self.show_key.toggled.connect(_toggle)
        form.addRow("", self.show_key)

        self.model_combo = QComboBox()
        self.model_combo.addItems(AVAILABLE_MODELS)
        cur = get_model_name()
        if cur in AVAILABLE_MODELS:
            self.model_combo.setCurrentText(cur)
        form.addRow("Modèle Gemini :", self.model_combo)

        lay.addLayout(form)

        # ── OCR engine preference ────────────────────────────────────────
        from PyQt5.QtWidgets import QButtonGroup, QRadioButton
        ocr_group = QGroupBox("Moteur OCR pour les documents scannés")
        ocr_lay = QVBoxLayout(ocr_group)
        ocr_lay.addWidget(QLabel(
            "Quel moteur utiliser lorsque l'application détecte un PDF scanné ?"
        ))
        self.rb_ask = QRadioButton("Demander à chaque fois")
        self.rb_cloud = QRadioButton(
            "Cloud Vision (recommandé — rapide, qualité supérieure, nécessite clé API)"
        )
        self.rb_local = QRadioButton(
            "OCR local pytesseract (hors-ligne, gratuit, plus lent)"
        )
        self._ocr_bg = QButtonGroup(self)
        self._ocr_bg.addButton(self.rb_ask,   0)
        self._ocr_bg.addButton(self.rb_cloud, 1)
        self._ocr_bg.addButton(self.rb_local, 2)
        pref = get_ocr_engine()
        if pref == "cloud_vision":
            self.rb_cloud.setChecked(True)
        elif pref == "local":
            self.rb_local.setChecked(True)
        else:
            self.rb_ask.setChecked(True)
        ocr_lay.addWidget(self.rb_ask)
        ocr_lay.addWidget(self.rb_cloud)
        ocr_lay.addWidget(self.rb_local)
        lay.addWidget(ocr_group)

        hint = QLabel(
            "<i>Recommandé : <b>gemini-2.5-flash</b> + Cloud Vision activé sur le même projet GCP. "
            "Avec un crédit GCP de 300 $, vous pouvez traiter ~200 000 pages OCR avant facturation.</i>"
        )
        hint.setStyleSheet("color: #555; font-size: 11px;")
        hint.setWordWrap(True)
        lay.addWidget(hint)

        bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        bb.accepted.connect(self._save)
        bb.rejected.connect(self.reject)
        lay.addWidget(bb)

    def _save(self):
        key = self.key_edit.text().strip()
        if not key:
            QMessageBox.warning(self, "Clé manquante", "Veuillez saisir une clé API Gemini.")
            return
        set_api_key(key)
        set_vision_api_key(self.vision_edit.text().strip())
        set_model_name(self.model_combo.currentText())
        if self.rb_cloud.isChecked():
            set_ocr_engine("cloud_vision")
        elif self.rb_local.isChecked():
            set_ocr_engine("local")
        else:
            set_ocr_engine("")
        QMessageBox.information(self, "Enregistré", "Configuration IA enregistrée.")
        self.accept()


class LearnedFormatsDialog(QDialog):
    """Manage the list of AI-learned formats (view, delete)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Formats appris par l'IA")
        self.resize(700, 480)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(
            "<b>Formats reconnus automatiquement.</b> "
            "Lorsqu'un PDF correspond à l'une des signatures ci-dessous, "
            "l'application l'extrait directement avec les indications apprises."
        ))

        self.lst = QListWidget()
        lay.addWidget(self.lst, 1)

        self.detail = QPlainTextEdit()
        self.detail.setReadOnly(True)
        self.detail.setMaximumHeight(180)
        lay.addWidget(self.detail)

        self.lst.currentRowChanged.connect(self._show_detail)

        btns = QHBoxLayout()
        btn_del = QPushButton("🗑 Supprimer")
        btn_del.clicked.connect(self._delete_current)
        btns.addWidget(btn_del)
        btns.addStretch(1)
        btn_close = QPushButton("Fermer")
        btn_close.clicked.connect(self.accept)
        btns.addWidget(btn_close)
        lay.addLayout(btns)

        self._reload()

    def _reload(self):
        self.lst.clear()
        self._items: List[Dict] = list_learned()
        if not self._items:
            self.lst.addItem("(aucun format appris pour le moment)")
            self.lst.setEnabled(False)
            return
        self.lst.setEnabled(True)
        for fmt in self._items:
            sig_count = len(fmt.get("signature") or [])
            scan_tag = " 📷" if fmt.get("is_scanned") else ""
            label = f"{fmt.get('name', '?')}{scan_tag}  —  {fmt.get('carrier', '')}  ({sig_count} mots-clés)"
            self.lst.addItem(QListWidgetItem(label))

    def _show_detail(self, idx: int):
        if not self._items or idx < 0 or idx >= len(self._items):
            self.detail.clear()
            return
        f = self._items[idx]
        import json
        self.detail.setPlainText(json.dumps(
            {k: v for k, v in f.items() if not k.startswith("_")},
            indent=2, ensure_ascii=False,
        ))

    def _delete_current(self):
        idx = self.lst.currentRow()
        if not self._items or idx < 0 or idx >= len(self._items):
            return
        name = self._items[idx].get("name", "")
        if QMessageBox.question(
            self, "Supprimer",
            f"Supprimer le format appris « {name} » ?",
        ) != QMessageBox.Yes:
            return
        delete_learned(name)
        self._reload()


class LearnedSummaryDialog(QDialog):
    """Show the result of learn_format_from_pdf and let the user edit/save it."""

    def __init__(self, learned: Dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Format détecté par l'IA")
        self.resize(620, 480)
        self._learned = learned or {}

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(
            "<b>L'IA a analysé le document.</b> Vérifiez et ajustez si besoin avant d'enregistrer."
        ))

        form = QFormLayout()
        self.name_edit = QLineEdit(str(self._learned.get("format_name", "")))
        form.addRow("Nom court :", self.name_edit)
        self.carrier_edit = QLineEdit(str(self._learned.get("carrier_name", "")))
        form.addRow("Transporteur :", self.carrier_edit)
        lay.addLayout(form)

        lay.addWidget(QLabel("<b>Mots-clés de signature</b> (un par ligne — utilisés pour reconnaître ce format à l'avenir) :"))
        self.sig_edit = QPlainTextEdit("\n".join(self._learned.get("signature_keywords") or []))
        self.sig_edit.setMaximumHeight(120)
        lay.addWidget(self.sig_edit)

        lay.addWidget(QLabel("<b>Indications d'extraction</b> (transmises à l'IA lors des prochaines extractions) :"))
        self.hints_edit = QPlainTextEdit(str(self._learned.get("extraction_hints", "")))
        lay.addWidget(self.hints_edit)

        self.scanned_chk = QCheckBox("Document scanné (utiliser la vision Gemini)")
        self.scanned_chk.setChecked(bool(self._learned.get("is_scanned")))
        lay.addWidget(self.scanned_chk)

        # Show how many example rows were captured (re-used as few-shot next time)
        ex_rows = self._learned.get("example_rows") or []
        ex_label = QLabel(
            f"<b>Exemples extraits :</b> {len(ex_rows)} ligne(s) mémorisée(s) — "
            "l'IA s'en servira comme référence sur les prochains documents de ce format."
            if ex_rows else
            "<i>⚠ Aucune ligne d'exemple n'a pu être extraite (extraction échouée ou format vide). "
            "Le format sera tout de même reconnu, mais sans référence few-shot.</i>"
        )
        ex_label.setWordWrap(True)
        ex_label.setStyleSheet("color: #2a6f2a; font-size: 11px;" if ex_rows else "color: #a05a00; font-size: 11px;")
        lay.addWidget(ex_label)

        # Indicate whether an AI-generated parse function was produced.
        # When present, future extractions of this format run LOCALLY (no Gemini
        # call) which is fast and free.
        tpl = self._learned.get("parse_template") or {}
        has_code = bool((tpl.get("parse_code") or "").strip())
        n_row_pat = len((tpl.get("row_patterns") or []))
        n_hdr_pat = len((tpl.get("header_field_patterns") or {}))
        n_sample = self._learned.get("_local_row_count_on_sample")
        if has_code or n_row_pat > 0:
            kind = "fonction Python parse()" if has_code else f"{n_row_pat} pattern(s) de ligne"
            extra = f" — {n_sample} ligne(s) extraite(s) sur l'échantillon" if isinstance(n_sample, int) else ""
            tpl_label = QLabel(
                f"<b>⚙️ Parser local généré :</b> {kind}, {n_hdr_pat} champ(s) d'en-tête{extra}. "
                f"Les prochains documents de ce format seront extraits LOCALEMENT (sans IA, instantané)."
            )
            tpl_label.setStyleSheet("color: #2a6f2a; font-size: 11px;")
        else:
            tpl_label = QLabel(
                "<i>⚠ Aucun parser local généré — les prochaines extractions de ce format "
                "passeront par l'IA (lent). Vous pouvez ré-apprendre pour réessayer.</i>"
            )
            tpl_label.setStyleSheet("color: #a05a00; font-size: 11px;")
        tpl_label.setWordWrap(True)
        lay.addWidget(tpl_label)

        bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        bb.button(QDialogButtonBox.Save).setText("💾 Enregistrer le format")
        bb.accepted.connect(self._save)
        bb.rejected.connect(self.reject)
        lay.addWidget(bb)

    def _save(self):
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Nom requis", "Donnez un nom court à ce format.")
            return
        sig = [s.strip() for s in self.sig_edit.toPlainText().splitlines() if s.strip()]
        if len(sig) < 1:
            QMessageBox.warning(self, "Signature vide",
                                "Ajoutez au moins un mot-clé pour identifier ce format à l'avenir.")
            return
        save_learned(
            name=name,
            signature=sig,
            carrier=self.carrier_edit.text().strip(),
            is_scanned=self.scanned_chk.isChecked(),
            model=get_model_name(),
            extraction_hints=self.hints_edit.toPlainText().strip(),
            example_rows=self._learned.get("example_rows") or [],
            parse_template=self._learned.get("parse_template") or {},
        )
        QMessageBox.information(self, "Format enregistré",
                                f"Le format « {name} » sera désormais reconnu automatiquement.")
        self.accept()


# ============================================================
# Convenience helpers for callers
# ============================================================
def ensure_api_key(parent) -> bool:
    """Prompt for API key if missing. Returns True if a key is now configured."""
    if has_api_key():
        return True
    QMessageBox.information(
        parent, "Configuration IA requise",
        "Pour utiliser l'extraction IA, configurez d'abord votre clé Google Gemini.",
    )
    dlg = GeminiConfigDialog(parent)
    return dlg.exec_() == QDialog.Accepted and has_api_key()
