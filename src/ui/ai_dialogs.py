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
from ..ai.gemini_client import (
    get_vision_api_key, set_vision_api_key,
    get_deepseek_api_key, set_deepseek_api_key,
    get_deepseek_model, set_deepseek_model,
    get_learning_providers, set_learning_providers,
)
from ..ai import llm_providers


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
    progress = pyqtSignal(str)

    def __init__(self, file_path: str, extra_feedback: str = "",
                 ocr_engine: str = "auto", previous_code: str = ""):
        super().__init__()
        self.file_path = file_path
        self.extra_feedback = extra_feedback
        self.ocr_engine = ocr_engine  # "auto" | "vision" | "local"
        self.previous_code = previous_code
        self._cancel = False

    def cancel(self):
        self._cancel = True
        self.requestInterruption()

    def run(self):
        from ..ai.ai_extractor import AICancelled
        try:
            data = learn_format_from_pdf(
                self.file_path,
                cancel_check=lambda: self._cancel,
                extra_feedback=self.extra_feedback,
                progress_cb=lambda msg: self.progress.emit(msg),
                ocr_engine=self.ocr_engine,
                previous_code=self.previous_code,
            )
            self.finished_ok.emit(data)
        except AICancelled:
            self.cancelled.emit()
        except Exception as e:
            import traceback
            self.failed.emit(f"{e}\n\n{traceback.format_exc()}")


# ════════════════════════════════════════════════════════════════════════
# Real-time learning progress dialog
# ════════════════════════════════════════════════════════════════════════
class LearnProgressDialog(QDialog):
    """Live progress + log panel for AI format learning.

    Owns the AILearnWorker and streams its progress messages into a
    scrollable log so the user can watch what each LLM provider is doing
    in real time. Closing the dialog cancels the worker.
    """

    finished_ok = pyqtSignal(dict)
    cancelled = pyqtSignal()
    failed = pyqtSignal(str)

    def __init__(self, worker: "AILearnWorker", parent=None, *,
                 title: str = "Apprentissage IA en cours…"):
        super().__init__(parent)
        self.worker = worker
        self.setWindowTitle(title)
        self.resize(720, 460)
        # Keep the dialog modal-ish but not freezing the main thread.
        self.setModal(True)

        lay = QVBoxLayout(self)
        self.lbl_status = QLabel("Démarrage…")
        self.lbl_status.setStyleSheet("font-weight: bold; color: #1A4076; padding: 4px;")
        lay.addWidget(self.lbl_status)

        self.txt_log = QPlainTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setStyleSheet(
            "font-family: Consolas, 'Courier New', monospace; font-size: 11px; "
            "background: #1A1A2E; color: #E8E8E8;"
        )
        lay.addWidget(self.txt_log, 1)

        bb = QHBoxLayout()
        bb.addStretch(1)
        self.btn_cancel = QPushButton("Annuler")
        self.btn_cancel.clicked.connect(self._on_cancel_clicked)
        bb.addWidget(self.btn_cancel)
        lay.addLayout(bb)

        self.worker.progress.connect(self._on_progress)
        self.worker.finished_ok.connect(self._on_done)
        self.worker.failed.connect(self._on_failed)
        self.worker.cancelled.connect(self._on_cancelled)

    def _on_progress(self, msg: str):
        from datetime import datetime
        ts = datetime.now().strftime("%H:%M:%S")
        self.lbl_status.setText(msg)
        self.txt_log.appendPlainText(f"[{ts}] {msg}")
        # Auto-scroll
        sb = self.txt_log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_done(self, data: dict):
        self.lbl_status.setText("Terminé.")
        self.txt_log.appendPlainText("[OK] Apprentissage terminé.")
        self.finished_ok.emit(data)
        self.accept()

    def _on_failed(self, err: str):
        self.lbl_status.setText("Échec.")
        self.txt_log.appendPlainText(f"[ERREUR] {err}")
        self.btn_cancel.setText("Fermer")
        self.failed.emit(err)

    def _on_cancelled(self):
        self.lbl_status.setText("Annulé.")
        self.txt_log.appendPlainText("[ANNULE] L'utilisateur a annule l'apprentissage.")
        self.cancelled.emit()
        self.reject()

    def _on_cancel_clicked(self):
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
        else:
            self.reject()

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
        super().closeEvent(event)


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
        self.setWindowTitle("Configuration IA — Modèles & OCR")
        self.resize(660, 720)

        lay = QVBoxLayout(self)

        info = QLabel(
            "<b>Clés API Google</b><br>"
            "• <b>Gemini</b> : structuration des manifestes (texte). Clé gratuite sur"
            "<a href='https://aistudio.google.com/apikey'>aistudio.google.com/apikey</a>.<br>"
            "• <b>Cloud Vision</b> : OCR rapide pour gros volumes (PDF scannés). Activez"
            "<i>Cloud Vision API</i> sur <a href='https://console.cloud.google.com/apis/library/vision.googleapis.com'>console.cloud.google.com</a>."
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

        # ── DeepSeek (second LLM provider) ───────────────────────────
        ds_group = QGroupBox("DeepSeek (deuxième fournisseur IA — texte uniquement)")
        ds_lay = QFormLayout(ds_group)
        self.ds_key_edit = QLineEdit()
        self.ds_key_edit.setEchoMode(QLineEdit.Password)
        self.ds_key_edit.setPlaceholderText("sk-…  (https://platform.deepseek.com)")
        ds_existing = get_deepseek_api_key()
        if ds_existing:
            self.ds_key_edit.setText(ds_existing)
        ds_lay.addRow("Clé DeepSeek :", self.ds_key_edit)

        self.ds_model_combo = QComboBox()
        self.ds_model_combo.addItems(["deepseek-chat", "deepseek-reasoner"])
        cur_ds = get_deepseek_model()
        if cur_ds in ("deepseek-chat", "deepseek-reasoner"):
            self.ds_model_combo.setCurrentText(cur_ds)
        ds_lay.addRow("Modèle DeepSeek :", self.ds_model_combo)
        lay.addWidget(ds_group)

        # ── Multi-model learning selection ────────────────────────────
        ml_group = QGroupBox(
            "Apprentissage multi-modèles (« Apprendre ce format à l'IA »)"
        )
        ml_lay = QVBoxLayout(ml_group)
        ml_lay.addWidget(QLabel(
            "Cochez les fournisseurs qui doivent être interrogés en parallèle "
            "lors de l'apprentissage d'un nouveau format. Le parser produit "
            "le plus grand nombre de lignes valides est conservé."
        ))
        self._provider_checks = {}
        enabled = set(get_learning_providers())
        for pid in llm_providers.all_provider_ids():
            prov = llm_providers.get_provider(pid)
            cb = QCheckBox(prov.display_name + (" (vision)" if prov.supports_vision else ""))
            cb.setChecked(pid in enabled)
            self._provider_checks[pid] = cb
            ml_lay.addWidget(cb)
        lay.addWidget(ml_group)

        hint = QLabel(
            "<i>Recommandé : <b>gemini-2.5-flash</b> + Cloud Vision activé sur le même projet GCP."
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
        set_deepseek_api_key(self.ds_key_edit.text().strip())
        set_deepseek_model(self.ds_model_combo.currentText())
        # Persist the multi-provider selection (always include at least gemini).
        chosen = [pid for pid, cb in self._provider_checks.items() if cb.isChecked()]
        if not chosen:
            chosen = ["gemini"]
        set_learning_providers(chosen)
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
            "<b>Formats reconnus automatiquement.</b>"
            "Lorsqu'un PDF correspond à l'une des signatures ci-dessous,"
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
        btn_del = QPushButton("Supprimer")
        btn_del.clicked.connect(self._delete_current)
        btns.addWidget(btn_del)
        btn_code = QPushButton("Éditer le parser…")
        btn_code.setToolTip("Voir / modifier le code Python du parser local appris.")
        btn_code.clicked.connect(self._edit_code)
        btns.addWidget(btn_code)
        btn_hints = QPushButton("Éditer les indications…")
        btn_hints.setToolTip("Texte injecté dans les prochaines extractions IA.")
        btn_hints.clicked.connect(self._edit_hints)
        btns.addWidget(btn_hints)
        btn_fb = QPushButton("Voir les feedbacks…")
        btn_fb.setToolTip("Liste des commentaires utilisateur attachés à ce format.")
        btn_fb.clicked.connect(self._view_feedback)
        btns.addWidget(btn_fb)
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
            scan_tag = "" if fmt.get("is_scanned") else ""
            label = f"{fmt.get('name', '?')}{scan_tag} — {fmt.get('carrier', '')} ({sig_count} mots-clés)"
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

    def _current_format(self) -> Optional[Dict]:
        idx = self.lst.currentRow()
        if not self._items or idx < 0 or idx >= len(self._items):
            return None
        return self._items[idx]

    def _edit_code(self):
        from ..ai.format_registry import update_format
        f = self._current_format()
        if not f:
            return
        tpl = f.get("parse_template") or {}
        code = tpl.get("parse_code") or ""
        dlg = _CodeEditorDialog(
            f"Code parser local — {f.get('name', '?')}",
            code, language="python", parent=self,
        )
        if dlg.exec_() == dlg.Accepted:
            new_code = dlg.text()
            tpl["parse_code"] = new_code
            update_format(f.get("name", ""), parse_template=tpl)
            QMessageBox.information(
                self, "Sauvegardé",
                "Le parser a été mis à jour. Re-testez sur un document.",
            )
            self._reload()

    def _edit_hints(self):
        from ..ai.format_registry import update_format
        f = self._current_format()
        if not f:
            return
        dlg = _CodeEditorDialog(
            f"Indications IA — {f.get('name', '?')}",
            f.get("extraction_hints", ""), language="text", parent=self,
        )
        if dlg.exec_() == dlg.Accepted:
            update_format(f.get("name", ""), extraction_hints=dlg.text())
            QMessageBox.information(self, "Sauvegardé", "Indications mises à jour.")
            self._reload()

    def _view_feedback(self):
        f = self._current_format()
        if not f:
            return
        fb = f.get("feedback") or []
        if not fb:
            QMessageBox.information(
                self, "Aucun feedback",
                f"Aucun feedback enregistré pour « {f.get('name', '?')} »."
            )
            return
        lines = []
        for i, e in enumerate(fb, 1):
            lines.append(
                f"[{i}] {e.get('timestamp', '')}  ({e.get('doc_name', '')})\n"
                f"    {e.get('text', '').strip()}\n"
            )
        dlg = _CodeEditorDialog(
            f"Feedbacks — {f.get('name', '?')} ({len(fb)} entrée(s))",
            "\n".join(lines), language="text", parent=self, read_only=True,
        )
        dlg.exec_()


class _CodeEditorDialog(QDialog):
    """Generic monospace text editor used by the dev tools."""

    def __init__(self, title: str, content: str, *,
                 language: str = "text", parent=None, read_only: bool = False):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(900, 620)
        lay = QVBoxLayout(self)
        self.edit = QPlainTextEdit()
        self.edit.setPlainText(content or "")
        self.edit.setReadOnly(read_only)
        font_css = "font-family: Consolas, 'Courier New', monospace; font-size: 12px;"
        self.edit.setStyleSheet(font_css)
        lay.addWidget(self.edit, 1)
        if read_only:
            bb = QDialogButtonBox(QDialogButtonBox.Close)
            bb.rejected.connect(self.reject)
            bb.accepted.connect(self.accept)
            # Close button maps to rejected by default; alias both to close
            bb.button(QDialogButtonBox.Close).clicked.connect(self.accept)
        else:
            bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
            bb.accepted.connect(self.accept)
            bb.rejected.connect(self.reject)
        lay.addWidget(bb)

    def text(self) -> str:
        return self.edit.toPlainText()


class DeveloperToolsDialog(QDialog):
    """Developer dashboard: prompts, formats, feedbacks at a glance."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Outils développeur — IA")
        self.resize(720, 520)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(
            "<b>Outils développeur.</b> Accès direct aux prompts internes, "
            "aux formats appris, aux feedbacks et aux logs."
        ))
        btn_prompts = QPushButton("Voir / éditer les prompts système…")
        btn_prompts.setToolTip(
            "Affiche les prompts d'extraction et d'apprentissage utilisés par "
            "l'IA. La modification est temporaire (jusqu'au prochain démarrage)."
        )
        btn_prompts.clicked.connect(self._edit_prompts)
        lay.addWidget(btn_prompts)

        btn_formats = QPushButton("Gérer les formats appris (avancé)…")
        btn_formats.clicked.connect(lambda: LearnedFormatsDialog(self).exec_())
        lay.addWidget(btn_formats)

        btn_logs = QPushButton("Ouvrir le dossier des logs IA…")
        btn_logs.clicked.connect(self._open_logs)
        lay.addWidget(btn_logs)

        btn_providers = QPushButton("Tester les fournisseurs IA configurés…")
        btn_providers.clicked.connect(self._test_providers)
        lay.addWidget(btn_providers)

        lay.addStretch(1)
        bb = QDialogButtonBox(QDialogButtonBox.Close)
        bb.rejected.connect(self.reject)
        bb.accepted.connect(self.accept)
        bb.button(QDialogButtonBox.Close).clicked.connect(self.accept)
        lay.addWidget(bb)

    def _edit_prompts(self):
        from ..ai import ai_extractor
        prompts = {
            "_EXTRACT_INSTRUCTIONS": getattr(ai_extractor, "_EXTRACT_INSTRUCTIONS", ""),
            "_LEARN_COMBINED_INSTRUCTIONS": getattr(ai_extractor, "_LEARN_COMBINED_INSTRUCTIONS", ""),
            "_FIX_INSTRUCTIONS": getattr(ai_extractor, "_FIX_INSTRUCTIONS", ""),
        }
        from PyQt5.QtWidgets import QInputDialog
        name, ok = QInputDialog.getItem(
            self, "Choisir un prompt",
            "Quel prompt afficher / modifier ?",
            list(prompts.keys()), 0, False,
        )
        if not ok:
            return
        dlg = _CodeEditorDialog(
            f"Prompt: {name}", prompts[name], language="text", parent=self,
        )
        if dlg.exec_() == dlg.Accepted:
            setattr(ai_extractor, name, dlg.text())
            QMessageBox.information(
                self, "Modifié (session)",
                "Le prompt est modifié pour la session en cours. "
                "Pour persister, modifiez src/ai/ai_extractor.py.",
            )

    def _open_logs(self):
        from ..ai.debug_log import _dir as _log_dir
        from PyQt5.QtGui import QDesktopServices
        from PyQt5.QtCore import QUrl
        try:
            d = _log_dir()
        except Exception:
            d = None
        if d:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(d)))
        else:
            QMessageBox.information(self, "Logs", "Dossier de logs introuvable.")

    def _test_providers(self):
        results = []
        for prov in [llm_providers.get_provider(p) for p in llm_providers.all_provider_ids()]:
            if not prov:
                continue
            tag = "OK " if prov.is_configured() else "(clé manquante)"
            results.append(f"  • {prov.display_name:18s} : {tag}")
        msg = "Fournisseurs IA disponibles :\n\n" + "\n".join(results)
        msg += "\n\nFournisseurs activés pour l'apprentissage :\n  "
        msg += ", ".join(get_learning_providers()) or "(aucun)"
        QMessageBox.information(self, "Fournisseurs IA", msg)


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
            f"<b>Exemples extraits :</b> {len(ex_rows)} ligne(s) mémorisée(s) —"
            "l'IA s'en servira comme référence sur les prochains documents de ce format."
            if ex_rows else
            "<i> Aucune ligne d'exemple n'a pu être extraite (extraction échouée ou format vide)."
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
            extra = f"— {n_sample} ligne(s) extraite(s) sur l'échantillon" if isinstance(n_sample, int) else ""
            tpl_label = QLabel(
                f"<b> Parser local généré :</b> {kind}, {n_hdr_pat} champ(s) d'en-tête{extra}."
                f"Les prochains documents de ce format seront extraits LOCALEMENT (sans IA, instantané)."
            )
            tpl_label.setStyleSheet("color: #2a6f2a; font-size: 11px;")
        else:
            tpl_label = QLabel(
                "<i> Aucun parser local généré — les prochaines extractions de ce format"
                "passeront par l'IA (lent). Vous pouvez ré-apprendre pour réessayer.</i>"
            )
            tpl_label.setStyleSheet("color: #a05a00; font-size: 11px;")
        tpl_label.setWordWrap(True)
        lay.addWidget(tpl_label)

        # ── Multi-LLM ensemble: show the score table ─────────────────
        learners = self._learned.get("_learners") or []
        if learners:
            lines = []
            for l in learners:
                tag = "  <-- WINNER" if l.get("won") else ""
                ok = "OK" if l.get("ok") else "FAIL"
                lines.append(
                    f"{l.get('provider', '?'):10s} ({l.get('model', '?')}): "
                    f"{ok}, {l.get('n_rows', 0)} lignes{tag}"
                )
            ens_label = QLabel(
                "<b>Apprentissage multi-modèles :</b><br>"
                "<pre style='background:#F4F7FB; padding:6px;'>"
                + "\n".join(lines)
                + "</pre>"
            )
            ens_label.setWordWrap(True)
            lay.addWidget(ens_label)

        bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        bb.button(QDialogButtonBox.Save).setText("Enregistrer le format")
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
