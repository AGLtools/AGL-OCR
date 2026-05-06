"""Interactive Format Trainer.

Lets the user teach AGL OCR a new manifest format by clicking on the document:
  1. Click the LABEL word (e.g. "Loading")
  2. Click the VALUE word (e.g. "ANTWERPEN")
  3. The app infers the spatial relationship (right/below/above + tolerances)
  4. Repeat for each field, then Save → writes config/parsers/<slug>.yaml

Once saved, the format is auto-detected on next document open.

Works on PDFs with embedded text (uses pdfplumber). For scanned documents
the user must use the standard template-builder workflow.
"""
from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional

import yaml
from PyQt5.QtCore import Qt, QRectF
from PyQt5.QtGui import QBrush, QColor, QPainter, QPen, QPixmap
from PyQt5.QtWidgets import (
    QApplication, QDialog, QGraphicsPixmapItem, QGraphicsRectItem,
    QGraphicsScene, QGraphicsView, QHBoxLayout, QLabel, QLineEdit,
    QListWidget, QListWidgetItem, QMessageBox, QPushButton, QVBoxLayout,
    QWidget, QGroupBox, QFormLayout,
)

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

from pdf2image import convert_from_path

from ..paths import poppler_bin
from ..config import CONFIG_DIR


# Standard fields offered to the user (key, human label, hint)
TRAINER_FIELDS: List[Tuple[str, str, str]] = [
    ("vessel",            "Vessel name",        "e.g. 'CMA CGM LEBU'"),
    ("voyage",            "Voyage number",      "e.g. '0BAN6N1MA'"),
    ("date_of_arrival",   "Date of arrival",    "e.g. '16-JAN-26'"),
    ("port_of_loading",   "Port of Loading",    "e.g. 'ANTWERPEN'"),
    ("port_of_discharge", "Port of Discharge",  "e.g. 'ABIDJAN'"),
    ("place_of_delivery", "Place of delivery",  "e.g. 'BOBO DIOULASSO'"),
    ("place_of_acceptance", "Place of acceptance", "e.g. 'MARSEILLE'"),
]


# ============================================================
# Clickable word rectangle on the canvas
# ============================================================
class WordRect(QGraphicsRectItem):
    def __init__(self, word: dict, rect: QRectF, on_click):
        super().__init__(rect)
        self.word = word
        self.on_click = on_click
        self._normal_brush = QBrush(QColor(0, 150, 255, 35))
        self._normal_pen = QPen(QColor(0, 150, 255, 130), 0.4)
        self._hover_brush = QBrush(QColor(255, 200, 0, 140))
        self._selected_brush = QBrush(QColor(0, 200, 0, 160))
        self.setPen(self._normal_pen)
        self.setBrush(self._normal_brush)
        self.setAcceptHoverEvents(True)
        self._selected = False

    def mark_selected(self, selected: bool):
        self._selected = selected
        self.setBrush(self._selected_brush if selected else self._normal_brush)

    def hoverEnterEvent(self, ev):
        if not self._selected:
            self.setBrush(self._hover_brush)

    def hoverLeaveEvent(self, ev):
        if not self._selected:
            self.setBrush(self._normal_brush)

    def mousePressEvent(self, ev):
        self.on_click(self)


# ============================================================
# Trainer dialog
# ============================================================
class FormatTrainerDialog(QDialog):
    """Walk the user through teaching a new manifest format."""

    def __init__(self, pdf_path: Path, parent=None):
        super().__init__(parent)
        if not HAS_PDFPLUMBER:
            QMessageBox.critical(
                self, "Dépendance manquante",
                "pdfplumber est requis.\n\nExécutez : pip install pdfplumber"
            )
            self.reject()
            return

        self.pdf_path = Path(pdf_path)
        self.setWindowTitle("Apprendre un nouveau format de manifeste")
        self.resize(1500, 900)

        # State
        self.rules: dict = {}                  # field_key -> rule dict
        self.current_step: int = 0
        self.current_phase: str = "label"      # "label" or "value"
        self.label_word: Optional[dict] = None
        self.label_rect: Optional[WordRect] = None
        self.word_rects: List[WordRect] = []
        self.scale_x = 1.0
        self.scale_y = 1.0

        self._build_ui()
        self._load_page()
        self._refresh_instruction()

    # ----------------------------------------------------------
    # UI
    # ----------------------------------------------------------
    def _build_ui(self):
        outer = QHBoxLayout(self)

        # ---- Left: clickable canvas ----
        self.scene = QGraphicsScene()
        self.view = QGraphicsView(self.scene)
        self.view.setRenderHint(QPainter.Antialiasing)
        self.view.setRenderHint(QPainter.SmoothPixmapTransform)
        self.view.setDragMode(QGraphicsView.ScrollHandDrag)
        outer.addWidget(self.view, 3)

        # ---- Right: control panel ----
        right = QWidget()
        right.setMaximumWidth(420)
        rlay = QVBoxLayout(right)
        outer.addWidget(right, 1)

        # Header / format meta
        meta = QGroupBox("Nouveau format")
        flay = QFormLayout(meta)
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("ex : MSC East Africa")
        self.keyword_edit = QLineEdit()
        self.keyword_edit.setPlaceholderText("ex : MSC (doit apparaître sur chaque page)")
        flay.addRow("Nom du format :", self.name_edit)
        flay.addRow("Mot-clé de détection :", self.keyword_edit)
        rlay.addWidget(meta)

        # Field list
        rlay.addWidget(QLabel("<b>Champs à apprendre</b>"
                              "<i>(cliquez une ligne pour y revenir)</i>"))
        self.field_list = QListWidget()
        for key, label, hint in TRAINER_FIELDS:
            it = QListWidgetItem(label)
            it.setData(Qt.UserRole, key)
            it.setToolTip(hint)
            self.field_list.addItem(it)
        self.field_list.itemClicked.connect(self._on_field_row_clicked)
        rlay.addWidget(self.field_list, 1)

        # Instruction box
        self.instruction = QLabel()
        self.instruction.setStyleSheet(
            "background:#fffadd;border:1px solid #e0c060;"
            "padding:10px;font-size:13px;border-radius:4px;"
        )
        self.instruction.setWordWrap(True)
        self.instruction.setMinimumHeight(110)
        rlay.addWidget(self.instruction)

        # Action buttons
        btn_skip = QPushButton("Ignorer ce champ")
        btn_skip.clicked.connect(self._skip_current)
        btn_redo = QPushButton("Refaire le champ courant")
        btn_redo.clicked.connect(self._redo_current)
        rlay.addWidget(btn_skip)
        rlay.addWidget(btn_redo)

        save_row = QHBoxLayout()
        btn_save = QPushButton("Enregistrer le format")
        btn_save.setStyleSheet(
            "font-weight:bold;background:#cce5ff;padding:8px;"
        )
        btn_save.clicked.connect(self._save)
        btn_cancel = QPushButton("Annuler")
        btn_cancel.clicked.connect(self.reject)
        save_row.addWidget(btn_save)
        save_row.addWidget(btn_cancel)
        rlay.addLayout(save_row)

    def _load_page(self):
        """Render page 1 and overlay clickable word rectangles."""
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            kwargs = {"first_page": 1, "last_page": 1, "dpi": 150}
            try:
                p = poppler_bin()
                if p:
                    kwargs["poppler_path"] = p
            except Exception:
                pass
            images = convert_from_path(str(self.pdf_path), **kwargs)
            img = images[0]
            tmp = Path(tempfile.gettempdir()) / "agl_trainer_page.png"
            img.save(tmp, "PNG")
            pix = QPixmap(str(tmp))
            self.scene.addItem(QGraphicsPixmapItem(pix))

            with pdfplumber.open(self.pdf_path) as pdf:
                pg = pdf.pages[0]
                self.scale_x = pix.width() / pg.width
                self.scale_y = pix.height() / pg.height
                words = pg.extract_words(x_tolerance=3, y_tolerance=3)
                for w in words:
                    x = w["x0"] * self.scale_x
                    y = w["top"] * self.scale_y
                    wd = (w["x1"] - w["x0"]) * self.scale_x
                    ht = (w["bottom"] - w["top"]) * self.scale_y
                    rect = QRectF(x, y, wd, ht)
                    item = WordRect(w, rect, self._on_word_clicked)
                    self.scene.addItem(item)
                    self.word_rects.append(item)

            self.view.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)
        finally:
            QApplication.restoreOverrideCursor()

    # ----------------------------------------------------------
    # Step driver
    # ----------------------------------------------------------
    def _refresh_instruction(self):
        if self.current_step >= len(TRAINER_FIELDS):
            self.instruction.setText(
                "<b> Tous les champs sont terminés.</b><br>"
                "Renseignez le nom du format + le mot-clé de détection ci-dessus,<br>"
                "puis cliquez sur <b>Enregistrer le format</b>."
            )
            return
        key, label, hint = TRAINER_FIELDS[self.current_step]
        self.field_list.setCurrentRow(self.current_step)
        if self.current_phase == "label":
            self.instruction.setText(
                f"<b>Étape {self.current_step + 1}/{len(TRAINER_FIELDS)} :"
                f"{label}</b><br>"
                f"Cliquez sur le <b>LIBELLÉ</b> dans le document<br>"
                f"<i>(le texte imprimé identifiant le champ,<br>"
                f"ex : 'Loading' pour Port de chargement)</i><br>"
                f"<small>{hint}</small><br>"
                f"<i>Ou cliquez sur « Ignorer » si ce champ n'existe pas dans ce format.</i>"
            )
        else:
            self.instruction.setText(
                f"<b>Étape {self.current_step + 1}/{len(TRAINER_FIELDS)} :"
                f"{label}</b><br>"
                f"Libellé sélectionné : <b>'{self.label_word['text']}'</b><br>"
                f"Cliquez maintenant sur le mot <b>VALEUR</b><br>"
                f"<i>{hint}</i>"
            )

    def _on_word_clicked(self, item: WordRect):
        if self.current_step >= len(TRAINER_FIELDS):
            QMessageBox.information(
                self, "Terminé",
                "Tous les champs sont cartographiés. Cliquez sur « Enregistrer le format » pour terminer."
            )
            return

        if self.current_phase == "label":
            # Clear previous label highlight if any
            if self.label_rect:
                self.label_rect.mark_selected(False)
            self.label_word = item.word
            self.label_rect = item
            item.mark_selected(True)
            self.current_phase = "value"
            self._refresh_instruction()
        else:
            # Compute spatial rule from label → value
            key, _, _ = TRAINER_FIELDS[self.current_step]
            rule = self._compute_rule(self.label_word, item.word)
            self.rules[key] = rule
            # Mark item in field list
            item_text = (
                f"{TRAINER_FIELDS[self.current_step][1]}"
                f"→ '{self.label_word['text']}' [{rule['direction']}]"
                f"'{item.word['text']}'"
            )
            self.field_list.item(self.current_step).setText(item_text)
            # Reset highlights
            if self.label_rect:
                self.label_rect.mark_selected(False)
            self.label_word = None
            self.label_rect = None
            self.current_phase = "label"
            self.current_step += 1
            self._refresh_instruction()

    def _on_field_row_clicked(self, item: QListWidgetItem):
        idx = self.field_list.row(item)
        self._jump_to(idx)

    def _jump_to(self, idx: int):
        if idx < 0 or idx >= len(TRAINER_FIELDS):
            return
        if self.label_rect:
            self.label_rect.mark_selected(False)
        self.label_word = None
        self.label_rect = None
        self.current_step = idx
        self.current_phase = "label"
        self._refresh_instruction()

    def _skip_current(self):
        if self.current_step >= len(TRAINER_FIELDS):
            return
        key, label, _ = TRAINER_FIELDS[self.current_step]
        self.rules.pop(key, None)
        self.field_list.item(self.current_step).setText(f"– {label} (ignoré)")
        if self.label_rect:
            self.label_rect.mark_selected(False)
        self.label_word = None
        self.label_rect = None
        self.current_phase = "label"
        self.current_step += 1
        self._refresh_instruction()

    def _redo_current(self):
        if self.current_step >= len(TRAINER_FIELDS):
            self.current_step = len(TRAINER_FIELDS) - 1
        if self.label_rect:
            self.label_rect.mark_selected(False)
        self.label_word = None
        self.label_rect = None
        self.current_phase = "label"
        key, label, _ = TRAINER_FIELDS[self.current_step]
        self.rules.pop(key, None)
        self.field_list.item(self.current_step).setText(label)
        self._refresh_instruction()

    # ----------------------------------------------------------
    # Spatial rule inference
    # ----------------------------------------------------------
    @staticmethod
    def _compute_rule(label_w: dict, value_w: dict) -> dict:
        """Infer direction + tolerances from label/value spatial layout."""
        lx0, ly0, lx1, ly1 = (
            float(label_w["x0"]), float(label_w["top"]),
            float(label_w["x1"]), float(label_w["bottom"]),
        )
        vx0, vy0, vx1, vy1 = (
            float(value_w["x0"]), float(value_w["top"]),
            float(value_w["x1"]), float(value_w["bottom"]),
        )
        lcy = (ly0 + ly1) / 2.0
        vcy = (vy0 + vy1) / 2.0
        l_height = max(ly1 - ly0, 8.0)

        same_line = abs(vcy - lcy) < l_height
        if same_line and vx0 >= lx0:
            # Value is to the right of label
            return {
                "label_anchor": label_w["text"],
                "direction": "right",
                "max_distance": int((vx1 - lx1) * 1.5) + 80,
                "y_tolerance": max(5, int(l_height * 0.6)),
                "max_words": 4,
            }
        if vy0 > ly1:
            # Value is below label
            return {
                "label_anchor": label_w["text"],
                "direction": "below",
                "max_distance": int((vy0 - ly1) * 1.8) + 20,
                "x_tolerance": int(abs((vx0 + vx1) / 2 - (lx0 + lx1) / 2)) + 40,
                "max_words": 4,
            }
        # Value is above label
        return {
            "label_anchor": label_w["text"],
            "direction": "above",
            "max_distance": int((ly0 - vy1) * 1.8) + 20,
            "x_tolerance": int(abs((vx0 + vx1) / 2 - (lx0 + lx1) / 2)) + 40,
            "max_words": 4,
        }

    # ----------------------------------------------------------
    # Save
    # ----------------------------------------------------------
    def _save(self):
        name = self.name_edit.text().strip()
        kw = self.keyword_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Champ manquant", "Veuillez saisir un nom de format.")
            self.name_edit.setFocus()
            return
        if not kw:
            QMessageBox.warning(
                self, "Champ manquant",
                "Veuillez saisir un mot-clé de détection (un mot qui apparaît toujours\n"
                "sur chaque page des manifestes de cet armateur, ex : 'MAERSK')."
            )
            self.keyword_edit.setFocus()
            return
        if not self.rules:
            QMessageBox.warning(
                self, "Aucun champ cartographié",
                "Cartographiez au moins un champ avant d'enregistrer."
            )
            return

        slug = re.sub(r"[^a-z0-9_]+", "_", name.lower()).strip("_")
        if not slug:
            slug = "custom_format"

        out_dir = CONFIG_DIR / "parsers"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{slug}.yaml"

        if out_path.exists():
            ret = QMessageBox.question(
                self, "Écraser ?",
                f"Un fichier de format « {slug}.yaml » existe déjà.\nÉcraser ?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
            )
            if ret != QMessageBox.Yes:
                return

        config = {
            "name": name,
            "shipowner": name,
            "detection_keyword": kw,
            "page_header_fields": self.rules,
        }
        out_path.write_text(
            yaml.safe_dump(config, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )

        QMessageBox.information(
            self, "Format enregistré",
            f"<b>Format « {name} »</b> enregistré avec succès.<br><br>"
            f"Fichier : <code>{out_path}</code><br><br>"
            f"Champs cartographiés : <b>{len(self.rules)}</b><br>"
            f"Mot-clé de détection : <b>'{kw}'</b><br><br>"
            f"La prochaine fois que vous ouvrirez un document contenant '{kw}',<br>"
            f"ce format sera appliqué automatiquement."
        )
        self.accept()
