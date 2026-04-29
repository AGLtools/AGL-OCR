"""Manifest review dialog: editable table of all rows extracted by ManifestParser.

Shows every row as one line of an editable QTableWidget. Edits are persisted
immediately via CorrectionStore. Includes a filter to focus on rows with
empty key fields (the typical correction workflow).

All UI text is in French (per AGL mandate).
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional

from PyQt5.QtCore import Qt, QSignalBlocker
from PyQt5.QtGui import QColor, QBrush, QFont
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QCheckBox, QLineEdit, QFileDialog, QMessageBox,
    QHeaderView, QComboBox,
)

from ..corrections import CorrectionStore
from ..exporter import ExcelExporter
from ..config import EXPORTS_DIR
from ..midas_mapper import MIDAS_COLUMNS, map_rows_to_midas
from ..ai import validate_rows, has_api_key
from .ai_dialogs import AIFixWorker, ensure_api_key


# Read-only metadata columns (never editable)
READONLY_FIELDS = {"source_file", "page", "_user_edited"}

# Key fields used by the "incomplete rows" filter
KEY_FIELDS_FOR_FILTER = (
    "bl_number", "container_number", "shipper", "consignee",
    "port_of_loading", "port_of_discharge", "weight",
)


class ManifestReviewDialog(QDialog):
    """Editable review dialog for ManifestParser output."""

    def __init__(
        self,
        rows: List[Dict[str, Any]],
        source_path: Path,
        parent=None,
    ):
        super().__init__(parent)
        self.source_path = source_path
        self.store = CorrectionStore(source_path)
        # If we already have stored corrections for this doc, prefer them
        # over the freshly-parsed rows (user has edited them previously).
        if self.store.has_manifest_rows():
            stored = self.store.get_manifest_rows()
            # If the stored count matches the parsed count, use stored.
            # Otherwise rebuild from parsed (schema/version drift).
            if len(stored) == len(rows):
                self.rows = stored
            else:
                self.rows = list(rows)
                self.store.save_manifest_rows(self.rows)
        else:
            self.rows = list(rows)
            self.store.save_manifest_rows(self.rows)

        self.setWindowTitle(
            f"Revue du manifeste — {len(self.rows)} conteneurs ({source_path.name})"
        )
        self.resize(1400, 750)

        self._midas_mode = False  # False = vue brute éditable, True = aperçu MIDAS éditable
        self._midas_rows: Optional[List[Dict[str, Any]]] = None  # MIDAS rows with user edits

        self._build_ui()
        self._populate_table()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self):
        lay = QVBoxLayout(self)

        # Header info
        info = QLabel(
            f"<b>{len(self.rows)}</b> conteneurs extraits de <b>{self.source_path.name}</b>. "
            f"Toutes les modifications sont <b>enregistrées automatiquement</b>."
        )
        info.setStyleSheet("padding: 6px;")
        lay.addWidget(info)

        # Filter bar
        bar = QHBoxLayout()
        self.search = QLineEdit()
        self.search.setPlaceholderText("🔍 Rechercher (BL, conteneur, expéditeur…)")
        self.search.textChanged.connect(self._refilter)
        bar.addWidget(self.search, 3)

        self.chk_incomplete = QCheckBox("Afficher seulement les lignes incomplètes")
        self.chk_incomplete.toggled.connect(self._refilter)
        bar.addWidget(self.chk_incomplete, 1)

        self.chk_edited = QCheckBox("Lignes modifiées seulement")
        self.chk_edited.toggled.connect(self._refilter)
        bar.addWidget(self.chk_edited, 1)

        lay.addLayout(bar)

        # Status label
        self.lbl_status = QLabel()
        self.lbl_status.setStyleSheet("color: #555; padding: 2px 6px;")
        lay.addWidget(self.lbl_status)

        # Main table
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed)
        self.table.itemChanged.connect(self._on_item_changed)
        lay.addWidget(self.table, 1)

        # Bottom bar
        btns = QHBoxLayout()
        self.btn_reset_row = QPushButton("↩ Réinitialiser la ligne sélectionnée")
        self.btn_reset_row.clicked.connect(self._reset_selected_row)
        btns.addWidget(self.btn_reset_row)

        self.btn_toggle_midas = QPushButton("📊 Aperçu MIDAS")
        self.btn_toggle_midas.setCheckable(True)
        self.btn_toggle_midas.setToolTip(
            "Bascule entre la vue brute (éditable) et l'aperçu MIDAS 43 colonnes (lecture seule).\n"
            "L'aperçu MIDAS montre exactement ce qui sera exporté."
        )
        self.btn_toggle_midas.toggled.connect(self._toggle_midas_view)
        btns.addWidget(self.btn_toggle_midas)

        # AI quality checks
        self.btn_validate_ai = QPushButton("🔍 Vérifier la qualité")
        self.btn_validate_ai.setToolTip(
            "Détecte les lignes incomplètes ou suspectes (n° BL/conteneur invalide,\n"
            "poids non numérique, champs obligatoires manquants)."
        )
        self.btn_validate_ai.clicked.connect(self._run_quality_check)
        btns.addWidget(self.btn_validate_ai)

        self.btn_ai_fix = QPushButton("✨ Corriger les lignes problématiques avec l'IA")
        self.btn_ai_fix.setStyleSheet("background: #e7d4ff; color: #4a148c;")
        self.btn_ai_fix.setToolTip(
            "Envoie chaque ligne problématique à Gemini avec le contexte du document\n"
            "pour correction automatique. Nécessite une clé API."
        )
        self.btn_ai_fix.clicked.connect(self._ai_fix_problems)
        btns.addWidget(self.btn_ai_fix)

        btns.addStretch(1)

        self.btn_export = QPushButton("⬇ Export brut")
        self.btn_export.setToolTip("Exporte les colonnes brutes (debug / inspection)")
        self.btn_export.clicked.connect(self._export_excel)
        btns.addWidget(self.btn_export)

        self.btn_export_midas = QPushButton("📊 Export MIDAS")
        self.btn_export_midas.setStyleSheet("font-weight: bold; background: #d4edda;")
        self.btn_export_midas.setToolTip("Exporte au format MIDAS 43 colonnes pour saisie d'intégration")
        self.btn_export_midas.clicked.connect(self._export_midas)
        btns.addWidget(self.btn_export_midas)

        self.btn_close = QPushButton("Fermer")
        self.btn_close.clicked.connect(self.accept)
        btns.addWidget(self.btn_close)
        lay.addLayout(btns)

    # ------------------------------------------------------------------
    # Population
    # ------------------------------------------------------------------
    def _columns(self) -> List[str]:
        if not self.rows:
            return []
        # Build column order from first row, but exclude "_user_edited"
        cols = [k for k in self.rows[0].keys() if k != "_user_edited"]
        return cols

    def _populate_table(self):
        if self._midas_mode:
            self._populate_midas()
            return
        cols = self._columns()
        self.table.clear()
        self.table.setColumnCount(len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        self.table.setRowCount(len(self.rows))

        with QSignalBlocker(self.table):
            for r, row in enumerate(self.rows):
                for c, key in enumerate(cols):
                    val = row.get(key, "")
                    item = QTableWidgetItem(str(val) if val is not None else "")
                    if key in READONLY_FIELDS:
                        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                        item.setForeground(QBrush(QColor("#888")))
                    if row.get("_user_edited"):
                        item.setBackground(QBrush(QColor("#FFF8C5")))
                    self.table.setItem(r, c, item)

        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        for c in range(len(cols)):
            self.table.resizeColumnToContents(c)
            # cap very wide columns
            if self.table.columnWidth(c) > 280:
                self.table.setColumnWidth(c, 280)

        self._refresh_status()

    def _populate_midas(self):
        """Render rows in MIDAS 43-column layout (editable).

        Empty mandatory cells are highlighted in red so the user can
        instantly spot extraction failures. Edits update self._midas_rows."""
        # (Re)compute if first time or rows changed
        if self._midas_rows is None:
            self._midas_rows = map_rows_to_midas(self.rows)
        midas_rows = self._midas_rows
        cols = MIDAS_COLUMNS
        self.table.clear()
        self.table.setColumnCount(len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        self.table.setRowCount(len(midas_rows))

        # Colonnes laissées volontairement vides (saisie équipe d'intégration)
        intentionally_empty = {
            "Numéro escale", "Index", "Range",
            "Code transitaire", "chargeur.code", "Code marchandise",
            "Manutentionaire", "Port transbo1", "Port transbo2",
        }
        red_brush = QBrush(QColor("#FFD6D6"))
        gray_brush = QBrush(QColor("#EEEEEE"))

        with QSignalBlocker(self.table):
            for r, row in enumerate(midas_rows):
                for c, key in enumerate(cols):
                    val = row.get(key, "")
                    item = QTableWidgetItem(str(val) if val is not None else "")
                    if not str(val).strip():
                        if key in intentionally_empty:
                            item.setBackground(gray_brush)
                            item.setToolTip("Saisie équipe d'intégration")
                        else:
                            item.setBackground(red_brush)
                            item.setToolTip("⚠ Champ vide — extraction à vérifier")
                    self.table.setItem(r, c, item)

        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        for c in range(len(cols)):
            self.table.resizeColumnToContents(c)
            if self.table.columnWidth(c) > 240:
                self.table.setColumnWidth(c, 240)

        self._refresh_status()

    def _toggle_midas_view(self, checked: bool):
        self._midas_mode = checked
        self.btn_toggle_midas.setText("✏ Vue brute (édition)" if checked else "📊 Aperçu MIDAS")
        # Reset computed MIDAS rows when leaving MIDAS mode so next entry
        # picks up any brute-mode corrections.
        if not checked:
            self._midas_rows = None
        self._populate_table()
        if checked:
            self.lbl_status.setText(
                f"Aperçu MIDAS — {len(self.rows)} lignes × {len(MIDAS_COLUMNS)} colonnes "
                f"· 🟥 vide à corriger · ⬜ saisie équipe"
            )

    def _refresh_status(self):
        edited = sum(1 for r in self.rows if r.get("_user_edited"))
        incomplete = sum(1 for r in self.rows if self._is_incomplete(r))
        self.lbl_status.setText(
            f"{len(self.rows)} lignes · "
            f"{edited} modifiées · "
            f"{incomplete} incomplètes"
        )

    @staticmethod
    def _is_incomplete(row: Dict[str, Any]) -> bool:
        return any(not (row.get(k) or "").strip() for k in KEY_FIELDS_FOR_FILTER if k in row)

    # ------------------------------------------------------------------
    # Edit handling
    # ------------------------------------------------------------------
    def _on_item_changed(self, item: QTableWidgetItem):
        r = item.row()
        c = item.column()
        new_val = item.text()

        if self._midas_mode:
            # Edit in MIDAS view — update _midas_rows only (no CorrectionStore)
            if self._midas_rows is None or r >= len(self._midas_rows):
                return
            key = MIDAS_COLUMNS[c] if c < len(MIDAS_COLUMNS) else None
            if not key:
                return
            if str(self._midas_rows[r].get(key, "")) == new_val:
                return
            self._midas_rows[r][key] = new_val
            # Remove red highlight once user fills a cell
            with QSignalBlocker(self.table):
                item.setBackground(QBrush(QColor("#FFF8C5")))
                item.setToolTip("✏ Modifié manuellement")
            self._refresh_status()
            return

        cols = self._columns()
        if c >= len(cols):
            return
        key = cols[c]
        if key in READONLY_FIELDS:
            return
        new_val = item.text()
        old_val = self.rows[r].get(key, "")
        if str(old_val) == new_val:
            return
        # Persist
        self.rows[r][key] = new_val
        self.store.update_manifest_row(r, {key: new_val})
        # Visual flag
        with QSignalBlocker(self.table):
            for cc in range(self.table.columnCount()):
                it = self.table.item(r, cc)
                if it:
                    it.setBackground(QBrush(QColor("#FFF8C5")))
        self._refresh_status()

    # ------------------------------------------------------------------
    # Quality validation + AI auto-fix
    # ------------------------------------------------------------------
    def _run_quality_check(self):
        """Highlight rows with detected issues. Updates row tooltip + status bar."""
        if self._midas_mode:
            self.btn_toggle_midas.setChecked(False)  # switch back to raw view
        issues = validate_rows(self.rows)
        # Reset all backgrounds in raw view
        if not self._midas_mode:
            self._populate_table()
        # Apply red tint + tooltip on problematic rows
        red = QBrush(QColor("#FFD6D6"))
        for ridx, iss in issues.items():
            if ridx >= self.table.rowCount():
                continue
            tip = " · ".join(iss)
            for c in range(self.table.columnCount()):
                it = self.table.item(ridx, c)
                if it:
                    if not self.rows[ridx].get("_user_edited"):
                        it.setBackground(red)
                    it.setToolTip(tip)
        self.lbl_status.setText(
            f"{len(self.rows)} lignes · {len(issues)} avec problèmes détectés"
            + (" · cliquez « Corriger avec IA »" if issues else " ✓")
        )
        if not issues:
            QMessageBox.information(self, "Qualité OK", "Aucun problème détecté.")

    def _ai_fix_problems(self):
        """Send each problematic row to Gemini for correction."""
        if not ensure_api_key(self):
            return
        issues_map = validate_rows(self.rows)
        if not issues_map:
            QMessageBox.information(self, "Rien à corriger",
                                    "Aucun problème détecté. Lancez d'abord « Vérifier la qualité ».")
            return
        if QMessageBox.question(
            self, "Correction IA",
            f"<b>{len(issues_map)} ligne(s)</b> seront envoyées à Gemini pour correction.<br><br>"
            "Cela peut prendre quelques secondes par ligne. Continuer ?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes,
        ) != QMessageBox.Yes:
            return

        # Build a context string from the source PDF (text-only, best effort)
        context = self._build_doc_context()

        # Process sequentially via a small queue of workers
        self._fix_queue = list(issues_map.items())
        self._fix_total = len(self._fix_queue)
        self._fix_done = 0
        self._fix_context = context
        self.btn_ai_fix.setEnabled(False)
        self.lbl_status.setText(f"Correction IA en cours… 0 / {self._fix_total}")
        self._launch_next_fix()

    def _launch_next_fix(self):
        if not self._fix_queue:
            self.btn_ai_fix.setEnabled(True)
            self.lbl_status.setText(
                f"Correction IA terminée — {self._fix_done} / {self._fix_total} ligne(s) traitée(s)."
            )
            self._populate_table()
            return
        ridx, iss = self._fix_queue.pop(0)
        self._cur_fix_worker = AIFixWorker(ridx, dict(self.rows[ridx]), iss, self._fix_context)
        self._cur_fix_worker.finished_ok.connect(self._on_fix_done)
        self._cur_fix_worker.failed.connect(self._on_fix_failed)
        self._cur_fix_worker.start()

    def _on_fix_done(self, ridx: int, fixed: dict):
        # Apply only changed values, mark row as edited, persist
        original = self.rows[ridx]
        changed = False
        for k, v in fixed.items():
            if k in ("source_file", "page", "_user_edited"):
                continue
            if str(v).strip() and str(original.get(k, "")).strip() != str(v).strip():
                original[k] = str(v).strip()
                changed = True
        if changed:
            original["_user_edited"] = True
            self.store.save_manifest_rows(self.rows)
        self._fix_done += 1
        self.lbl_status.setText(
            f"Correction IA en cours… {self._fix_done} / {self._fix_total}"
        )
        self._launch_next_fix()

    def _on_fix_failed(self, ridx: int, err: str):
        self._fix_done += 1
        # Keep going — don't abort the whole batch on a single failure
        self._launch_next_fix()

    def _build_doc_context(self) -> str:
        """Best-effort: extract embedded text from the source PDF (max ~16k chars)."""
        try:
            import pdfplumber
            chunks = []
            total = 0
            with pdfplumber.open(str(self.source_path)) as pdf:
                for p in pdf.pages:
                    t = p.extract_text() or ""
                    if t:
                        chunks.append(t)
                        total += len(t)
                        if total > 16000:
                            break
            return "\n\n".join(chunks)[:16000]
        except Exception:
            return ""

    def _reset_selected_row(self):
        rows = sorted({i.row() for i in self.table.selectedIndexes()})
        if not rows:
            return
        if QMessageBox.question(
            self, "Réinitialiser",
            f"Réinitialiser {len(rows)} ligne(s) sélectionnée(s) ?\n"
            f"(Le drapeau « modifiée » sera retiré, les valeurs gardées en l'état)",
            QMessageBox.Yes | QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        for r in rows:
            self.rows[r].pop("_user_edited", None)
            self.store.data.manifest_rows[r].pop("_user_edited", None)
        self.store._flush()
        self._populate_table()

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------
    def _refilter(self):
        text = self.search.text().lower().strip()
        only_incomplete = self.chk_incomplete.isChecked()
        only_edited = self.chk_edited.isChecked()
        for r, row in enumerate(self.rows):
            visible = True
            if text:
                blob = " ".join(str(v) for v in row.values()).lower()
                if text not in blob:
                    visible = False
            if visible and only_incomplete and not self._is_incomplete(row):
                visible = False
            if visible and only_edited and not row.get("_user_edited"):
                visible = False
            self.table.setRowHidden(r, not visible)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def _export_excel(self):
        if not self.rows:
            QMessageBox.information(self, "Rien à exporter", "Aucune ligne disponible.")
            return
        default = str(EXPORTS_DIR / f"{self.source_path.stem}_export.xlsx")
        path, _ = QFileDialog.getSaveFileName(
            self, "Exporter vers Excel", default, "Fichiers Excel (*.xlsx)"
        )
        if not path:
            return
        # Strip internal flags before export
        clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in self.rows]
        out = ExcelExporter(output_path=Path(path)).export(clean)
        QMessageBox.information(
            self, "Export terminé",
            f"{len(clean)} lignes exportées vers :\n{out}"
        )

    def _export_midas(self):
        """Export at MIDAS 43-column format, using edited MIDAS rows if available."""
        if not self.rows:
            QMessageBox.information(self, "Rien à exporter", "Aucune ligne disponible.")
            return
        default = str(EXPORTS_DIR / f"MIDAS_{self.source_path.stem}.xlsx")
        path, _ = QFileDialog.getSaveFileName(
            self, "Exporter au format MIDAS", default, "Fichiers Excel (*.xlsx)"
        )
        if not path:
            return
        try:
            exp = ExcelExporter(output_path=Path(path))
            if self._midas_rows is not None:
                # User has edited the MIDAS view — export directly from the
                # edited MIDAS rows (already in 43-column format).
                from ..midas_mapper import MIDAS_COLUMNS as _COLS
                from openpyxl import Workbook
                from openpyxl.styles import Font, PatternFill, Alignment
                wb = Workbook()
                ws = wb.active
                ws.title = "MIDAS"
                ws.append(_COLS)
                header_font = Font(bold=True, color="FFFFFF", size=10)
                header_fill = PatternFill("solid", fgColor="1A4076")
                header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
                for cell in ws[1]:
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = header_align
                ws.row_dimensions[1].height = 32
                for row in self._midas_rows:
                    ws.append([row.get(h, "") for h in _COLS])
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions
                for col_cells in ws.columns:
                    vals = [c.value for c in col_cells if c.value is not None]
                    length = max((len(str(v)) for v in vals), default=10)
                    ws.column_dimensions[col_cells[0].column_letter].width = min(max(length + 2, 12), 32)
                wb.save(path)
                out = Path(path)
            else:
                out = exp.export_midas(self.rows)
        except Exception as e:
            QMessageBox.critical(self, "Erreur d'export MIDAS", str(e))
            return
        QMessageBox.information(
            self, "Export MIDAS terminé",
            f"{len(self._midas_rows or self.rows)} ligne(s) exportée(s) au format MIDAS vers :\n{out}\n\n"
            f"Colonnes laissées vides (saisie équipe d'intégration) :\n"
            f"  • Numéro escale, Index, Range\n"
            f"  • Code transitaire / chargeur / marchandise\n"
            f"  • Manutentionaire",
        )
