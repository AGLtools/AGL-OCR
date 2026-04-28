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

        btns.addStretch(1)

        self.btn_export = QPushButton("⬇ Exporter vers Excel")
        self.btn_export.setStyleSheet("font-weight: bold; background: #d4edda;")
        self.btn_export.clicked.connect(self._export_excel)
        btns.addWidget(self.btn_export)

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
