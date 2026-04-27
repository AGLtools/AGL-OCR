"""Excel exporter."""
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import List, Dict

from openpyxl import Workbook, load_workbook

from .config import EXPORTS_DIR, load_fields


class ExcelExporter:
    def __init__(self, output_path: Path | None = None):
        self.fields = load_fields()
        self.headers = ["Source File", "Template", "Page"] + [f["key"] for f in self.fields]
        self.output_path = output_path or (
            EXPORTS_DIR / f"AGL_OCR_export_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        )

    def export(self, rows: List[Dict]) -> Path:
        """rows: each dict has keys 'source', 'template', 'page', plus field keys."""
        if self.output_path.exists():
            wb = load_workbook(self.output_path)
            ws = wb.active
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "Extractions"
            ws.append(self.headers)

        for row in rows:
            ws.append([row.get(h, "") for h in self.headers])

        # Auto-size columns
        for col_cells in ws.columns:
            length = max((len(str(c.value)) for c in col_cells if c.value is not None),
                         default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(length + 2, 50)

        wb.save(self.output_path)
        return self.output_path
