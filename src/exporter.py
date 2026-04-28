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
        """rows: each dict has keys 'source', 'template', 'page', plus field keys.
        If rows come from the manifest parser (ManifestRow.to_dict()) they carry
        their own schema; the exporter auto-builds columns from the actual keys."""
        if not rows:
            raise ValueError("No rows to export.")

        # Detect manifest rows: they have 'bl_number' or 'vessel' keys
        # (standard OCR rows use 'BL_Number', 'Vessel_Name' etc.)
        is_manifest = any("bl_number" in r or "vessel" in r for r in rows)

        if is_manifest:
            # Build headers from the union of all keys, preserving insertion order
            all_keys: list = []
            seen: set = set()
            for r in rows:
                for k in r.keys():
                    if k not in seen:
                        all_keys.append(k)
                        seen.add(k)
            headers = all_keys
        else:
            headers = self.headers

        if self.output_path.exists():
            wb = load_workbook(self.output_path)
            ws = wb.active
            # If the file already has manifest-style columns, keep them
            existing_headers = [c.value for c in next(ws.iter_rows(max_row=1))]
            if existing_headers != headers:
                # Mismatch (e.g. old file had different schema) — start fresh sheet
                ws = wb.create_sheet(title=f"Extractions_{datetime.now():%H%M%S}")
                ws.append(headers)
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "Extractions"
            ws.append(headers)

        for row in rows:
            ws.append([row.get(h, "") for h in headers])

        # Auto-size columns
        for col_cells in ws.columns:
            length = max((len(str(c.value)) for c in col_cells if c.value is not None),
                         default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(length + 2, 50)

        wb.save(self.output_path)
        return self.output_path
