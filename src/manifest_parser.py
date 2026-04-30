"""Manifest parser: state-machine extraction of structured cargo manifests.

Uses pdfplumber for PDFs with embedded text (generated PDFs like CMA CGM).
Falls back to OCR token stream for scanned documents.

Architecture:
  - Words grouped into lines by Y-proximity.
  - Lines processed top-to-bottom with a state machine that tracks:
      vessel / voyage (page header)
      current BL record (bl_number, parties, port info)
      current SPLIT / container (container_number, weight, seal, etc.)
  - Output: one dict (ManifestRow) per container SPLIT.
  - Parser config loaded from config/parsers/<name>.yaml.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Dict, Optional, Any
import yaml

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

from .config import resource_dir
from .spatial_index import SpatialPage, extract_field


# ============================================================
# Output dataclass — one row per container split
# ============================================================
@dataclass
class ManifestRow:
    source_file: str = ""
    page: int = 0
    # Vessel header
    vessel: str = ""
    voyage: str = ""
    date_of_arrival: str = ""
    # BL header
    bl_number: str = ""
    bl_type: str = ""           # "WAYBILL" or "BL"
    shipped_on_board: str = ""
    movement: str = ""
    port_of_loading: str = ""
    port_of_discharge: str = ""
    place_of_delivery: str = ""
    place_of_acceptance: str = ""
    # Parties
    shipper: str = ""
    consignee: str = ""
    notify: str = ""
    freight_forwarder: str = ""
    # Container split
    split_number: int = 0
    container_number: str = ""
    seal1: str = ""
    seal2: str = ""
    seal3: str = ""
    container_type: str = ""    # e.g. 40HC
    tare: str = ""
    pack_qty: str = ""
    pack_unit: str = ""
    weight: str = ""
    weight_unit: str = ""
    volume: str = ""
    volume_unit: str = ""
    # Goods
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================
# Parser config (YAML)
# ============================================================
DEFAULT_CONFIG: dict = {
    "name": "generic",
    "shipowner": "",
    # X-column boundaries (in PDF points)
    "col_left_max": 200,        # x < this = left address column
    "col_mid_min": 245,         # x > this = cargo/container detail column
    # Y below which data starts (skip page header)
    "data_y_min": 100,
    # BL trigger
    "bl_number_x_min": 60,
    "bl_number_x_max": 95,
    "bl_number_pattern": r"^[A-Z]{2,5}[0-9]{4,}",
    "waybill_marker": "WAYBILL",
    # Section labels (left column)
    "section_shipper": "SH:",
    "section_consignee": "CN:",
    "section_notify": "N:",
    "section_freight_forwarder": "FE:",
    # SPLIT / container keywords (mid column)
    "split_marker": "SPLIT",
    "pack_label": "PACK:",
    "inpack_label": "IN-PACK:",
    "weight_label": "WGT:",
    "volume_label": "VOL:",
    "ctr_label": "CTR:",
    "seal1_label": "SEAL1:",
    "seal2_label": "SEAL2:",
    "seal3_label": "SEAL3:",
    "sztp_label": "SZTP:",
    "tare_label": "TARE:",
    # BL total / end marker
    "bl_total_marker": "B/L TOTAL",
    # Page header keywords
    "voyage_label": "VOYAGE:",
    "vessel_label": "VESSEL:",
    "arrival_label": "DATE OF ARRIVAL",
    "pol_label": "B/L Port of Loading",
    "pod_label": "B/L Port of Discharge",
    "delivery_label": "B/L Place of delivery",
}


def load_parser_config(name: str) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    cfg_path = resource_dir() / "config" / "parsers" / f"{name}.yaml"
    if cfg_path.exists():
        override = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        cfg.update(override)
    return cfg


# ============================================================
# Line utilities
# ============================================================
def _group_lines(words: list, y_tol: float = 4.0) -> list[list[dict]]:
    """Group pdfplumber word dicts into lines by Y proximity."""
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: list[list[dict]] = []
    current: list[dict] = [sorted_words[0]]
    current_y = sorted_words[0]["top"]
    for w in sorted_words[1:]:
        if abs(w["top"] - current_y) <= y_tol:
            current.append(w)
        else:
            lines.append(sorted(current, key=lambda w: w["x0"]))
            current = [w]
            current_y = w["top"]
    if current:
        lines.append(sorted(current, key=lambda w: w["x0"]))
    return lines


def _words_in_range(line: list[dict],
                    x_min: float = -1, x_max: float = 99999) -> list[dict]:
    return [w for w in line if x_min <= w["x0"] <= x_max]


def _text_in_range(line: list[dict],
                   x_min: float = -1, x_max: float = 99999) -> str:
    return " ".join(w["text"] for w in _words_in_range(line, x_min, x_max))


def _value_after_label(line: list[dict], label: str,
                       label_x_min: float = -1) -> str:
    """Return text of words that come AFTER `label` token in `line`."""
    tokens = [w["text"] for w in line if w["x0"] >= label_x_min]
    joined = " ".join(tokens)
    # Find label and take everything after
    idx = joined.upper().find(label.upper())
    if idx == -1:
        return ""
    after = joined[idx + len(label):].strip()
    return after


def _extract_after(text: str, label: str) -> str:
    idx = text.upper().find(label.upper())
    if idx == -1:
        return ""
    return text[idx + len(label):].strip()


# ============================================================
# Main parser class
# ============================================================
class ManifestParser:
    """Parse a cargo manifest PDF into a list of ManifestRow dicts."""

    def __init__(self, config_name: str = "cma_cgm"):
        self.cfg = load_parser_config(config_name)
        self._available = HAS_PDFPLUMBER

    @property
    def available(self) -> bool:
        return self._available

    def parse(self, pdf_path: str | Path,
              progress_callback=None) -> list[dict]:
        """Parse all pages; return list of dicts (one per container split)."""
        if not HAS_PDFPLUMBER:
            raise RuntimeError(
                "pdfplumber is required for manifest parsing. "
                "Run: pip install pdfplumber"
            )
        pdf_path = Path(pdf_path)
        rows: list[dict] = []

        # BL-level state (persists across page breaks)
        vessel = ""
        voyage = ""
        date_arrival = ""
        bl: Optional[ManifestRow] = None        # current BL header
        split: Optional[ManifestRow] = None     # current container split
        section = ""                            # SH / CN / N / FE / DESC
        shipper_lines: list[str] = []
        consignee_lines: list[str] = []
        notify_lines: list[str] = []
        ff_lines: list[str] = []
        desc_lines: list[str] = []

        cfg = self.cfg
        fname = pdf_path.name

        col_left_max = cfg["col_left_max"]
        col_mid_min = cfg["col_mid_min"]
        data_y_min = cfg["data_y_min"]
        bl_x_min = cfg["bl_number_x_min"]
        bl_x_max = cfg["bl_number_x_max"]
        bl_pat = re.compile(cfg["bl_number_pattern"])

        def _flush_split():
            nonlocal split
            if split is None or split.split_number == 0:
                return
            split.shipper = " | ".join(shipper_lines)
            split.consignee = " | ".join(consignee_lines)
            split.notify = " | ".join(notify_lines)
            split.freight_forwarder = " | ".join(ff_lines)
            split.description = " ".join(desc_lines)
            rows.append(split.to_dict())
            split = None

        def _flush_bl():
            nonlocal bl, split, section
            nonlocal shipper_lines, consignee_lines, notify_lines, ff_lines, desc_lines
            _flush_split()
            bl = None
            split = None
            section = ""
            shipper_lines = []
            consignee_lines = []
            notify_lines = []
            ff_lines = []
            desc_lines = []

        def _new_split(num: int, page_num: int) -> ManifestRow:
            r = ManifestRow(
                source_file=fname,
                page=page_num,
                vessel=vessel,
                voyage=voyage,
                date_of_arrival=date_arrival,
            )
            if bl:
                r.bl_number = bl.bl_number
                r.bl_type = bl.bl_type
                r.shipped_on_board = bl.shipped_on_board
                r.movement = bl.movement
                r.port_of_loading = bl.port_of_loading
                r.port_of_discharge = bl.port_of_discharge
                r.place_of_delivery = bl.place_of_delivery
                r.place_of_acceptance = bl.place_of_acceptance
            r.split_number = num
            return r

        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            for pg_idx, pg in enumerate(pdf.pages):
                if progress_callback:
                    progress_callback(pg_idx, total)

                words = pg.extract_words(
                    x_tolerance=6, y_tolerance=3,
                    keep_blank_chars=False,
                    extra_attrs=["size"],
                )
                lines = _group_lines(words, y_tol=4)

                # ---- Per-page header fields via SPATIAL lookup ----
                # This handles "Port of Loading" / "Port of Discharge" /
                # "Place of delivery" / vessel / voyage / date even when the
                # PDF text stream order does not match the visual layout.
                spatial = SpatialPage.from_pdfplumber_page(
                    pg, x_tolerance=3, y_tolerance=3
                )
                header_rules = cfg.get("page_header_fields", {})
                page_header: Dict[str, str] = {}
                for fname_key, rule in header_rules.items():
                    val = extract_field(spatial, rule)
                    if val:
                        page_header[fname_key] = val

                # Promote page-level globals to running state
                if page_header.get("vessel"):
                    vessel = page_header["vessel"]
                if page_header.get("voyage"):
                    voyage = page_header["voyage"]
                if page_header.get("date_of_arrival"):
                    date_arrival = page_header["date_of_arrival"]
                pol_page = page_header.get("port_of_loading", "")
                pod_page = page_header.get("port_of_discharge", "")
                delivery_page = page_header.get("place_of_delivery", "")

                # Process data lines
                for line in lines:
                    if not line:
                        continue
                    y = line[0]["top"]
                    if y < data_y_min:
                        continue

                    full_text = _text_in_range(line)
                    left_text = _text_in_range(line, x_max=col_left_max)
                    mid_text = _text_in_range(line, x_min=col_mid_min)
                    full_upper = full_text.upper()

                    # ---- B/L TOTAL → close current BL ----
                    if cfg["bl_total_marker"].upper() in full_upper:
                        _flush_bl()
                        continue

                    # ---- New BL number ----
                    # BL number is a single token at x ≈ bl_x_min..bl_x_max
                    left_words = _words_in_range(line, bl_x_min - 5, bl_x_max + 5)
                    if left_words and len(left_words) == 1:
                        tok = left_words[0]["text"]
                        if bl_pat.match(tok):
                            # Continuation: same BL repeated at top of next page.
                            # Do NOT flush — keep current parties/splits state.
                            if bl is not None and bl.bl_number == tok:
                                continue
                            _flush_bl()
                            bl = ManifestRow()
                            bl.bl_number = tok
                            # Check for waybill marker in same line
                            if cfg["waybill_marker"].upper() in full_upper:
                                bl.bl_type = "WAYBILL"
                            else:
                                bl.bl_type = "BL"
                            # Extract shipped on board / movement from same line
                            bl.shipped_on_board = _extract_after(full_text, "Board:").split()[0] if "Board:" in full_text else ""
                            m_idx = full_text.upper().find("MOVEMENT:")
                            if m_idx != -1:
                                bl.movement = full_text[m_idx + 9:].strip().split()[0]
                            # Port info from header band of this page
                            bl.port_of_loading = pol_page
                            bl.port_of_discharge = pod_page
                            bl.place_of_delivery = delivery_page
                            bl.place_of_acceptance = page_header.get("place_of_acceptance", "")
                            section = ""
                            continue

                    if bl is None:
                        continue  # haven't found a BL record yet

                    # ---- Section labels (left column) ----
                    first_word = line[0]["text"] if line else ""

                    if first_word == cfg["section_shipper"]:
                        section = "SH"
                        # Value is all words on this line AFTER the label token
                        content_words = [w for w in line if w["x0"] > line[0]["x0"] + 5]
                        content = " ".join(w["text"] for w in content_words)
                        # Filter out mid-column spillover (only left column content)
                        left_content_words = [w for w in content_words if w["x0"] < col_left_max]
                        content = " ".join(w["text"] for w in left_content_words)
                        if content:
                            shipper_lines.append(content)

                    elif first_word == cfg["section_consignee"]:
                        section = "CN"
                        content_words = [w for w in line if w["x0"] > line[0]["x0"] + 5]
                        left_content_words = [w for w in content_words if w["x0"] < col_left_max]
                        content = " ".join(w["text"] for w in left_content_words)
                        if content:
                            consignee_lines.append(content)

                    elif first_word == cfg["section_notify"]:
                        section = "N"
                        content_words = [w for w in line if w["x0"] > line[0]["x0"] + 5]
                        left_content_words = [w for w in content_words if w["x0"] < col_left_max]
                        content = " ".join(w["text"] for w in left_content_words)
                        if content:
                            notify_lines.append(content)

                    elif first_word == cfg["section_freight_forwarder"]:
                        section = "FE"
                        content_words = [w for w in line if w["x0"] > line[0]["x0"] + 5]
                        left_content_words = [w for w in content_words if w["x0"] < col_left_max]
                        content = " ".join(w["text"] for w in left_content_words)
                        if content:
                            ff_lines.append(content)

                    # ---- Address continuation (left column, x 82-200) ----
                    elif left_text and not mid_text:
                        # Only accept tokens strictly in the address zone
                        left_w = _words_in_range(line, bl_x_max + 1, col_left_max)
                        if left_w:
                            addr = " ".join(w["text"] for w in left_w)
                            if section == "SH":
                                shipper_lines.append(addr)
                            elif section == "CN":
                                consignee_lines.append(addr)
                            elif section == "N":
                                notify_lines.append(addr)
                            elif section == "FE":
                                ff_lines.append(addr)

                    # ---- SPLIT line (mid column) ----
                    if cfg["split_marker"].upper() in mid_text.upper() and ":" in mid_text:
                        # Flush previous split before starting new one
                        _flush_split()
                        # Extract split number
                        split_num = 0
                        m = re.search(r"SPLIT\s*:\s*(\d+)", mid_text.upper())
                        if m:
                            split_num = int(m.group(1))
                        split = _new_split(split_num, pg_idx + 1)
                        # Extract PACK and WGT from same line
                        pack_val = _extract_after(mid_text, cfg["pack_label"]).split()
                        if len(pack_val) >= 2:
                            split.pack_qty = pack_val[0]
                            split.pack_unit = pack_val[1]
                        elif len(pack_val) == 1:
                            split.pack_qty = pack_val[0]
                        wgt_val = _extract_after(mid_text, cfg["weight_label"]).split()
                        if len(wgt_val) >= 2:
                            split.weight = wgt_val[0]
                            split.weight_unit = wgt_val[1]
                        elif len(wgt_val) == 1:
                            split.weight = wgt_val[0]
                        continue

                    # ---- IN-PACK / VOL line ----
                    if split and cfg["volume_label"].upper() in mid_text.upper():
                        vol_val = _extract_after(mid_text, cfg["volume_label"]).split()
                        if len(vol_val) >= 2:
                            split.volume = vol_val[0]
                            split.volume_unit = vol_val[1]
                        elif len(vol_val) == 1:
                            split.volume = vol_val[0]

                    # ---- CTR / SEAL1 line ----
                    if split and cfg["ctr_label"].upper() in mid_text.upper():
                        split.container_number = _extract_after(
                            mid_text, cfg["ctr_label"]
                        ).split()[0] if _extract_after(mid_text, cfg["ctr_label"]) else ""
                    if split and cfg["seal1_label"].upper() in mid_text.upper():
                        split.seal1 = _extract_after(
                            mid_text, cfg["seal1_label"]
                        ).split()[0] if _extract_after(mid_text, cfg["seal1_label"]) else ""

                    # ---- TARE / SZTP / SEAL2 line ----
                    if split and cfg["tare_label"].upper() in mid_text.upper():
                        tare_val = _extract_after(mid_text, cfg["tare_label"]).split()
                        split.tare = tare_val[0] if tare_val else ""
                    if split and cfg["sztp_label"].upper() in mid_text.upper():
                        sztp_val = _extract_after(mid_text, cfg["sztp_label"]).split()
                        split.container_type = sztp_val[0] if sztp_val else ""
                    if split and cfg["seal2_label"].upper() in mid_text.upper():
                        s2 = _extract_after(mid_text, cfg["seal2_label"]).split()
                        split.seal2 = s2[0] if s2 else ""
                    if split and cfg["seal3_label"].upper() in mid_text.upper():
                        s3 = _extract_after(mid_text, cfg["seal3_label"]).split()
                        split.seal3 = s3[0] if s3 else ""

                    # ---- Description (mid-right column, no keyword prefix) ----
                    if split and mid_text and not any(
                        kw in mid_text.upper() for kw in (
                            "SPLIT", "CTR:", "SEAL", "TARE:", "SZTP:", "PACK:",
                            "WGT:", "VOL:", "SHP", "STAT:", "FREE", "FREIGHT",
                            "STOWED", "GOODS", "BESC", "FCL", "SHIPPED"
                        )
                    ):
                        # Only right-column words (description area)
                        desc_words = _words_in_range(line, 340, 9999)
                        if desc_words:
                            desc_lines.append(
                                " ".join(w["text"] for w in desc_words)
                            )

            # End of last page — flush whatever is open
            _flush_split()

        # Inject parser-level metadata into each row for downstream mapping
        # (e.g. MIDAS mapper uses _shipowner as fallback when BL prefix is unknown)
        shipowner = self.cfg.get("shipowner", "") if isinstance(self.cfg, dict) else ""
        if shipowner:
            for r in rows:
                if isinstance(r, dict):
                    r.setdefault("_shipowner", shipowner)

        return rows

    # ── Detect which parser to use from PDF content ──────────────────────────
    @staticmethod
    def detect_format(pdf_path: str | Path) -> Optional[str]:
        """Sniff the PDF and return a config name (e.g. 'cma_cgm') or None.

        Scans all parser YAMLs in config/parsers/ for their `detection_keyword`
        and returns the first match. Falls back to built-in heuristics for
        well-known carriers.
        """
        if not HAS_PDFPLUMBER:
            return None
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Read first 500 words of page 1
                words = pdf.pages[0].extract_words(x_tolerance=6)
                text = " ".join(w["text"] for w in words[:500]).upper()
        except Exception:
            return None

        # 1) Scan user-trained YAMLs (sorted alphabetically for determinism)
        parsers_dir = resource_dir() / "config" / "parsers"
        if parsers_dir.exists():
            for yml in sorted(parsers_dir.glob("*.yaml")):
                try:
                    cfg = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
                except Exception:
                    continue
                kw = cfg.get("detection_keyword")
                if kw and kw.upper() in text:
                    return yml.stem

        # 2) Built-in heuristics for known carriers
        if "CMA" in text and ("CGM" in text or "FRENCH LINE" in text):
            return "cma_cgm"
        if "MAERSK" in text:
            return "maersk"
        # NOTE: MSC is intentionally NOT in built-in heuristics.
        # MSC manifests have no working built-in parser — they should be handled
        # by the AI-learned format (parse_template) registered at runtime.
        # 3) Detect SAKINA-style scanned manifests (text might be sparse/absent)
        if "SAKINA" in text or ("DSM LIVERPOOL" in text and "QUESTIONNAIRE" in text):
            return "sakina"
        return None

    @staticmethod
    def _is_scanned_format(fmt: Optional[str]) -> bool:
        """Formats that require OCR (scanned PDFs, not pdfplumber)."""
        return fmt in ("sakina",)

    def parse_scanned(self, pdf_path: str | Path) -> list[dict]:
        """Route a scanned PDF to the correct extractor based on format detection."""
        from .extractors.sakina_extractor import SakinaExtractor
        pdf_path = Path(pdf_path)
        # Try to detect format; if pdfplumber sees nothing, check filename heuristics
        fmt = self.detect_format(pdf_path)
        if fmt is None:
            # pdfplumber returned no text — assume SAKINA-style scanned doc
            fmt = "sakina"
        if fmt == "sakina":
            return SakinaExtractor().extract(pdf_path)
        # Future: add other scanned formats here
        return []
