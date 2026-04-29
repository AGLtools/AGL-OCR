"""Extracteur pour manifestes scannés au format SAKINA (DSM LIVERPOOL, etc.).

Architecture heuristique (sans ML) :
  1. Classification de chaque page par mots-clés
  2. Extraction du header depuis questionnaire + récap (regex Q&A)
  3. Extraction des lignes cargo par segmentation BETWEEN-BL :
       Le Tesseract lit les tableaux paysage colonne par colonne.
       Pour chaque ref BL N, les données de la ligne (expéditeur, destinataire,
       marchandise) se trouvent dans le texte entre la ref BL précédente (N-1)
       et la ref BL courante N.
  4. Assemblage en liste de dicts compatibles ManifestRow / MidasMapper
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from ..ocr_engine import OCREngine, Page


# ── Page type ───────────────────────────────────────────────────────────────
PAGE_QA          = "questionnaire"
PAGE_RECAP       = "recap"
PAGE_CARGO_TABLE = "cargo_table"
PAGE_CUSTOMS     = "customs"
PAGE_UNKNOWN     = "unknown"

_PAGE_KEYWORDS = {
    PAGE_QA:          ["questionnaire", "quel est", "capitaine", "equipage", "pavillon"],
    PAGE_RECAP:       ["recapitulation", "total feuillet", "arretee la presente"],
    PAGE_CARGO_TABLE: ["manifeste d'entree", "expediteur", "destinataire",
                       "de (from)", "a (to)", "shipper", "consignee"],
    PAGE_CUSTOMS:     ["ministere", "republique de cote d'ivoire",
                       "nombre de conteneur", "vrac solide"],
}

# ── Q&A patterns ─────────────────────────────────────────────────────────────
_QA_VESSEL   = re.compile(r"nom du navire\s*[?:]\s*(.+?)(?=QUEL|JEL|$)", re.I)
_QA_FLAG     = re.compile(r"pavillon\s*[?:]\s*([A-Z ]+?)(?=QUEL|JEL|$)", re.I)
_QA_MASTER   = re.compile(r"nom du capitaine\s*[?:]\s*(.+?)(?=QUEL|JEL|$)", re.I)
_QA_NEXT_PORT= re.compile(r"port qu.il touchera\s*[?:]\s*(.+?)(?=QUEL|JEL|DANS|$)", re.I)
_QA_POL      = re.compile(r"pris sa cargaison\s*[?:]\s*(.+?)(?=QUEL|JEL|COMB|$)", re.I)
_QA_POD      = re.compile(r"port final\s*[?:]\s*(.+?)(?=QUEL|JEL|one|SERVICE|$)", re.I)
_QA_DATE     = re.compile(r"abidjan[,\s]+le\s+(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})", re.I)
_QA_ACCONIER = re.compile(r"acconier\s*[?:]\s*(.+?)(?=QUEL|JEL|$)", re.I)
_QA_COMPANY  = re.compile(r"compagnie appartient.+?[?:]\s*(.+?)(?=COMB|$)", re.I)

# ── Recap patterns ────────────────────────────────────────────────────────────
_RECAP_VESSEL  = re.compile(r"M/?V[:\s]+([A-Z0-9 ]+?)(?=Vge|ETA|$)", re.I)
_RECAP_VOYAGE  = re.compile(r"Vge\s*[:\s]+([A-Z0-9/]+)", re.I)
_RECAP_ETA     = re.compile(r"ETA\s*[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", re.I)
_RECAP_MASTER  = re.compile(r"CAPITAINE\s*[:\s]+([A-Z ]+?)(?=RECAPIT|MANI|SAKINA|$)", re.I)
_RECAP_POL     = re.compile(r"([A-Z][A-Z. ]+)\s*/\s*ABIDJAN", re.I)

# ── Known cargo descriptions ──────────────────────────────────────────────────
# Patterns allow optional numbers between words (OCR embeds weight in description)
_CARGO_DESCS = [
    r"(DI\s+AMMONIUM(?:[\s\d,]+)?PHOSPHATE\s+IN\s+BULK)",
    r"(PINK\s+GRANNULAR\s+MURIATE\s+OF\s+POTASH\s+IN\s+BULK)",
    r"(MURIATE\s+OF\s+POTASH\s+IN\s+BULK)",
    r"(PRILLED\s+UREA\s+IN\s+BULK)",
    r"(GRANULAR\s+UREA\s+IN\s+BULK)",
    r"(DIAMMONIUM\s+PHOSPHATE\s+IN\s+BULK)",
    r"(PRILLED\s+URE\w*\s+IN\s+BULK)",
]

# ── Known shipper patterns ────────────────────────────────────────────────────
_SHIPPERS = [
    r"(JOINT\s+STOCK\s+COMPANY\s+APATIT)",
    r"(PACIFIC\s+RIL\s+INTERNATIONAL\s+FERTILIZER\s+LIMITED)",
    r"(PACIFIC\s+RIL\w*)",            # minimal fallback — OCR garbles the full name
    r"(ACRON\s+\w+)",
    r"(URALCHEM\s+\w+)",
]

# ── Known consignee patterns ─────────────────────────────────────────────────
_CONSIGNEES = [
    r"(ETG\s+INPUTS\s+IVC(?:\s+LIMITED)?)",
    r"(SOBIMAP)",
    r"(CORIS\s+BANK\s+INTERNATIONAL\s*SA)",
    r"(ETG\s+AGROSCIENCES(?:\s+(?:COTE\s+D.?IVOIRE|LIMITED))*\s*(?:SA|CO)?)",
    r"(PACIFIC\s+\w+\s+FERTILIZER)",
]

# ── OCR digit lookalike mapping ───────────────────────────────────────────────
# Applied only to the suffix of EAIF#### refs to correct 1→I, 5→S etc.
_OCR_DIGIT_MAP = str.maketrans("IOSZBLGT", "10528167")


def _normalize_ocr(text: str) -> str:
    """Fix systematic OCR substitutions in SAKINA documents."""
    def _fix_bl(m: re.Match) -> str:
        return "EAIF" + m.group(1).translate(_OCR_DIGIT_MAP)
    text = re.sub(r"EAIF([A-Z0-9]{4})\b", _fix_bl, text, flags=re.I)
    text = text.replace("\u00c6", "'").replace("\u00da", "E").replace("\u00b6", "O")
    return text


def _clean_port(raw: str) -> str:
    """Remove OCR noise after the country name."""
    if not raw:
        return raw
    raw = raw.strip().replace("\u00c6", "'").replace("\u00da", "E")
    noise = re.compile(
        r"\s+(?:one\b|a\s+service|service\b|consignation|sakina|manif|quel"
        r"|dans|acconier|combien|arrete|total|vge|eta|mani|via)",
        re.I
    )
    m = noise.search(raw)
    if m:
        raw = raw[:m.start()]
    return raw.rstrip(",/ \t")[:60].strip()


@dataclass
class _CargoLine:
    bl_number:   str = ""
    shipper:     str = ""
    consignee:   str = ""
    description: str = ""
    weight:      str = ""
    pack_qty:    str = ""
    transit_to:  str = ""


@dataclass
class _Header:
    vessel:             str = ""
    voyage:             str = ""
    date_of_arrival:    str = ""
    flag:               str = ""
    master:             str = ""
    port_of_loading:    str = ""
    port_of_discharge:  str = ""
    shipowner_company:  str = ""
    acconier:           str = ""
    source_file:        str = ""


class SakinaExtractor:
    """Extract structured data from SAKINA-style scanned manifests."""

    def __init__(self):
        self.engine = OCREngine()

    # ────────────────────────────────────────────────────────────────
    # Main entry point
    # ────────────────────────────────────────────────────────────────
    def extract(self, pdf_path: Path) -> list[dict]:
        """Return a list of ManifestRow-compatible dicts."""
        pages = self.engine.load_document(pdf_path)
        for p in pages:
            self.engine.ensure_page_ocr(p)

        # ── Header: try every page ───────────────────────────────────
        header = _Header(source_file=str(pdf_path))
        for p in pages:
            self._extract_qa_header(p, header)
            self._extract_recap_header(p, header)

        # ── Ports from combined text ─────────────────────────────────
        all_text = _normalize_ocr(" ".join(p.text() for p in pages))
        self._refine_ports(all_text, header)

        # ── Cargo pages ──────────────────────────────────────────────
        page_types = [self._classify_page(p) for p in pages]
        cargo_texts = [
            _normalize_ocr(p.text())
            for p, pt in zip(pages, page_types)
            if pt in (PAGE_CARGO_TABLE, PAGE_CUSTOMS, PAGE_UNKNOWN)
        ]
        if not cargo_texts:
            cargo_texts = [_normalize_ocr(p.text()) for p in pages]

        cargo_lines = self._extract_by_bl_refs(cargo_texts)
        if not cargo_lines:
            return [self._assemble_row(header, _CargoLine())]

        return [self._assemble_row(header, ln) for ln in cargo_lines]

    # ────────────────────────────────────────────────────────────────
    # Page classification
    # ────────────────────────────────────────────────────────────────
    def _classify_page(self, page: Page) -> str:
        text_lc = _normalize_ocr(page.text()).lower()
        scores = {pt: 0 for pt in _PAGE_KEYWORDS}
        for pt, kws in _PAGE_KEYWORDS.items():
            for kw in kws:
                if kw in text_lc:
                    scores[pt] += 1
        if scores[PAGE_QA] > 0 and "quel est" not in text_lc and "questionnaire" not in text_lc:
            scores[PAGE_QA] = max(0, scores[PAGE_QA] - 2)
        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else PAGE_UNKNOWN

    # ────────────────────────────────────────────────────────────────
    # Header extractors
    # ────────────────────────────────────────────────────────────────
    def _extract_qa_header(self, page: Page, hdr: _Header) -> None:
        flat = re.sub(r"\s+", " ", _normalize_ocr(page.text()))

        def _get(pattern) -> str:
            m = pattern.search(flat)
            return m.group(1).strip() if m else ""

        if not hdr.vessel:
            hdr.vessel = _get(_QA_VESSEL)
        if not hdr.flag:
            hdr.flag = _get(_QA_FLAG)
        if not hdr.master:
            hdr.master = _get(_QA_MASTER)
        if not hdr.acconier:
            hdr.acconier = _get(_QA_ACCONIER)
        if not hdr.shipowner_company:
            hdr.shipowner_company = _get(_QA_COMPANY)
        if not hdr.date_of_arrival:
            m = _QA_DATE.search(flat)
            if m:
                hdr.date_of_arrival = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"

    def _extract_recap_header(self, page: Page, hdr: _Header) -> None:
        flat = re.sub(r"\s+", " ", _normalize_ocr(page.text()))

        def _get(pattern) -> str:
            m = pattern.search(flat)
            return m.group(1).strip() if m else ""

        if not hdr.vessel:
            v = _get(_RECAP_VESSEL)
            if v and len(v) > 3:
                hdr.vessel = v.strip()
        if not hdr.voyage:
            hdr.voyage = _get(_RECAP_VOYAGE)
        if not hdr.date_of_arrival:
            hdr.date_of_arrival = _get(_RECAP_ETA)
        if not hdr.master:
            m_val = _get(_RECAP_MASTER)
            if m_val and len(m_val) > 4:
                hdr.master = m_val.strip()

    def _refine_ports(self, full_text: str, hdr: _Header) -> None:
        flat = re.sub(r"\s+", " ", full_text)

        if not hdr.port_of_loading:
            m = _QA_POL.search(flat)
            if m:
                hdr.port_of_loading = _clean_port(m.group(1))
        if not hdr.port_of_loading:
            m = _RECAP_POL.search(flat)
            if m:
                hdr.port_of_loading = _clean_port(m.group(1))
        if not hdr.port_of_loading:
            m = re.search(r"DE\s*\(FROM\)\s*[:\s]+([A-Z][A-Z., ']+?)(?=\s+A\s*\(TO\)|$)", flat, re.I)
            if m:
                hdr.port_of_loading = _clean_port(m.group(1))

        if not hdr.port_of_discharge:
            m = _QA_POD.search(flat)
            if m:
                hdr.port_of_discharge = _clean_port(m.group(1))
        if not hdr.port_of_discharge:
            m = _QA_NEXT_PORT.search(flat)
            if m:
                hdr.port_of_discharge = _clean_port(m.group(1))
        if not hdr.port_of_discharge:
            m = re.search(
                r"A\s*\(TO\)\s*[:\s]+([A-Z][A-Z., ']{4,40}?)(?=\s+EXPEDITEUR|\s{3,}|$)",
                flat, re.I
            )
            if m:
                hdr.port_of_discharge = _clean_port(m.group(1))

    # ────────────────────────────────────────────────────────────────
    # BL-anchored cargo extraction — BETWEEN-BL segmentation
    # ────────────────────────────────────────────────────────────────
    def _extract_by_bl_refs(self, page_texts: list[str]) -> list[_CargoLine]:
        """
        Primary strategy: BETWEEN-BL segmentation.

        Tesseract reads landscape tables column by column, so for BL ref N,
        the row data (shipper, consignee, description) appears in the corpus
        segment BETWEEN the previous BL ref (N-1) and the current BL ref N.

        For description+weight we also check explicit co-location from customs
        pages (dw_map), which overrides the segment result when reliable.
        """
        corpus = " ".join(page_texts)

        # Build explicit BL→{desc, weight} from customs-page explicit pairings
        dw_map = self._build_desc_weight_map(corpus)

        # Collect unique BL positions (first occurrence of each)
        seen: set[str] = set()
        unique_bls: list[tuple[str, int]] = []
        for m in re.finditer(r"EAIF\d{3,4}", corpus, re.I):
            bl = m.group(0).upper()
            if bl not in seen:
                seen.add(bl)
                unique_bls.append((bl, m.start()))

        results: list[_CargoLine] = []
        for i, (bl, pos) in enumerate(unique_bls):
            bl_end = pos + len(bl)

            # "Between" segment: from end of previous BL to start of this BL
            if i > 0:
                prev_end = unique_bls[i - 1][1] + len(unique_bls[i - 1][0])
            else:
                prev_end = max(0, pos - 800)
            between = corpus[prev_end: pos]

            # "After" segment: from this BL to next BL (or +600 chars)
            if i + 1 < len(unique_bls):
                next_pos = unique_bls[i + 1][1]
            else:
                next_pos = bl_end + 600
            after = corpus[bl_end: next_pos]

            full_ctx = between + after

            # ── Description ─────────────────────────────────────────
            # Only use dw_map if it has BOTH description AND weight (customs data)
            pref = dw_map.get(bl, {})
            desc = ""
            if pref.get("description") and pref.get("weight"):
                desc = pref["description"]
            if not desc:
                # Keyword inference from "between" segment (robust for garbled tables)
                desc = self._extract_desc_keywords(between)
            if not desc:
                desc = self._normalize_desc(self._extract_field(between, _CARGO_DESCS))
            if not desc:
                desc = self._extract_desc_keywords(after[:400])
            if not desc:
                desc = self._normalize_desc(self._extract_field(after[:300], _CARGO_DESCS))

            # ── Weight ───────────────────────────────────────────────
            weight = pref.get("weight", "")
            if not weight:
                wm = re.search(r"(?<!\d)(\d{5,9})(?!\d)", after[:200])
                if wm:
                    weight = wm.group(1)

            # ── Shipper / Consignee from "between" segment ────────────
            shipper   = self._extract_field(between, _SHIPPERS)
            consignee = self._extract_field(between, _CONSIGNEES)
            if not shipper:
                shipper = self._extract_field(after[:400], _SHIPPERS)
            if not consignee:
                consignee = self._extract_field(after[:400], _CONSIGNEES)

            # ── Transit ──────────────────────────────────────────────
            # Require "EN TRANSIT" to appear near "BURKINA FASO" (avoids false positives
            # when BURKINA FASO leaks from the previous row's "between" segment)
            transit = ""
            if re.search(
                r"EN\s+TRANSIT.{0,150}BURKINA\s+FASO|BURKINA\s+FASO.{0,150}EN\s+TRANSIT",
                full_ctx, re.I
            ):
                transit = "BURKINA FASO"

            results.append(_CargoLine(
                bl_number=bl,
                shipper=shipper,
                consignee=consignee,
                description=desc,
                weight=weight,
                transit_to=transit,
            ))

        if not results:
            results = self._fallback_by_description(corpus)

        return results

    def _build_desc_weight_map(self, corpus: str) -> dict[str, dict]:
        """
        Map BL ref → {description, weight} from explicit co-locations.
        Prefers entries where BOTH description AND weight appear after the BL ref.
        A later occurrence with weight overrides an earlier one without weight.
        """
        result: dict[str, dict] = {}

        for m in re.finditer(r"(EAIF\d{3,4})", corpus, re.I):
            bl = m.group(1).upper()
            after = corpus[m.end(): m.end() + 400]
            for pat in _CARGO_DESCS:
                dm = re.search(pat, after, re.I)
                if dm:
                    desc_raw = re.sub(r"\d+", " ", dm.group(1))
                    desc = re.sub(r"\s+", " ", desc_raw).strip()
                    weight_region = after[: dm.end() + 50]
                    wm = re.search(r"(?<!\d)(\d{5,9})(?!\d)", weight_region)
                    new_weight = wm.group(1) if wm else ""
                    existing = result.get(bl, {})
                    # Override: if new has weight and old doesn't (customs page data wins)
                    if bl not in result or (new_weight and not existing.get("weight")):
                        result[bl] = {
                            "description": self._normalize_desc(desc),
                            "weight": new_weight,
                        }
                    break

        return result

    # ────────────────────────────────────────────────────────────────
    # Helpers
    # ────────────────────────────────────────────────────────────────
    @staticmethod
    def _extract_field(ctx: str, patterns: list[str]) -> str:
        for pat in patterns:
            m = re.search(pat, ctx, re.I)
            if m:
                return re.sub(r"\s+", " ", m.group(1)).strip()
        return ""

    @staticmethod
    def _normalize_desc(desc: str) -> str:
        """Fix common OCR truncations in cargo descriptions."""
        if not desc:
            return desc
        desc = re.sub(r"\bURE\b", "UREA", desc)
        desc = re.sub(r"\d+\s*", "", desc)   # strip any embedded numbers
        desc = re.sub(r"\s+", " ", desc)
        return desc.strip()

    @staticmethod
    def _extract_desc_keywords(ctx: str) -> str:
        """Infer cargo description from keyword presence in context."""
        u = ctx.upper()
        if "PINK" in u and ("GRANNULAR" in u or "GRANULAR" in u):
            return "PINK GRANNULAR MURIATE OF POTASH IN BULK"
        if "MURIATE" in u and "POTASH" in u:
            return "MURIATE OF POTASH IN BULK"
        if ("AMMONIUM" in u or "PHOSPHATE" in u) and ("DI " in u or "DIAMMONIUM" in u):
            return "DI AMMONIUM PHOSPHATE IN BULK"
        if "UREA" in u or "URE " in u:
            prefix = "PRILLED " if "PRILLED" in u else ""
            return f"{prefix}UREA IN BULK"
        return ""

    def _fallback_by_description(self, corpus: str) -> list[_CargoLine]:
        """Last-resort: extract by cargo description when no BL refs found."""
        seen: set[str] = set()
        lines: list[_CargoLine] = []
        for pat in _CARGO_DESCS:
            for m in re.finditer(pat, corpus, re.I):
                desc = re.sub(r"\s+", " ", m.group(1)).strip()
                if desc in seen:
                    continue
                seen.add(desc)
                ctx = corpus[max(0, m.start() - 300): m.end() + 200]
                wm = re.search(r"(?<!\d)(\d{5,9})(?!\d)", corpus[m.end(): m.end() + 200])
                transit = "BURKINA FASO" if re.search(r"BURKINA\s+FASO", ctx, re.I) else ""
                lines.append(_CargoLine(
                    description=self._normalize_desc(desc),
                    weight=wm.group(1) if wm else "",
                    shipper=self._extract_field(ctx, _SHIPPERS),
                    consignee=self._extract_field(ctx, _CONSIGNEES),
                    transit_to=transit,
                ))
        return lines

    # ────────────────────────────────────────────────────────────────
    # Assemble ManifestRow-compatible dict
    # ────────────────────────────────────────────────────────────────
    def _assemble_row(self, hdr: _Header, ln: _CargoLine) -> dict:
        return {
            "source_file":         hdr.source_file,
            "page":                0,
            "vessel":              hdr.vessel,
            "voyage":              hdr.voyage,
            "date_of_arrival":     hdr.date_of_arrival,
            "bl_number":           ln.bl_number,
            "bl_type":             "BL",
            "shipped_on_board":    "",
            "movement":            "LCL" if ln.transit_to else "FCL/FCL",
            "port_of_loading":     hdr.port_of_loading,
            "port_of_discharge":   hdr.port_of_discharge,
            "place_of_delivery":   ln.transit_to if ln.transit_to else hdr.port_of_discharge,
            "place_of_acceptance": hdr.port_of_loading,
            "shipper":             ln.shipper,
            "consignee":           ln.consignee,
            "notify":              "",
            "freight_forwarder":   hdr.acconier,
            "split_number":        1,
            "container_number":    "",
            "seal1":               "",
            "seal2":               "",
            "seal3":               "",
            "container_type":      "VRAC",
            "tare":                "",
            "pack_qty":            ln.pack_qty,
            "pack_unit":           "COLIS",
            "weight":              ln.weight,
            "weight_unit":         "KGS",
            "volume":              "",
            "volume_unit":         "",
            "description":         ln.description,
            "_shipowner":          hdr.shipowner_company,
            "_is_scanned":         True,
            "_transit_to":         ln.transit_to,
        }
