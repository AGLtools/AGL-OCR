"""Universal AI-powered manifest extraction via Gemini.

Handles:
- Text PDFs (pdfplumber) → send text → JSON rows
- Scanned PDFs (no embedded text) → upload to Gemini File API for native vision
- Format learning (single sample → signature + hints persisted)
- Per-row correction (issue list → corrected dict)

All Gemini calls use response_mime_type=application/json so we get strict JSON
back (no markdown fences to strip).

Output schema matches src.manifest_parser.ManifestRow keys, so downstream
midas_mapper / review dialog work without changes.
"""
from __future__ import annotations
import base64
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

from .gemini_client import get_model, generate_with_fallback, get_model_name
from . import vision_client
from . import debug_log


class AICancelled(Exception):
    """Raised when an AI/OCR operation is cancelled by the user."""
    pass


def _check_cancel(cb):
    if cb is not None and cb():
        raise AICancelled("Opération annulée par l'utilisateur")


# ── Output JSON schema (string-templated to avoid .format() collisions) ────
_SCHEMA_JSON = """{
  "vessel": "nom du navire",
  "voyage": "numéro de voyage / VOY",
  "date_of_arrival": "DD/MM/YYYY",
  "shipowner": "armateur (compagnie maritime émettrice du document)",
  "rows": [
    {
      "bl_number": "numéro de connaissement (BL/HBL)",
      "bl_type": "BL ou WAYBILL",
      "shipper": "expéditeur (raison sociale complète)",
      "consignee": "destinataire (raison sociale complète)",
      "notify": "à notifier",
      "freight_forwarder": "transitaire / forwarder",
      "port_of_loading": "port de chargement (ville, pays)",
      "port_of_discharge": "port de déchargement (ville, pays)",
      "place_of_delivery": "lieu de livraison final si transit",
      "place_of_acceptance": "lieu de prise en charge",
      "container_number": "n° conteneur ISO (4 lettres + 7 chiffres) ou vide si vrac",
      "container_type": "type ISO (20GP, 40HC, 40RH...) ou vide",
      "seal1": "n° de scellé principal",
      "weight": "poids brut, NOMBRE SEUL sans séparateurs (ex: 27439560)",
      "weight_unit": "KGS ou MT ou TONS",
      "pack_qty": "nombre de colis (nombre seul)",
      "pack_unit": "type de colis (BAGS, PCS, BULK...)",
      "volume": "volume si présent (nombre seul)",
      "volume_unit": "M3 / CBM",
      "description": "description complète de la marchandise",
      "_transit_to": "pays/ville de destination finale si EN TRANSIT, sinon vide"
    }
  ]
}"""


# ── Extraction prompt ──────────────────────────────────────────────────────
_EXTRACT_INSTRUCTIONS = """Tu es un expert en analyse de manifestes maritimes (cargo manifests, bills of lading).

Ta tâche : extraire TOUTES les lignes de cargaison du document fourni dans une structure JSON STRICTE.

Règles métier IMPORTANTES :
1. Une ligne de sortie = UN CONTENEUR (split). Si un BL contient 3 conteneurs, produis 3 objets dans "rows" avec le MÊME bl_number et les détails propres à chaque conteneur.
2. Si la cargaison est en VRAC (bulk, pas de conteneur), produis UNE ligne par BL avec container_number vide.
3. Les champs d'en-tête (vessel, voyage, date_of_arrival, shipowner) sont au niveau racine — ils s'appliquent implicitement à toutes les lignes.
4. Numéros de conteneur ISO 6346 : 4 lettres MAJUSCULES + 7 chiffres (ex: MSCU1234567, EAIF1520 N'EST PAS un conteneur, c'est un BL).
5. Poids : extrais UNIQUEMENT le nombre brut, sans espaces ni virgules (ex: « 27 439 560 KGS » → weight: "27439560", weight_unit: "KGS").
6. Si un champ n'apparaît pas dans le document, mets "" (chaîne vide). N'INVENTE JAMAIS de données.
7. Corrige les erreurs OCR évidentes : I↔1, O↔0, S↔5, B↔8, Z↔2 dans les codes alphanumériques (BL, conteneurs).
8. Le shipowner est le transporteur maritime émetteur (CMA CGM, MSC, Maersk, etc.), PAS le shipper de la marchandise.
9. Pour transit : si tu vois « EN TRANSIT POUR X » ou « FINAL DESTINATION X » à côté d'une ligne, mets X dans _transit_to.

Réponds UNIQUEMENT avec du JSON valide conforme au schéma ci-dessous. Pas de markdown, pas de texte explicatif.

SCHÉMA :
""" + _SCHEMA_JSON


# -- LEARN prompt: classification + AI-generated parse(text) Python function --
# Two-section response: JSON metadata, then Python code between explicit markers.
# This avoids ALL JSON-escape pitfalls with regex backslashes.
#
# CRITICAL: this prompt is paired with an IMAGE of page 1 so the model can SEE
# the spatial layout (columns, alignment) AND read the EXACT text pdfplumber
# produces. The image+text pair lets the model understand the gap between
# "what a human reads" and "what the parser receives".
_LEARN_COMBINED_INSTRUCTIONS = """Tu es L'EXPERT mondial en retro-ingenierie de manifestes maritimes (MSC, CMA-CGM, Maersk, Hapag-Lloyd, ONE, Evergreen, COSCO, etc.).

Tu recois DEUX choses pour ce travail :
  (A) Une IMAGE de la page 1 du manifeste (mise en page reelle, colonnes, alignement visuel).
  (B) Le TEXTE BRUT exact extrait par pdfplumber (multi-pages).

==> COMPRENDS BIEN : ton parser tournera sur le TEXTE (B), JAMAIS sur l'image.
==> Mais l'image (A) te montre la STRUCTURE LOGIQUE pour que tu puisses
    faire le pont avec ce que pdfplumber a produit (souvent tres different
    visuellement : pdfplumber FUSIONNE souvent les colonnes sur UNE SEULE
    LIGNE separees par des espaces, alors que visuellement elles paraissent
    sur plusieurs lignes).

Ta mission : produire (1) un JSON de classification, et (2) une FONCTION
PYTHON `parse(text)` capable d'extraire toutes les lignes de cargaison
LOCALEMENT (sans IA) lors des prochains documents identiques.

REPONDS EXACTEMENT DANS CE FORMAT, RIEN AVANT, RIEN APRES :

<<<JSON>>>
{
  "format_name": "MAJUSCULES (ex: MSC, CMA_CGM, MAERSK)",
  "carrier_name": "nom complet du transporteur",
  "signature_keywords": ["token1", "token2", "token3"],
  "is_scanned": false,
  "layout_analysis": "explique en 3-5 phrases comment tu vois le format dans l'image ET comment pdfplumber l'a converti en texte (col fusionnees ? ligne par ligne ? blocs ?). Cite des marqueurs concrets visibles dans le texte qui delimitent un BL et un conteneur.",
  "extraction_strategy": "decris en 2-3 phrases l'algorithme exact que ton parse() va utiliser (split sur quoi, regex pour quoi, boucle imbriquee pour conteneurs ?).",
  "shipowner": "valeur litterale (ex: MSC)",
  "header_field_patterns": {
    "vessel":            "regex avec UN groupe capture (...)",
    "voyage":            "regex avec UN groupe capture (...)",
    "date_of_arrival":   "regex avec UN groupe capture (...)",
    "port_of_loading":   "regex avec UN groupe capture (...) — port d'embarquement (souvent 'PORT OF LOADING' / 'POL')",
    "port_of_discharge": "regex avec UN groupe capture (...) — port de debarquement (souvent 'PORT OF DISCHARGE' / 'POD')",
    "place_of_acceptance": "regex OU vide si non present — lieu de prise en charge",
    "place_of_delivery":   "regex OU vide si non present — lieu de livraison final"
  },
  "spatial_template": {
    "bl_marker_pattern":  "regex qui matche UN numero de BL (groupe 1) — ex 'ABJ\\\\d{9}' ou 'MEDU\\\\w+'",
    "bl_split_strategy":  "marker | gap | page",
    "container_marker":   "regex avec UN groupe capture pour le numero de conteneur — ex '\\\\(\\\\s*CN\\\\s*\\\\)\\\\s*([A-Z]{4}\\\\d{7})' ou 'CN:\\\\s*([A-Z]{4}\\\\d{7})'. Vide si pas de conteneurs.",
    "weight_scope":       "per_bl | per_container | per_page (per_bl quand le poids dans le doc est un TOTAL par BL — c'est le cas le plus frequent)",
    "field_rules": [
      {
        "field_name":   "shipper",
        "anchor_text":  ["SHIPPER"],
        "direction":    "below",
        "max_distance": 80,
        "x_tolerance":  60,
        "max_words":    20,
        "stop_at":      ["CONSIGNEE", "NOTIFY"],
        "scope":        "bl_block"
      }
    ]
  }
}
<<<END_JSON>>>
<<<PYTHON>>>
def parse(text: str) -> list:
    rows = []
    # ... ton code ici, utilise `re` directement (deja injecte, NE PAS importer) ...
    return rows
<<<END_PYTHON>>>

REGLES POUR LE BLOC PYTHON :

0. PRIORITE : LE TEMPLATE SPATIAL EST L'EXTRACTEUR PRIMAIRE.
   Le bloc <<<JSON>>>.spatial_template est CONSOMME PAR UN MOTEUR
   D'EXTRACTION SPATIAL (Python) deja implemente — tu CONFIGURES
   ce moteur, tu ne le programmes pas. Le bloc <<<PYTHON>>> reste un
   FILET DE SECURITE en cas d'echec du moteur spatial. Construis
   spatial_template avec soin :
   - bl_marker_pattern : la regex la plus specifique possible qui ne
     matche QUE de vrais numeros de BL (un par BL).
   - container_marker : prefixe le pattern avec une ANCRE textuelle
     ((CN), CN:, etc.) pour eviter les faux positifs sur les BL eux-memes.
   - weight_scope : choisis "per_bl" si le poids dans le doc est un
     TOTAL pour tout le BL (cas le plus frequent : MSC, PIL, CMA-CGM).
     Choisis "per_container" UNIQUEMENT si chaque conteneur affiche
     son propre poids distinct dans le texte.
   - field_rules : decrit chaque champ par sa relation spatiale
     (anchor_text + direction + max_distance). Utilise scope="bl_block"
     pour les champs qui varient d'un BL a l'autre (shipper, consignee,
     weight, volume, seals, ports si specifiques au BL) et scope="page"
     (defaut) pour les champs document-niveau (vessel, voyage, date).
     Direction = "below" si la valeur est SOUS le label (typique pour
     SHIPPER / CONSIGNEE multi-lignes), "right" si elle est A DROITE
     (typique pour VESSEL: XXX, VOY: YYY).
     stop_at = liste de mots qui terminent la capture (utile pour ne
     pas continuer dans le label suivant).

1. Le code DOIT definir UNE fonction `def parse(text: str) -> list:` qui retourne
   une liste de dictionnaires, UN par ligne de cargaison (un par BL si vrac,
   un par CONTENEUR si conteneurise — typique : MSC, CMA-CGM).

2. Modules : `re` est DEJA INJECTE dans le namespace. N'ECRIS AUCUN `import`,
   pas meme `import re`. Tout `import` fera planter le sandbox.

3. ECRIS DU PYTHON 100% VALIDE :
   - Indentation 4 espaces.
   - PAS de commentaires C-style /* ... */ — utilise `#`.
   - PAS de typedict, PAS d'annotations exotiques.
   - Pas de f-string complexe inutile.

4. Cles AUTORISEES (utilise UNIQUEMENT celles-ci, n'invente RIEN d'autre) :
   bl_number, bl_type, container_number, container_type,
   weight, weight_unit, pack_qty, pack_unit, volume, volume_unit,
   seal1, seal2, seal3, shipper, consignee, notify, freight_forwarder,
   port_of_loading, port_of_discharge, place_of_delivery, place_of_acceptance,
   description, page.

5. STRATEGIE GAGNANTE pour les manifestes MULTI-LIGNES :
   - Decoupe d'abord en BLOCS-PAR-BL avec un marqueur stable et UNIQUE
     visible dans le texte (ex: 'SH:' debut de ligne, 'B/L NR.', 'BL Nr.',
     prefixe armateur MEDU/CMAU/MAEU). Choisis le marqueur qui apparait
     EXACTEMENT une fois par BL dans le texte reel.
   - Pour chaque bloc, extrait les champs sur le bloc entier (pas ligne-par-ligne).
   - PATTERN RECOMMANDE pour les conteneurs (quand le format utilise `CN:`):
     ```python
     cn_iter = list(re.finditer(r'CN:([A-Z]{4}\\d{7})', block))
     for i, cm in enumerate(cn_iter):
         cn_start = cm.start()
         cn_end = cn_iter[i+1].start() if i+1 < len(cn_iter) else len(block)
         cn_block = block[cn_start:cn_end]
         container_number = cm.group(1)   # toujours juste, jamais de faux positif
     ```
     Si le format n'utilise pas `CN:`, cherche les conteneurs ISO directement
     avec `re.finditer(r'\\b([A-Z]{4}\\d{7})\\b', block)` en filtrant les
     resultats qui ressemblent a des numeros BL (ex: si len > 11 chars autour).

6. PIEGES CONNUS pdfplumber / OCR :
   - Les COLONNES sont souvent FUSIONNEES sur la meme ligne, separees par
     des espaces multiples. Ne suppose PAS un layout 'label\\nvaleur'.
   - Les en-tetes de colonne (ex: 'B/L NR.') matchent souvent ta regex AVANT
     les vraies valeurs. Filtre-les explicitement (ex: skip si la valeur
     capturee est 'NR.' ou un mot-cle, ou cherche au moins N caracteres).
   - Les nombres peuvent contenir des virgules (ex: '22,754.000 kgs.'). 
     Ta regex doit autoriser virgules ET points : `[\\d,]+\\.?\\d*`.
   - ATTENTION AUX FAUX POSITIFS DE NUMEROS DE CONTENEUR : les numeros BL
     (ex: MEDUJ4270495) contiennent des sous-chaines [A-Z]{4}\\d{7}
     (ex: EDUJ4270495). N'utilise JAMAIS `[A-Z]{4}\\d{7}` seul pour trouver
     un conteneur. Utilise TOUJOURS le prefixe `CN:` comme ancre :
       cn_matches = list(re.finditer(r'CN:([A-Z]{4}\\d{7})', block))
     ou, pour le premier conteneur inline sur la meme ligne que le BL :
       re.search(r'CN:([A-Z]{4}\\d{7})', first_line)
     Cette approche est robuste car le manifeste delimite toujours les
     conteneurs avec le prefixe `CN:`.
   - OCR TOLERANT : les marqueurs fixes comme `(CN)`, `(SN)`, `5a.Port`,
     `B/L.NO.` peuvent apparaitre avec des espaces inseres par l'OCR :
     `( CN )`, `5 a . Port`, `B / L . NO .`. Utilise `\\s*` autour de
     chaque caractere de ponctuation dans tes regex critiques. Ex :
       r'\\(\\s*CN\\s*\\)\\s*([A-Z]{4}\\d{7})'
       r'5\\s*a\\s*\\.\\s*Port\\s+of\\s+loading\\s+([^\\n]+)'

7. POIDS — REGLE ABSOLUE (erreur tres frequente) :
   - Le poids indique dans le manifeste est le POIDS TOTAL DU BL, PAS le
     poids par conteneur.
   - NE DIVISE JAMAIS le poids par le nombre de conteneurs.
   - ASSIGNE LE MEME POIDS TOTAL a chaque ligne conteneur du meme BL.
   - Si le manifeste montre "22 754 KGS" pour un BL de 3 conteneurs, chaque
     ligne doit avoir weight=22754, pas weight=7584.67.
   - Idem pour volume, pack_qty : valeurs identiques sur chaque ligne conteneur.

8. ROBUSTESSE :
   - Tolerant aux espaces multiples (`\\s+`).
   - Skippe silencieusement les blocs non-cargo (en-tetes, totaux, footers).
   - Pas d'exception non rattrapee : enrobe les conversions float/int en
     try/except si le pattern peut matcher du texte malforme.
   - JAMAIS print(), open(), exec(), eval(), pas d'acces fichier/reseau.

9. AUTO-VERIFICATION OBLIGATOIRE AVANT DE REPONDRE :
   Mentalement, applique ton parse() sur le TEXTE (B) reel. Compte les BL
   distincts visibles dans l'image (A). Ta fonction DOIT en produire AU
   MOINS autant. Si ton premier jet donne 0, REPENSE l'approche depuis le
   debut et recris parse(). Ne soumets JAMAIS un parse() qui rendrait 0
   sur le texte fourni.

[header_field_patterns]
- UN SEUL groupe capture (...) par regex, qui isole la valeur sans le label.
- Le code prend le premier match trouve dans tout le document.

[signature_keywords]
- 3 a 6 tokens TRES SPECIFIQUES presents dans CE document, 1-3 mots max chacun.
- Evite les generiques : BL, VESSEL, CARGO, MANIFEST.
"""

# ── Per-row correction prompt ──────────────────────────────────────────────
_FIX_INSTRUCTIONS = """Tu es un expert en correction de données extraites de manifestes maritimes.

Voici une ligne de cargaison qui présente des problèmes de qualité. Corrige-la en t'appuyant sur le contexte du document fourni.

Règles :
- Renvoie UNIQUEMENT un JSON avec les MÊMES clés que la ligne d'origine.
- Ne modifie QUE les champs où tu trouves une correction sûre dans le contexte.
- Si tu n'es pas sûr d'un champ, laisse-le tel quel — n'invente JAMAIS.
- Format poids : nombre seul sans séparateurs (ex: "27439560").
- Format conteneur : 4 lettres + 7 chiffres ISO 6346.
- Pas de markdown, pas d'explication.
"""


# ============================================================
# PUBLIC API
# ============================================================
def extract_rows_from_pdf(
    pdf_path: str | Path,
    *,
    extra_hints: str = "",
    example_rows: Optional[List[Dict]] = None,
    progress_cb=None,
    cancel_check=None,
) -> List[Dict]:
    """Universal entry point — works on any PDF (text or scanned).

    Strategy:
    1. Try pdfplumber to extract embedded text (per page).
    2. If text is empty, OCR via Cloud Vision.
    3. If the document is long (>10 pages), split into chunks and call
       Gemini once per chunk, then concatenate rows.
    """
    pdf_path = Path(pdf_path)
    _check_cancel(cancel_check)
    if progress_cb:
        progress_cb("Lecture du texte intégré du PDF…")

    pages = _read_pdf_text_pages(pdf_path)
    total_text = "\n\n".join(pages).strip()
    source_kind = "text"

    if len(total_text) < 200:
        # Scanned PDF — OCR. Honor the user's saved OCR engine preference
        # (Cloud Vision / local Tesseract / auto).
        from .gemini_client import get_ocr_engine
        pref = get_ocr_engine()
        if progress_cb:
            engine_label = {
                "cloud_vision": "Cloud Vision",
                "local": "Tesseract local",
            }.get(pref, "auto")
            progress_cb(f"Document scanné — OCR ({engine_label}) en cours…")
        if pref == "cloud_vision":
            ocr_text = vision_client.ocr_pdf(
                pdf_path,
                progress_cb=lambda i, n: (
                    progress_cb(f"OCR Cloud Vision page {i}/{n}…") if progress_cb else None
                ),
                cancel_check=cancel_check,
            )
        elif pref == "local":
            ocr_text = vision_client.local_ocr_pdf(
                pdf_path,
                progress_cb=lambda i, n: (
                    progress_cb(f"OCR local page {i}/{n}…") if progress_cb else None
                ),
                cancel_check=cancel_check,
            )
        else:
            ocr_text = vision_client.ocr_scanned_pdf(
                pdf_path,
                progress_cb=lambda i, n: (
                    progress_cb(f"OCR page {i}/{n}…") if progress_cb else None
                ),
                cancel_check=cancel_check,
            )
        source_kind = "ocr"
        debug_log.log_call(
            kind="ocr",
            source_file=str(pdf_path),
            ocr_text=ocr_text,
            extra={"chars": len(ocr_text), "engine": pref},
        )
        if not ocr_text.strip():
            return []
        pages = _split_text_by_size(ocr_text, target_chars=12000)
        total_text = ocr_text

    # Decide if we need to chunk
    chunks = _build_chunks(pages, max_chars=18000)
    n = len(chunks)
    if progress_cb:
        progress_cb(
            f"Structuration via Gemini ({n} chunk(s))…"
            + (" (texte issu de Vision OCR)" if source_kind == "vision" else "")
        )
    _check_cancel(cancel_check)

    all_rows: List[Dict] = []
    header: Dict = {}

    if n == 1:
        # ── Single chunk: no parallelism overhead ─────────────────
        rows, hdr = _extract_one_chunk(
            chunks[0],
            source_file=str(pdf_path),
            extra_hints=extra_hints,
            example_rows=example_rows,
            chunk_index=1,
            total_chunks=1,
        )
        header = hdr or {}
        for r in rows:
            for k, v in header.items():
                if not r.get(k):
                    r[k] = v
            all_rows.append(r)
    else:
        # ── Multiple chunks: fire all requests in parallel ─────────
        chunk_results: Dict[int, tuple] = {}
        done_lock = threading.Lock()
        done_count = [0]

        with ThreadPoolExecutor(max_workers=min(n, 5)) as pool:
            future_to_idx = {
                pool.submit(
                    _extract_one_chunk,
                    ct,
                    source_file=str(pdf_path),
                    extra_hints=extra_hints,
                    example_rows=example_rows,
                    chunk_index=i,
                    total_chunks=n,
                ): i
                for i, ct in enumerate(chunks, 1)
            }
            for fut in as_completed(future_to_idx):
                _check_cancel(cancel_check)
                with done_lock:
                    done_count[0] += 1
                    done = done_count[0]
                if progress_cb:
                    progress_cb(f"Gemini {done}/{n} chunks terminés…")
                chunk_results[future_to_idx[fut]] = fut.result()

        for i in sorted(chunk_results):
            rows, hdr = chunk_results[i]
            if not header and hdr:
                header = hdr
            for r in rows:
                for k, v in header.items():
                    if not r.get(k):
                        r[k] = v
                all_rows.append(r)

    return all_rows


def extract_rows_from_text(
    text: str,
    *,
    source_file: str = "",
    extra_hints: str = "",
    example_rows: Optional[List[Dict]] = None,
) -> List[Dict]:
    """Single-shot extraction (used by learn_format_from_pdf on small samples).

    For large multi-page documents, prefer extract_rows_from_pdf which chunks.
    """
    rows, _ = _extract_one_chunk(
        text,
        source_file=source_file,
        extra_hints=extra_hints,
        example_rows=example_rows,
        chunk_index=1,
        total_chunks=1,
    )
    return rows


def _extract_one_chunk(
    text: str,
    *,
    source_file: str,
    extra_hints: str,
    example_rows: Optional[List[Dict]],
    chunk_index: int,
    total_chunks: int,
) -> tuple[List[Dict], Dict]:
    """Run Gemini on a single text chunk. Returns (rows, header)."""
    prompt = _build_extract_prompt(
        text=text,
        extra_hints=extra_hints,
        example_rows=example_rows,
        chunk_info=(chunk_index, total_chunks),
    )
    raw = ""
    parsed = None
    err = ""
    try:
        resp = generate_with_fallback(
            prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.1,
                "max_output_tokens": 65000,
            },
        )
        raw = getattr(resp, "text", "") or ""
        parsed = _parse_json(raw)
        rows = _flatten_to_rows(parsed, source_file=source_file)
        header = {}
        if isinstance(parsed, dict):
            header = {
                "vessel": _s(parsed.get("vessel")),
                "voyage": _s(parsed.get("voyage")),
                "date_of_arrival": _s(parsed.get("date_of_arrival")),
                "_shipowner": _s(parsed.get("shipowner")),
            }
            header = {k: v for k, v in header.items() if v}
        return rows, header
    except Exception as e:
        err = str(e)
        raise
    finally:
        debug_log.log_call(
            kind=f"extract_chunk_{chunk_index}_of_{total_chunks}",
            source_file=source_file,
            prompt=prompt,
            raw_response=raw,
            parsed=parsed,
            error=err,
            extra={
                "input_chars": len(text),
                "hints_chars": len(extra_hints),
                "chunk": chunk_index,
                "total_chunks": total_chunks,
            },
        )


def _render_first_page_png(pdf_path: Path, dpi: int = 150) -> Optional[bytes]:
    """Render page 1 of a PDF to PNG bytes for multimodal Gemini calls.

    Returns None if rendering fails (Gemini call falls back to text-only).
    """
    try:
        from pdf2image import convert_from_path
        from ..paths import poppler_bin
        import io as _io
        images = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            poppler_path=poppler_bin(),
            fmt="png",
            first_page=1,
            last_page=1,
        )
        if not images:
            return None
        buf = _io.BytesIO()
        images[0].save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception:
        return None


def _render_cargo_page_png(pdf_path: Path, ocr_text: str, dpi: int = 150) -> Optional[bytes]:
    """Render the FIRST PAGE THAT CONTAINS ACTUAL CARGO DATA to PNG.

    For many manifests (e.g. PIL) page 1 is a summary/recap with no BL
    details — showing that to the model is counter-productive. We detect
    which page starts the real cargo by looking for the first occurrence
    of a BL-number-like pattern in the OCR text and mapping it to a page.

    Falls back to page 1 if detection fails.
    """
    try:
        from pdf2image import convert_from_path
        from ..paths import poppler_bin
        import io as _io

        # Try to detect which 1-based page index starts cargo.
        cargo_page = 1
        if ocr_text:
            # Split by page markers inserted by our OCR helpers (format: "=== PAGE N ===")
            page_markers = list(re.finditer(r"=== PAGE (\d+) ===", ocr_text))
            bl_pattern = re.compile(
                r"\b[A-Z]{2,4}\d{7,12}\b"  # generic BL / container number
            )
            if page_markers:
                for m in page_markers:
                    pg = int(m.group(1))
                    start = m.end()
                    end = page_markers[page_markers.index(m) + 1].start() if page_markers.index(m) + 1 < len(page_markers) else len(ocr_text)
                    chunk = ocr_text[start:end]
                    if bl_pattern.search(chunk):
                        cargo_page = pg
                        break
            else:
                # No page markers — try to find first BL-like pattern
                # and estimate page by char offset (rough: ~3000 chars/page)
                m_bl = bl_pattern.search(ocr_text)
                if m_bl:
                    chars_before = m_bl.start()
                    cargo_page = max(1, chars_before // 3000 + 1)

        images = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            poppler_path=poppler_bin(),
            fmt="png",
            first_page=cargo_page,
            last_page=cargo_page,
        )
        if not images:
            return None
        buf = _io.BytesIO()
        images[0].save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception:
        return _render_first_page_png(pdf_path, dpi=dpi)


def _build_representative_sample(text: str, max_chars: int = 60000) -> str:
    """Build a text sample that ALWAYS includes real cargo BL blocks.

    Plain text[:N] fails when page 1 is a summary/recap — the model never
    sees what the BL blocks look like. This helper:
    1. Keeps the first 2000 chars (document header / voyage info).
    2. Finds the first 3 complete BL blocks and includes them verbatim.
    3. Fills the rest of the budget with consecutive text after those blocks.

    Returns at most `max_chars` characters.
    """
    if not text or len(text) <= max_chars:
        return text or ""

    # 1) Header slice (vessel/voyage info is usually in the first chars)
    header_slice = text[:2000]

    # 2) Find BL blocks — look for the first occurrence of a BL-number-like
    #    pattern surrounded by word boundaries.
    bl_pattern = re.compile(r"\b([A-Z]{2,4}\d{9,12})\b")
    matches = list(bl_pattern.finditer(text))
    if not matches:
        return text[:max_chars]

    # Take positions of first 3 distinct BL numbers
    seen_bls: set = set()
    anchor_positions = []
    for m in matches:
        bl = m.group(1)
        if bl not in seen_bls:
            seen_bls.add(bl)
            anchor_positions.append(m.start())
        if len(anchor_positions) >= 3:
            break

    # Start sample from the first BL anchor (skip the recap page)
    first_cargo_offset = anchor_positions[0]
    # Back up slightly to catch whatever marker precedes the BL number
    cargo_start = max(0, first_cargo_offset - 200)

    # We want header + a gap indicator + cargo text
    cargo_budget = max_chars - len(header_slice) - 100
    cargo_text = text[cargo_start: cargo_start + cargo_budget]

    gap = "\n\n[... début du manifeste, pages de résumé omises ...]\n\n" if cargo_start > 2200 else ""

    return (header_slice + gap + cargo_text)[:max_chars]


# Set of row keys that, when systematically empty across EVERY produced row,
# almost always indicate the parser missed an extraction step (vs. data that
# truly does not exist in the document). Used by the self-refinement round.
_AUDITED_ROW_KEYS = (
    "bl_number", "container_number", "container_type",
    "weight", "pack_qty", "volume",
    "shipper", "consignee", "notify",
    "port_of_loading", "port_of_discharge", "place_of_delivery",
    "seal1",
)


def _audit_rows_for_gaps(rows: List[Dict], full_text: str) -> List[str]:
    """Find row keys that are ALWAYS empty but for which the source text
    contains plausible evidence of a value. Returns a list of human-readable
    diagnostic lines to feed back to the model.
    """
    if not rows:
        return []
    diagnostics: List[str] = []
    text_lower = (full_text or "").lower()
    # 1) Always-empty keys
    for key in _AUDITED_ROW_KEYS:
        non_empty = sum(
            1 for r in rows
            if r.get(key) not in (None, "", 0, 0.0)
        )
        if non_empty == 0:
            # Only flag if the source text contains plausible evidence.
            evidence_hints = {
                "port_of_loading":   ("port of loading", "port of loadi", "5a.port", "pol "),
                "port_of_discharge": ("port of discharge", "5b.port", "pod "),
                "place_of_delivery": ("place of delivery", "final destination", "destination"),
                "shipper":           ("shipper", "shippe r"),
                "consignee":         ("consignee",),
                "notify":            ("notify",),
                "seal1":             ("seal", "(sn)", "sn:"),
                "weight":            ("kgs", "kg ", "weight"),
                "volume":            ("cbm", "m3", "volume"),
                "pack_qty":          ("pkg", "pkgs", "package", "carton", "bag", "bale"),
                "container_type":    ("container", "20'", "40'", "20gp", "40gp", "40hc"),
            }
            hints = evidence_hints.get(key, ())
            has_evidence = any(h in text_lower for h in hints) if hints else True
            if has_evidence:
                diagnostics.append(
                    f"- `{key}` est VIDE sur les {len(rows)} lignes alors que "
                    f"le texte source contient des marqueurs comme "
                    f"{', '.join(repr(h) for h in hints[:3])}."
                )
    # 2) Suspicious uniform values (same weight per container = the
    #    "weight divided across containers" mistake).
    weights = [r.get("weight") for r in rows if r.get("weight") is not None]
    if len(weights) >= 4:
        # Group by bl_number; if within the same BL all containers share
        # exactly the same fractional weight, the parser likely divided.
        from collections import defaultdict
        by_bl: Dict = defaultdict(list)
        for r in rows:
            if r.get("bl_number") and r.get("weight") is not None:
                by_bl[r["bl_number"]].append(r["weight"])
        suspect = 0
        for bl, ws in by_bl.items():
            if len(ws) >= 2 and len(set(round(float(w), 3) for w in ws)) == 1:
                # All containers of this BL have identical fractional weight
                if any(float(w) != int(float(w)) for w in ws):
                    suspect += 1
        if suspect >= 2:
            diagnostics.append(
                f"- Le poids est DIVISE de maniere identique entre les "
                f"conteneurs d'un meme BL ({suspect} BL concernes). Verifie "
                f"si le poids dans le texte est un TOTAL par BL (a garder "
                f"tel quel sur chaque conteneur, ou a mettre seulement sur "
                f"le 1er) plutot qu'a diviser."
            )
    return diagnostics


def _self_refine_winner(
    *, winner: Dict, full_text: str, sample: str, page1_png,
    providers, build_prompt, ask_provider, validate_code,
    progress_cb=None, cancel_check=None,
):
    """Audit the winner's output, and if gaps are detected, ask the model
    to patch its own parser using the FULL 16-page context as ground truth.

    Returns the new winner dict if the refined version is strictly better
    (more rows OR same rows with fewer empty-field gaps), else None.
    """
    from .template_parser import run_parse_code

    code = winner.get("code") or ""
    if not code:
        return None

    rows = run_parse_code(code, full_text)
    if not rows:
        return None

    diagnostics = _audit_rows_for_gaps(rows, full_text)
    if not diagnostics:
        if progress_cb:
            progress_cb("Auto-verification : aucun defaut detecte, parser conserve.")
        return None

    if progress_cb:
        progress_cb(
            f"Auto-verification : {len(diagnostics)} defaut(s) detecte(s) — "
            f"raffinement en cours…"
        )

    # Sample 5 rows to show the model what its parser produced.
    sample_rows = rows[:5]
    sample_rows_json = json.dumps(sample_rows, ensure_ascii=False, indent=2)

    refine_feedback = (
        "\n\n=========================================================\n"
        "*** ROUND 2 — AUTO-VERIFICATION & CORRECTION CIBLEE ***\n"
        "=========================================================\n"
        "Ton parser a tourne sur le DOCUMENT COMPLET et a produit "
        f"{len(rows)} lignes. En auditant la sortie, on a detecte les\n"
        "defauts suivants (a corriger sans regression sur le reste) :\n\n"
        + "\n".join(diagnostics)
        + "\n\nVoici un ECHANTILLON des lignes produites par TON parser :\n"
        "```json\n" + sample_rows_json + "\n```\n\n"
        "Le TEXTE source ci-dessous est la VERITE TERRAIN — il contient\n"
        "les valeurs que ton parser a manquees. Relis-le, repere les\n"
        "marqueurs reels (ex: `5a.Port of loading ABIDJAN`, `(SN)`,\n"
        "`Final destination`), puis CORRIGE ta fonction `parse()` pour\n"
        "extraire ces champs manquants. GARDE intacte la logique qui\n"
        "fonctionne deja (decoupe par BL, conteneurs, etc.).\n"
        "RE-EMETS la reponse complete au format <<<JSON>>>...<<<PYTHON>>>.\n"
        "=========================================================\n"
    )

    # Always retry with Gemini (best at multimodal context).
    refine_prov = next((p for p in providers if p.id == "gemini"), providers[0])
    _check_cancel(cancel_check)
    meta_r, code_r, _, err_r = ask_provider(refine_prov, refine_feedback)
    if not code_r:
        if progress_cb:
            progress_cb(f"Auto-verification : echec du raffinement ({err_r or 'pas de code'}).")
        return None
    ok_r, n_rows_r, why_r = validate_code(code_r)
    if not (ok_r and n_rows_r > 0):
        if progress_cb:
            progress_cb(f"Auto-verification : raffinement invalide ({why_r}), parser original conserve.")
        return None

    # Compare gap count: refined version must close at least one gap
    # without losing rows.
    rows_r = run_parse_code(code_r, full_text)
    diags_r = _audit_rows_for_gaps(rows_r, full_text)
    closed_gaps = max(0, len(diagnostics) - len(diags_r))
    row_loss = max(0, len(rows) - len(rows_r))

    # Acceptance rule: must close gaps AND keep at least 95% of rows.
    if closed_gaps >= 1 and len(rows_r) >= int(0.95 * len(rows)):
        if progress_cb:
            progress_cb(
                f"Auto-verification : OK — {closed_gaps} defaut(s) corrige(s), "
                f"{len(rows_r)} ligne(s) (vs {len(rows)})."
            )
        return {
            "provider": refine_prov.id,
            "model": refine_prov.model,
            "meta": meta_r or winner.get("meta") or {},
            "code": code_r,
            "ok": True,
            "n_rows": n_rows_r,
            "reason": "",
            "error": "",
        }

    if progress_cb:
        progress_cb(
            f"Auto-verification : raffinement rejete (gaps fermes={closed_gaps}, "
            f"perte={row_loss} lignes), parser original conserve."
        )
    return None


def learn_format_from_pdf(
    pdf_path: str | Path,
    *,
    cancel_check=None,
    extra_feedback: str = "",
    progress_cb=None,
    ocr_engine: str = "auto",
    previous_code: str = "",
) -> Dict:
    """Learn a manifest format. Staged pipeline (cheap by default).

    Pipeline
    --------
    1. ``extract_fingerprint`` (local, no LLM) → DocumentFingerprint
    2. ``select_learning_strategy`` → DISCOVER_ENSEMBLE | UPGRADE | PATCH
    3. Single LLM call (Flash) for UPGRADE/PATCH, or parallel ensemble
       for first-time discovery, with a compact prompt (~3.5 KB).
    4. Local validation of the returned ``spatial_template`` against the
       fingerprint sample blocks ; deterministic fallback rules patch
       missing fields without another LLM call.

    Set the env var ``AGL_LEGACY_LEARN=1`` to force the old monolithic
    pipeline (kept as a safety hatch).
    """
    import os as _os
    if _os.environ.get("AGL_LEGACY_LEARN") == "1":
        return _learn_format_from_pdf_legacy(
            pdf_path,
            cancel_check=cancel_check,
            extra_feedback=extra_feedback,
            progress_cb=progress_cb,
            ocr_engine=ocr_engine,
            previous_code=previous_code,
        )
    try:
        return _learn_format_from_pdf_staged(
            pdf_path,
            cancel_check=cancel_check,
            extra_feedback=extra_feedback,
            progress_cb=progress_cb,
            ocr_engine=ocr_engine,
            previous_code=previous_code,
        )
    except Exception as e:
        # Hard safety net : if the new pipeline crashes for any reason,
        # keep the user productive by falling back to the legacy path.
        if progress_cb:
            progress_cb(f"Pipeline staged en echec ({e}) — bascule legacy.")
        return _learn_format_from_pdf_legacy(
            pdf_path,
            cancel_check=cancel_check,
            extra_feedback=extra_feedback,
            progress_cb=progress_cb,
            ocr_engine=ocr_engine,
            previous_code=previous_code,
        )


# ────────────────────────────────────────────────────────────────────────
# Staged learning pipeline (cheap by default — see plan)
# ────────────────────────────────────────────────────────────────────────
class LearningStrategy:
    DISCOVER_ENSEMBLE = "discover"
    UPGRADE_SINGLE = "upgrade"
    PATCH_SINGLE = "patch"


_LEARN_SPATIAL_PROMPT = """Tu CONFIGURES un moteur d'extraction spatial deja
implemente. Tu ne programmes PAS. Tu retournes UNIQUEMENT un JSON
SpatialTemplate. Aucune explication, aucun bloc Python.

{facts}

EXEMPLES DE BLOCS BL REELS (extraits du document):
{samples}

REPONDS EXACTEMENT DANS CE FORMAT (rien avant, rien apres) :
<<<JSON>>>
{{
  "format_name":   "MAJUSCULES (ex: PIL, MSC, CMA_CGM)",
  "carrier_name": "nom complet du transporteur",
  "signature_keywords": ["3-6 tokens specifiques au document"],
  "is_scanned": {is_scanned_lit},
  "shipowner": "valeur litterale",
  "header_field_patterns": {{
    "vessel":            "regex avec UN groupe (...)",
    "voyage":            "regex avec UN groupe (...)",
    "date_of_arrival":   "regex avec UN groupe (...)",
    "port_of_loading":   "regex avec UN groupe (...) ou \\"\\"",
    "port_of_discharge": "regex avec UN groupe (...) ou \\"\\""
  }},
  "spatial_template": {{
    "bl_marker_pattern":  "regex BL (un groupe). PRENDS la valeur pre-detectee si fournie.",
    "bl_split_strategy":  "marker",
    "container_marker":   "regex avec UN groupe pour le n° conteneur (ou \\"\\" si vrac).",
    "weight_scope":       "PRENDS la valeur pre-detectee. NE LA CHANGE PAS.",
    "field_rules": [
      {{"field_name": "shipper",           "anchor_text": ["SHIPPER"],   "direction": "below", "max_distance": 80,  "stop_at": ["CONSIGNEE","NOTIFY"], "scope": "bl_block"}},
      {{"field_name": "consignee",         "anchor_text": ["CONSIGNEE"], "direction": "below", "max_distance": 80,  "stop_at": ["NOTIFY","DESCRIPTION"], "scope": "bl_block"}},
      {{"field_name": "port_of_loading",   "anchor_text": ["Port","of","loading"],   "direction": "right", "max_distance": 250, "scope": "page"}},
      {{"field_name": "port_of_discharge", "anchor_text": ["Port","of","discharge"], "direction": "right", "max_distance": 250, "scope": "page"}},
      {{"field_name": "weight",            "anchor_text": ["WEIGHT"],    "direction": "right", "max_distance": 150, "scope": "bl_block"}}
    ]
  }}
}}
<<<END_JSON>>>

REGLES IMPERATIVES :
- bl_marker_pattern, container_marker, weight_scope : reprends EXACTEMENT
  les valeurs pre-detectees ci-dessus si elles sont fournies. Ne les
  remets PAS en cause — elles ont ete validees par comptage sur le
  document complet.
- field_rules : au minimum shipper, consignee, weight, port_of_loading,
  port_of_discharge. Ajoute volume / pack_qty / seals / notify si visibles
  dans les blocs exemples.
- direction = "below" si la valeur est SOUS le label (typique SHIPPER /
  CONSIGNEE multi-lignes). "right" si A DROITE (typique VESSEL: XXX).
- scope = "bl_block" pour les champs qui varient par BL (shipper, weight,
  ports specifiques au BL). scope = "page" pour les champs document-niveau
  (vessel, voyage, date).
- N'INVENTE rien : si un champ n'est pas dans les blocs exemples, ne
  l'inclus pas. Mieux vaut une regle manquante (le moteur fera fallback)
  qu'une regle erronee.
"""


_LEARN_PATCH_PROMPT = """PATCH d'une regle SpatialRule existante (PAS un
re-apprentissage complet). Le moteur d'extraction tourne deja localement.

{facts}

REGLE ACTUELLE A CORRIGER :
{current_rule}

{evidence}

REPONDS UNIQUEMENT avec le JSON de la SpatialRule corrigee, dans ce format
exact (rien avant, rien apres) :
<<<JSON>>>
{{
  "field_name":   "...",
  "anchor_text":  ["..."],
  "direction":    "right | below | above",
  "max_distance": 200,
  "x_tolerance":  40,
  "y_tolerance":  4,
  "max_words":    8,
  "stop_at":      [],
  "scope":        "page | bl_block"
}}
<<<END_JSON>>>

Tu CORRIGES UNE regle. Ne renvoie pas un template complet.
"""


def select_learning_strategy(
    fingerprint, existing_format: Optional[Dict],
) -> str:
    """Pick the cheapest viable strategy."""
    if not existing_format:
        return LearningStrategy.DISCOVER_ENSEMBLE
    pt = (existing_format.get("parse_template") or {})
    has_spatial = bool(pt.get("spatial_template") or pt.get("spatial_rules"))
    has_feedback_diffs = any(
        bool(f.get("diffs"))
        for f in (existing_format.get("feedback") or [])
    )
    if has_feedback_diffs:
        return LearningStrategy.PATCH_SINGLE
    if not has_spatial:
        return LearningStrategy.UPGRADE_SINGLE
    return LearningStrategy.DISCOVER_ENSEMBLE


def _validate_spatial_template(template_dict: Dict, fingerprint) -> Dict:
    """Run the spatial parser on the fingerprint sample text and report
    coverage. Pure-local — no LLM call.
    """
    out = {"ok": False, "rows": 0, "missing_fields": [], "error": ""}
    try:
        from ..spatial_template import SpatialTemplate, is_usable
        from ..spatial_parser import parse_with_spatial_template
    except Exception as e:
        out["error"] = f"import: {e}"
        return out
    try:
        st = SpatialTemplate.from_dict(template_dict)
    except Exception as e:
        out["error"] = f"from_dict: {e}"
        return out
    if not is_usable(st):
        out["error"] = "template inutilisable (pas de field_rules ni bl_marker_pattern)"
        return out
    try:
        sample_text = "\n=== PAGE 1 ===\n" + "\n\n".join(fingerprint.sample_blocks)
        rows = parse_with_spatial_template(
            fingerprint.pdf_path, st,
            ocr_text=sample_text if fingerprint.is_scanned else None,
        )
    except Exception as e:
        out["error"] = f"parse: {e}"
        return out
    out["rows"] = len(rows)
    expected = max(1, fingerprint.total_bls)
    out["ok"] = len(rows) >= max(1, int(expected * 0.5))
    if rows:
        # Identify fields that NEVER appeared in any row.
        important = ("shipper", "consignee", "weight",
                     "port_of_loading", "port_of_discharge")
        out["missing_fields"] = [
            f for f in important
            if not any((r.get(f) or "") for r in rows)
        ]
    return out


# Deterministic anchor variants for known fields. Used by
# ``_apply_fallback_rules`` when the LLM omitted a critical rule.
_FALLBACK_ANCHORS = {
    "port_of_loading": [
        ["Port", "of", "loading"], ["POL"], ["5a.Port"],
        ["Port", "chargement"], ["Lieu", "embarquement"],
    ],
    "port_of_discharge": [
        ["Port", "of", "discharge"], ["POD"], ["5b.Port"],
        ["Port", "dechargement"], ["Lieu", "debarquement"],
    ],
    "shipper":   [["SHIPPER"], ["EXPEDITEUR"], ["FROM"]],
    "consignee": [["CONSIGNEE"], ["DESTINATAIRE"], ["TO"]],
    "weight":    [["WEIGHT"], ["POIDS"], ["GROSS", "WEIGHT"], ["KGS"]],
}


def _apply_fallback_rules(template_dict: Dict, fingerprint) -> Dict:
    """Inject deterministic SpatialRule entries for any critical field
    that the LLM omitted. No LLM call.
    """
    spatial = template_dict.setdefault("spatial_template", {}) \
        if "spatial_template" in template_dict else template_dict
    rules = spatial.setdefault("field_rules", [])
    have = {r.get("field_name") for r in rules if isinstance(r, dict)}
    for field_name, anchor_lists in _FALLBACK_ANCHORS.items():
        if field_name in have:
            continue
        # Use the first variant ; the engine itself does fuzzy matching.
        rules.append({
            "field_name": field_name,
            "anchor_text": anchor_lists[0],
            "direction": "right" if field_name.startswith("port") else "below",
            "max_distance": 250 if field_name.startswith("port") else 80,
            "x_tolerance": 60,
            "y_tolerance": 4,
            "max_words": 12,
            "stop_at": ["CONSIGNEE", "NOTIFY", "DESCRIPTION"]
                       if field_name in ("shipper", "consignee") else [],
            "scope": "page" if field_name.startswith("port") else "bl_block",
        })
    # Force weight_scope from the deterministic detector.
    spatial["weight_scope"] = fingerprint.weight_scope
    return template_dict


def _build_spatial_prompt(fingerprint) -> str:
    samples = "\n--- BLOC ---\n".join(fingerprint.sample_blocks) or "(aucun bloc detecte)"
    return _LEARN_SPATIAL_PROMPT.format(
        facts=fingerprint.to_prompt_facts(),
        samples=samples,
        is_scanned_lit="true" if fingerprint.is_scanned else "false",
    )


def _build_patch_prompt(
    fingerprint, current_rule: Dict, evidence: str,
) -> str:
    return _LEARN_PATCH_PROMPT.format(
        facts=fingerprint.to_prompt_facts(),
        current_rule=json.dumps(current_rule, indent=2, ensure_ascii=False),
        evidence=evidence or "(pas d'evidence spatiale)",
    )


def _parse_spatial_template_response(raw: str) -> Optional[Dict]:
    if not raw:
        return None
    m = re.search(r"<<<JSON>>>\s*(\{[\s\S]*?\})\s*(?:<<<END_JSON>>>|$)", raw)
    chunk = m.group(1) if m else raw
    try:
        return json.loads(chunk)
    except Exception:
        return _parse_json(chunk)


def _read_existing_format(name: str) -> Optional[Dict]:
    if not name:
        return None
    try:
        from .format_registry import list_learned, _slug  # type: ignore
        slug = _slug(name)
        for fmt in list_learned():
            if fmt.get("name") == name or _slug(fmt.get("name", "")) == slug:
                return fmt
    except Exception:
        pass
    return None


def _learn_format_from_pdf_staged(
    pdf_path: str | Path,
    *,
    cancel_check=None,
    extra_feedback: str = "",
    progress_cb=None,
    ocr_engine: str = "auto",
    previous_code: str = "",
) -> Dict:
    """The new cheap pipeline. Falls back to legacy on hard failure."""
    from . import llm_providers
    from .document_fingerprint import extract_fingerprint

    pdf_path = Path(pdf_path)
    _check_cancel(cancel_check)

    # ── OCR upfront only if the PDF is scanned ──────────────────
    digital_text = _read_pdf_text(pdf_path, max_pages=6)
    is_scanned = len(digital_text.strip()) < 200
    ocr_text: Optional[str] = None
    if is_scanned:
        if progress_cb:
            progress_cb("Document scanne — OCR en cours…")
        ocr_max_pages = 30
        if ocr_engine == "vision":
            ocr_text = vision_client.ocr_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR Cloud Vision page {d}/{n}…") if progress_cb else None
                ),
            )
        elif ocr_engine == "local":
            ocr_text = vision_client.local_ocr_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR local page {d}/{n}…") if progress_cb else None
                ),
            )
        else:
            ocr_text = vision_client.ocr_scanned_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR page {d}/{n}…") if progress_cb else None
                ),
            )

    # ── Stage 1 : structural fingerprint ─────────────────────────
    if progress_cb:
        progress_cb("Stage 1/4 : empreinte structurelle locale…")
    fp = extract_fingerprint(pdf_path, ocr_text=ocr_text, is_scanned=is_scanned)
    if progress_cb:
        progress_cb(
            f"  -> {fp.total_bls} BL, {fp.total_containers} conteneurs, "
            f"weight_scope={fp.weight_scope}, "
            f"bl_pattern={fp.best_bl_pattern!r} ({fp.best_bl_hits}x)"
        )

    # ── Stage 2 : strategy selection ────────────────────────────
    existing_format = _read_existing_format(
        fp.format_hint or Path(pdf_path).stem
    )
    strategy = select_learning_strategy(fp, existing_format)
    if progress_cb:
        progress_cb(f"Stage 2/4 : strategie = {strategy}")

    prompt = _build_spatial_prompt(fp)
    if progress_cb:
        size_msg = f"prompt={len(prompt):,} chars"
        if fp.page1_image:
            size_msg += f" + image ({len(fp.page1_image):,} octets)"
        progress_cb(f"Stage 3/4 : appel LLM ({size_msg})")

    # ── Stage 3 : LLM call ──────────────────────────────────────
    raw = ""
    used_provider = ""
    if strategy == LearningStrategy.DISCOVER_ENSEMBLE:
        providers = llm_providers.configured_learning_providers()
        if not providers:
            raise RuntimeError("Aucun fournisseur IA configure.")
        # Parallel call but with the COMPACT prompt — payload is small
        # enough that the cost is now acceptable.
        results: List[Dict] = []
        with ThreadPoolExecutor(max_workers=len(providers)) as pool:
            futs = {}
            for p in providers:
                img = fp.page1_image if p.supports_vision else None
                futs[pool.submit(
                    p.generate, prompt,
                    image_bytes=img, max_tokens=4000, temperature=0.0,
                )] = p
            for fut in as_completed(futs):
                _check_cancel(cancel_check)
                p = futs[fut]
                try:
                    resp = fut.result()
                    results.append({"provider": p, "raw": resp.text or ""})
                except Exception as e:
                    if progress_cb:
                        progress_cb(f"  • {p.display_name} ERREUR : {e}")
        # Pick the first response that contains a parseable spatial_template.
        for r in results:
            tpl = _parse_spatial_template_response(r["raw"])
            if tpl and (tpl.get("spatial_template") or {}).get("field_rules"):
                raw = r["raw"]
                used_provider = r["provider"].id
                break
        if not raw and results:
            raw = results[0]["raw"]
            used_provider = results[0]["provider"].id
    else:
        # UPGRADE / PATCH : single Gemini Flash call (fall back to Pro).
        try:
            resp = llm_providers.call_single(
                "gemini_flash", prompt,
                image_bytes=fp.page1_image,
                max_tokens=4000,
            )
            used_provider = "gemini_flash"
        except llm_providers.LLMError:
            resp = llm_providers.call_single(
                "gemini", prompt,
                image_bytes=fp.page1_image,
                max_tokens=4000,
            )
            used_provider = "gemini"
        raw = resp.text or ""

    if progress_cb:
        progress_cb(f"  -> reponse {len(raw):,} chars (via {used_provider})")

    meta = _parse_spatial_template_response(raw) or {}
    debug_log.log_call(
        kind=f"learn_staged:{used_provider}",
        source_file=str(pdf_path),
        prompt=prompt, raw_response=raw, parsed=meta,
        extra={"strategy": strategy, "fingerprint": {
            "total_bls": fp.total_bls,
            "total_containers": fp.total_containers,
            "weight_scope": fp.weight_scope,
            "is_scanned": fp.is_scanned,
        }},
    )

    if not meta:
        raise RuntimeError("LLM: reponse non parsable (pas de bloc JSON).")

    # ── Stage 4 : local validation + fallback rules ─────────────
    if progress_cb:
        progress_cb("Stage 4/4 : validation locale + regles fallback…")
    spatial_descriptor = meta.get("spatial_template") or {}
    # Force the deterministically-detected facts (LLM cannot override).
    if fp.best_bl_pattern and not spatial_descriptor.get("bl_marker_pattern"):
        spatial_descriptor["bl_marker_pattern"] = fp.best_bl_pattern
    if fp.weight_scope:
        spatial_descriptor["weight_scope"] = fp.weight_scope
    spatial_descriptor.setdefault("bl_split_strategy", "marker")
    # Inject fallback rules where the LLM omitted critical fields.
    spatial_descriptor = _apply_fallback_rules(spatial_descriptor, fp)
    meta["spatial_template"] = spatial_descriptor

    validation = _validate_spatial_template(spatial_descriptor, fp)
    if progress_cb:
        progress_cb(
            f"  -> validation: {validation['rows']} ligne(s) sur sample, "
            f"missing={validation['missing_fields']}"
        )

    parsed: Dict = dict(meta)
    parsed["is_scanned"] = fp.is_scanned
    parsed["parse_template"] = {
        "header_field_patterns": (meta.get("header_field_patterns") or {}),
        "parse_code": "",   # spatial-only path
        "shipowner": (meta.get("shipowner") or fp.format_hint or ""),
        "row_count": validation["rows"],
        "spatial_template": spatial_descriptor,
    }
    parsed["sample_text"] = "\n\n".join(fp.sample_blocks)[:4000]
    parsed.setdefault("example_rows", [])
    parsed["_local_row_count_on_sample"] = validation["rows"]
    parsed["_winner_provider"] = used_provider
    parsed["_winner_model"] = used_provider
    parsed["_strategy"] = strategy
    parsed["_fingerprint"] = {
        "total_bls": fp.total_bls,
        "total_containers": fp.total_containers,
        "weight_scope": fp.weight_scope,
        "format_hint": fp.format_hint,
    }
    return parsed


# ────────────────────────────────────────────────────────────────────────
# Legacy pipeline (kept as fallback — see AGL_LEGACY_LEARN env var)
# ────────────────────────────────────────────────────────────────────────
def _learn_format_from_pdf_legacy(
    pdf_path: str | Path,
    *,
    cancel_check=None,
    extra_feedback: str = "",
    progress_cb=None,
    ocr_engine: str = "auto",
    previous_code: str = "",
) -> Dict:
    """Learn a manifest format from a sample PDF using ENSEMBLE LLM voting.

    `ocr_engine` : "auto" (smart dispatch) | "vision" (Cloud Vision only) |
                   "local" (Tesseract only). Only used when the PDF has no
                   embedded text (scanned document).
    `previous_code` : optional Python source of the parser produced by a
                   previous learning round. Shown to the model as a "diff
                   reference" so it can apply user feedback as a fix to a
                   known baseline rather than starting from scratch.
    """
    from .template_parser import run_parse_code
    from . import llm_providers

    pdf_path = Path(pdf_path)
    _check_cancel(cancel_check)

    # Try embedded text first. If document is scanned, fall back to OCR.
    digital_text = _read_pdf_text(pdf_path, max_pages=6)
    is_scanned = len(digital_text.strip()) < 200
    if is_scanned:
        if progress_cb:
            progress_cb("Document scanné — OCR en cours (peut prendre du temps)…")
        # OCR cap: 30 pages is a sweet spot — large enough to give the AI
        # several real cargo lines for pattern induction, small enough to
        # keep the call under a minute on parallel Tesseract.
        ocr_max_pages = 30
        if ocr_engine == "vision":
            text = vision_client.ocr_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR Cloud Vision page {d}/{n}…") if progress_cb else None
                ),
            )
        elif ocr_engine == "local":
            text = vision_client.local_ocr_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR local page {d}/{n}…") if progress_cb else None
                ),
            )
        else:  # "auto"
            text = vision_client.ocr_scanned_pdf(
                pdf_path, max_pages=ocr_max_pages, cancel_check=cancel_check,
                progress_cb=lambda d, n: (
                    progress_cb(f"OCR page {d}/{n}…") if progress_cb else None
                ),
            )
        full_text = text
        debug_log.log_call(
            kind="learn_ocr",
            source_file=str(pdf_path),
            ocr_text=text,
            extra={"chars": len(text), "engine": ocr_engine, "pages": ocr_max_pages},
        )
    else:
        text = digital_text
        try:
            full_text = _read_pdf_text(pdf_path, max_pages=None)
        except Exception:
            full_text = text
        if len(full_text.strip()) < len(text.strip()):
            full_text = text

    # Build a SMART sample: always start from the first BL cargo block,
    # not page 1 (which may be a summary/recap with no BL data at all).
    sample = _build_representative_sample(text, max_chars=60000)
    # Render the first page that actually contains cargo data (not page 1
    # recap) so Gemini's visual context matches the text it will parse.
    page1_png = _render_cargo_page_png(pdf_path, text)
    if progress_cb and page1_png:
        progress_cb(f"Image cargo page rendue ({len(page1_png):,} octets).")

    def _split_response(raw: str) -> tuple[Dict, str]:
        """Split AI response into (metadata_dict, python_code)."""
        meta: Dict = {}
        code = ""
        if not raw:
            return meta, code
        m_json = re.search(
            r"<<<JSON>>>\s*(\{[\s\S]*?\})\s*(?:<<<END_JSON>>>|<<<PYTHON>>>|$)",
            raw,
        )
        if m_json:
            try:
                meta = json.loads(m_json.group(1))
            except Exception:
                meta = _parse_json(m_json.group(1)) or {}
        else:
            meta = _parse_json(raw) or {}
        m_py = re.search(
            r"<<<PYTHON>>>\s*(?:```(?:python)?\s*)?([\s\S]*?)(?:```\s*)?<<<END_PYTHON>>>",
            raw,
        )
        if not m_py:
            m_py = re.search(r"<<<PYTHON>>>\s*(?:```(?:python)?\s*)?([\s\S]+?)$", raw)
        if m_py:
            code = m_py.group(1).strip()
            code = re.sub(r"\s*```\s*$", "", code).strip()
        return meta, code

    def _validate_code(code: str) -> tuple[bool, int, str]:
        """Compile + run on FULL document. Returns (ok, n_rows, reason)."""
        if not code or "def parse" not in code:
            return False, 0, "no_parse_fn"
        try:
            compile(code, "<learned_parser>", "exec")
        except SyntaxError:
            return False, 0, "syntax"
        rows = run_parse_code(code, full_text)
        if not rows:
            return True, 0, "zero_rows"
        return True, len(rows), ""

    def _build_prompt(extra: str = "") -> str:
        scan_warning = ""
        if is_scanned:
            scan_warning = (
                "\n\n*** ATTENTION : ce document est un SCAN (pas de texte natif). "
                "Le TEXTE (B) ci-dessous provient de l'OCR (Cloud Vision). "
                "Il peut contenir des fautes OCR : I/1, O/0, S/5, B/8, Z/2, etc. "
                "Ton parser doit etre TOLERANT aux fautes OCR. ***\n"
            )
        feedback_block = ""
        if extra_feedback and extra_feedback.strip():
            feedback_block = (
                "\n\n=========================================================\n"
                "*** FEEDBACK UTILISATEUR — IMPERATIF, A APPLIQUER ABSOLUMENT ***\n"
                "=========================================================\n"
                "L'utilisateur a deja teste un parser precedent et identifie\n"
                "les erreurs ci-dessous. Le NOUVEAU parser DOIT corriger ces\n"
                "erreurs SANS REGRESSION. Lis chaque point, comprends la cause,\n"
                "et adapte ta logique en consequence. NE TE CONTENTE PAS de\n"
                "regenerer un parser similaire — change ce qu'il faut changer.\n\n"
                + extra_feedback.strip()
                + "\n=========================================================\n"
                "*** FIN FEEDBACK — APPLIQUE-LE DANS TON CODE ***\n"
                "=========================================================\n"
            )
        previous_code_block = ""
        if previous_code and previous_code.strip():
            previous_code_block = (
                "\n\n=========================================================\n"
                "*** PARSER PRECEDENT (a corriger selon le feedback) ***\n"
                "=========================================================\n"
                "Voici le parser genere lors de la session precedente. Utilise-le\n"
                "comme BASE et applique les corrections demandees par l'utilisateur.\n"
                "Ne repars pas de zero si la base est correcte sur les autres champs.\n\n"
                "```python\n" + previous_code.strip() + "\n```\n"
                "=========================================================\n"
            )
        return (
            _LEARN_COMBINED_INSTRUCTIONS
            + scan_warning
            + feedback_block
            + previous_code_block
            + extra
            + "\n\n--- TEXTE DU MANIFESTE ("
            + ("OCR Cloud Vision" if is_scanned else "extrait par pdfplumber")
            + ", multi-pages) ---\n"
            + sample
            + "\n--- FIN TEXTE ---\n"
            + ("\n(L'IMAGE de la page 1 est jointe ci-dessus pour le contexte spatial.)\n"
               if page1_png else "")
        )

    def _ask_provider(provider, extra: str = "") -> tuple[Dict, str, str, str]:
        """Call ONE provider. Returns (meta, code, raw, error)."""
        if progress_cb:
            tag = "[Round 2]" if extra else "[Round 1]"
            progress_cb(f"  → {tag} appel a {provider.display_name} ({provider.model})…")
        prompt = _build_prompt(extra)
        if progress_cb:
            progress_cb(
                f"     prompt={len(prompt):,} chars"
                + (f" + image page1 ({len(page1_png)} octets)" if (page1_png and provider.supports_vision) else "")
            )
        raw = ""
        meta: Dict = {}
        code = ""
        err = ""
        import time as _time
        t0 = _time.time()
        try:
            img = page1_png if provider.supports_vision else None
            resp = provider.generate(
                prompt,
                image_bytes=img,
                max_tokens=16384,
                temperature=0.0,
                json_mode=False,
            )
            raw = resp.text or ""
            meta, code = _split_response(raw)
        except Exception as e:
            err = str(e)
        finally:
            elapsed = _time.time() - t0
            if progress_cb:
                if err:
                    progress_cb(f"     ← {provider.display_name} ERREUR ({elapsed:.1f}s) : {err[:160]}")
                else:
                    fmt_name = (meta or {}).get("format_name") or "?"
                    progress_cb(
                        f"     ← {provider.display_name} reponse {len(raw):,} chars en {elapsed:.1f}s "
                        f"(code={len(code):,} chars, format='{fmt_name}')"
                    )
            debug_log.log_call(
                kind=f"learn:{provider.id}",
                source_file=str(pdf_path),
                prompt=prompt, raw_response=raw,
                parsed={"meta": meta, "code_chars": len(code)},
                error=err,
                extra={
                    "provider": provider.id,
                    "model": provider.model,
                    "image_attached": bool(page1_png and provider.supports_vision),
                },
            )
        return meta, code, raw, err

    def _diagnose_failure(code: str, raw: str, why: str) -> str:
        """Build a richer human-readable reason for a failed parser."""
        if why == "no_parse_fn":
            snippet = (raw or "").strip().replace("\n", " ⏎ ")[:140]
            return f"reponse sans 'def parse' (debut: {snippet!r})" if snippet else "reponse vide"
        if why == "syntax":
            try:
                compile(code, "<learned_parser>", "exec")
            except SyntaxError as se:
                return f"erreur de syntaxe ligne {se.lineno}: {se.msg}"
            return "erreur de syntaxe"
        if why == "zero_rows":
            # Find anchor markers the model tried to use, so we know WHY
            # it matched nothing.
            anchors = []
            for m in re.finditer(r"re\.(?:search|finditer|findall|split)\(\s*r?[\"']([^\"']{2,80})[\"']", code or ""):
                anchors.append(m.group(1))
            anchors = anchors[:4]
            anchor_txt = ", ".join(repr(a) for a in anchors) if anchors else "(aucune regex trouvee)"
            return f"parser execute mais 0 ligne extraite — ancres regex testees: {anchor_txt}"
        return why or "?"

    providers = llm_providers.configured_learning_providers()
    if not providers:
        raise RuntimeError(
            "Aucun fournisseur IA configure pour l'apprentissage.\n"
            "Menu IA -> Configuration : ajoutez au moins une cle API."
        )

    # ── Round 1: query every provider in parallel ─────────────────
    if progress_cb:
        names = ", ".join(p.display_name for p in providers)
        progress_cb(f"Apprentissage par {len(providers)} modele(s) IA : {names}…")
    results: List[Dict] = []  # one dict per provider attempt
    if len(providers) == 1:
        meta, code, raw, err = _ask_provider(providers[0])
        ok, n_rows, why = _validate_code(code)
        if progress_cb:
            if ok and n_rows > 0:
                status = f"OK {n_rows} ligne(s) extraite(s)"
            else:
                diag = _diagnose_failure(code, raw, why) if not err else err
                status = f"echec — {diag}"
            progress_cb(f"  • {providers[0].display_name} → {status}")
        results.append({
            "provider": providers[0].id, "model": providers[0].model,
            "meta": meta, "code": code, "ok": ok,
            "n_rows": n_rows, "reason": why, "error": err,
        })
    else:
        with ThreadPoolExecutor(max_workers=len(providers)) as pool:
            futs = {pool.submit(_ask_provider, p): p for p in providers}
            for fut in as_completed(futs):
                _check_cancel(cancel_check)
                p = futs[fut]
                try:
                    meta, code, raw, err = fut.result()
                except Exception as e:
                    meta, code, raw, err = {}, "", "", str(e)
                ok, n_rows, why = _validate_code(code)
                results.append({
                    "provider": p.id, "model": p.model,
                    "meta": meta, "code": code, "ok": ok,
                    "n_rows": n_rows, "reason": why, "error": err,
                })
                if progress_cb:
                    if ok and n_rows > 0:
                        status = f"OK {n_rows} ligne(s) extraite(s) sur le document complet"
                    else:
                        diag = _diagnose_failure(code, "", why) if not err else err
                        status = f"echec — {diag}"
                    progress_cb(f"  • {p.display_name} → {status}")

    # ── Pick the winner ───────────────────────────────────────────
    # 1. Prefer ok=True AND n_rows>0
    # 2. Among those, max n_rows
    # 3. Tie-break: prefer Gemini (multimodal) over text-only providers
    def _rank(r):
        ok_score = 1 if (r["ok"] and r["n_rows"] > 0) else 0
        prov_bonus = 1 if r["provider"] == "gemini" else 0
        return (ok_score, r["n_rows"], prov_bonus)
    results.sort(key=_rank, reverse=True)
    winner = results[0]
    if progress_cb:
        ranking = " | ".join(
            f"{r['provider']}={'OK ' + str(r['n_rows']) if (r['ok'] and r['n_rows']>0) else 'KO'}"
            for r in results
        )
        progress_cb(f"Classement Round 1 : {ranking}")
        if winner["ok"] and winner["n_rows"] > 0:
            progress_cb(
                f"Gagnant Round 1 : {winner['provider']} ({winner['model']}) "
                f"avec {winner['n_rows']} ligne(s)."
            )
        else:
            progress_cb("Aucun parser n'a fonctionne au Round 1.")

    # ── Retry the winner once if it still produced 0 rows ─────────
    if not (winner["ok"] and winner["n_rows"] > 0):
        if progress_cb:
            progress_cb(
                "Round 1.5 : reformulation avec snippet du texte reel "
                "(premier bloc BL detecte)…"
            )
        # Don't use text[:3500] which may be the recap/summary page.
        # Find the first BL block for a representative snippet.
        snippet_sample = _build_representative_sample(full_text, max_chars=5000)
        feedback = (
            "\n\nFEEDBACK CRITIQUE : ta fonction parse() a retourne 0 ligne sur "
            "le DOCUMENT COMPLET. Voici un BLOC REEL contenant les premiers BL "
            "tels que l'OCR les produit (adapte tes regex a CE format exact) :\n"
            "----- DEBUT TEXTE REEL -----\n"
            + snippet_sample +
            "\n----- FIN TEXTE REEL -----\n\n"
            "RE-ECRIS parse() en t'appuyant sur CE texte. Tes regex DOIVENT "
            "matcher les marqueurs exacts que tu vois ci-dessus. "
            "Ta nouvelle version DOIT extraire au moins les BL visibles dans ce snippet."
        )
        # Retry only with the best-supported provider (Gemini if available)
        retry_prov = next((p for p in providers if p.id == "gemini"), providers[0])
        meta2, code2, raw2, err2 = _ask_provider(retry_prov, feedback)
        ok2, n_rows2, why2 = _validate_code(code2)
        if progress_cb:
            if ok2 and n_rows2 > 0:
                progress_cb(f"  • {retry_prov.display_name} (retry) → OK {n_rows2} ligne(s)")
            else:
                diag = _diagnose_failure(code2, raw2, why2) if not err2 else err2
                progress_cb(f"  • {retry_prov.display_name} (retry) → echec — {diag}")
        if ok2 and n_rows2 > winner["n_rows"]:
            winner = {
                "provider": retry_prov.id, "model": retry_prov.model,
                "meta": meta2 or winner["meta"], "code": code2, "ok": True,
                "n_rows": n_rows2, "reason": "", "error": "",
            }

    # ── Round 2: SELF-VERIFICATION & REFINEMENT ───────────────────
    # The winner produced rows, but did it extract every field correctly?
    # Run the parser, audit the output, and if we spot systematic gaps
    # (fields that are ALWAYS empty across every row) feed those gaps
    # back to the model so it can patch the parser using the SAME 16-page
    # context as ground truth. This is the "self-correction" round the
    # user asked for — efficient because we only do it when needed.
    if winner["ok"] and winner["n_rows"] > 0:
        refined = _self_refine_winner(
            winner=winner,
            full_text=full_text,
            sample=sample,
            page1_png=page1_png,
            providers=providers,
            build_prompt=_build_prompt,
            ask_provider=_ask_provider,
            validate_code=_validate_code,
            progress_cb=progress_cb,
            cancel_check=cancel_check,
        )
        if refined is not None:
            winner = refined

    meta = winner["meta"] or {}
    code = winner["code"]
    n_rows = winner["n_rows"]
    ok = winner["ok"]

    # Provenance summary so the UI can show "winner: deepseek (217 rows)
    # vs gemini (195)".
    learners_summary = [
        {"provider": r["provider"], "model": r["model"],
         "n_rows": r["n_rows"], "ok": r["ok"], "reason": r["reason"],
         "won": r is winner}
        for r in results
    ]

    parsed: Dict = dict(meta or {})
    parsed["is_scanned"] = is_scanned
    # Persist the spatial template descriptor (when the model produced one)
    # so the local parser can use it as the PRIMARY extraction path.
    spatial_descriptor = (meta or {}).get("spatial_template") or {}
    parsed["parse_template"] = {
        "header_field_patterns": (meta or {}).get("header_field_patterns") or {},
        "parse_code": code if (ok and n_rows > 0) else "",
        "shipowner": (meta or {}).get("shipowner") or "",
        "row_count": n_rows,
        "spatial_template": spatial_descriptor,
    }
    parsed["sample_text"] = text[:4000]
    parsed.setdefault("example_rows", [])
    parsed["_local_row_count_on_sample"] = n_rows
    parsed["_learners"] = learners_summary
    parsed["_winner_provider"] = winner["provider"]
    parsed["_winner_model"] = winner["model"]
    return parsed


def _count_template_matches(template: Dict, text: str) -> int:
    """Count lines of `text` that match at least one row_pattern in template."""
    if not template or not isinstance(template, dict):
        return 0
    compiled = []
    for pat in template.get("row_patterns") or []:
        try:
            compiled.append(re.compile(pat, re.IGNORECASE))
        except re.error:
            continue
    if not compiled:
        return 0
    return sum(1 for line in text.splitlines() if any(rx.search(line) for rx in compiled))


def ai_fix_row(row: Dict, issues: List[str], context: str = "") -> Dict:
    """Send a problematic row + its issues + document context to Gemini.

    Returns a NEW dict merging original row with AI corrections.
    """
    prompt = (
        _FIX_INSTRUCTIONS
        + "\n\nProblèmes détectés : " + ", ".join(issues)
        + "\n\nLigne actuelle (JSON) :\n" + json.dumps(row, ensure_ascii=False, indent=2)
        + "\n\nContexte du document (texte brut) :\n" + (context[:8000] if context else "(non fourni)")
        + "\n\nLigne corrigée :"
    )
    resp = generate_with_fallback(
        prompt,
        generation_config={
            "response_mime_type": "application/json",
            "temperature": 0.1,
            "max_output_tokens": 2048,   # fix_row = single row JSON
        },
    )
    fixed = _parse_json(resp.text)
    if not isinstance(fixed, dict):
        return row
    out = {**row}
    for k, v in fixed.items():
        if v is not None and str(v).strip():
            out[k] = v
    return out


# ============================================================
# INTERNAL HELPERS
# ============================================================
def _build_extract_prompt(
    text: str,
    extra_hints: str = "",
    example_rows: Optional[List[Dict]] = None,
    chunk_info: Optional[tuple] = None,
) -> str:
    hint_block = ""
    if extra_hints and extra_hints.strip():
        hint_block = "\n\nINDICATIONS SPÉCIFIQUES À CE FORMAT :\n" + extra_hints.strip()
    example_block = ""
    if example_rows:
        # Show up to 3 example rows from a previously-learned sample of the same format.
        sample = example_rows[:3]
        example_block = (
            "\n\nEXEMPLES DE LIGNES DÉJÀ EXTRAITES POUR CE FORMAT (à utiliser comme référence "
            "structurelle — respecte les mêmes clés, le même style de valeurs) :\n"
            + json.dumps(sample, ensure_ascii=False, indent=2)
        )
    chunk_block = ""
    if chunk_info and chunk_info[1] > 1:
        idx, total = chunk_info
        chunk_block = (
            f"\n\nIMPORTANT — EXTRACTION PAR LOTS : ce texte est le LOT {idx}/{total} "
            f"d'un long manifeste. Extrais UNIQUEMENT les BL présents dans ce lot. "
            f"Les champs d'en-tête (vessel, voyage, date, shipowner) sont les mêmes pour "
            f"tous les lots — répète-les si tu les vois ici, sinon laisse vides."
        )
    # Per-chunk text cap (we already chunk before calling, so 24000 here is a safety net)
    truncated = text[:24000]
    return (
        _EXTRACT_INSTRUCTIONS
        + hint_block
        + example_block
        + chunk_block
        + "\n\n--- TEXTE DU MANIFESTE ---\n"
        + truncated
        + "\n--- FIN TEXTE ---\n\nJSON :"
    )


def _extract_via_vision(
    pdf_path: Path,
    extra_hints: str = "",
    progress_cb=None,
) -> List[Dict]:
    """DEPRECATED: kept for backwards compat. Now routes through Cloud Vision."""
    if progress_cb:
        progress_cb("OCR Cloud Vision en cours…")
    text = vision_client.ocr_pdf(
        pdf_path,
        progress_cb=lambda i, n: (
            progress_cb(f"OCR page {i}/{n}…") if progress_cb else None
        ),
    )
    if not text.strip():
        return []
    return extract_rows_from_text(text, source_file=str(pdf_path), extra_hints=extra_hints)


def _read_pdf_text(pdf_path: Path, max_pages: Optional[int] = None) -> str:
    """Best-effort text extraction via pdfplumber. Returns "" on failure."""
    pages = _read_pdf_text_pages(pdf_path, max_pages=max_pages)
    return "\n\n".join(pages)


def _read_pdf_text_pages(pdf_path: Path, max_pages: Optional[int] = None) -> List[str]:
    """Per-page text extraction via pdfplumber. Returns list of page strings."""
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return []
    try:
        out: List[str] = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages
            if max_pages:
                pages = pages[:max_pages]
            for p in pages:
                t = p.extract_text() or ""
                out.append(t)
        return out
    except Exception:
        return []


def _split_text_by_size(text: str, target_chars: int = 12000) -> List[str]:
    """Split a single text blob into roughly equal chunks at line boundaries."""
    if len(text) <= target_chars:
        return [text]
    lines = text.splitlines(keepends=True)
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for line in lines:
        if cur_len + len(line) > target_chars and cur:
            chunks.append("".join(cur))
            cur, cur_len = [], 0
        cur.append(line)
        cur_len += len(line)
    if cur:
        chunks.append("".join(cur))
    return chunks


def _build_chunks(pages: List[str], max_chars: int = 18000) -> List[str]:
    """Group consecutive pages into chunks of up to max_chars characters.

    Always keeps the first page in chunk #1 (so the AI sees the manifest header).
    """
    if not pages:
        return []
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for i, p in enumerate(pages):
        page_text = p or ""
        # If a single page is itself too big, hard-split it
        if len(page_text) > max_chars:
            if cur:
                chunks.append("\n\n".join(cur))
                cur, cur_len = [], 0
            chunks.extend(_split_text_by_size(page_text, target_chars=max_chars))
            continue
        if cur_len + len(page_text) > max_chars and cur:
            chunks.append("\n\n".join(cur))
            cur, cur_len = [], 0
        cur.append(page_text)
        cur_len += len(page_text) + 2
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks

def _parse_json(raw: str):
    """Robust JSON parsing — strip markdown fences, repair truncated output."""
    if not raw:
        return None
    s = raw.strip()
    # Strip ```json ... ``` if model added them despite mime_type hint
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    # Fast path
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # Try the largest balanced {...} block
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # Truncated response: try to salvage as much of "rows":[...] as possible
    return _repair_truncated_json(s)


def _repair_truncated_json(raw: str):
    """Recover header + as many complete row objects as possible from a
    truncated Gemini response (output limit hit mid-array)."""
    # Header keys typically appear early — extract them via simple regex
    header = {}
    for key in ("vessel", "voyage", "date_of_arrival", "shipowner"):
        mm = re.search(rf'"{key}"\s*:\s*"([^"]*)"', raw)
        if mm:
            header[key] = mm.group(1)
    # Find rows array start
    start = raw.find('"rows"')
    if start < 0:
        return header or None
    arr_start = raw.find('[', start)
    if arr_start < 0:
        return header or None
    # Walk and collect each top-level {...} object inside the array
    rows: List[Dict] = []
    depth = 0
    in_str = False
    esc = False
    obj_start = -1
    for i in range(arr_start + 1, len(raw)):
        c = raw[i]
        if in_str:
            if esc:
                esc = False
            elif c == '\\':
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
            continue
        if c == '{':
            if depth == 0:
                obj_start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and obj_start >= 0:
                obj_text = raw[obj_start:i + 1]
                try:
                    rows.append(json.loads(obj_text))
                except json.JSONDecodeError:
                    pass
                obj_start = -1
        elif c == ']' and depth == 0:
            break
    if rows or header:
        return {**header, "rows": rows}
    return None


def _flatten_to_rows(data, source_file: str = "") -> List[Dict]:
    """Inject header into each row dict; ensure required keys exist."""
    if not isinstance(data, dict):
        return []
    header = {
        "vessel": _s(data.get("vessel")),
        "voyage": _s(data.get("voyage")),
        "date_of_arrival": _s(data.get("date_of_arrival")),
        "_shipowner": _s(data.get("shipowner")),
        "source_file": source_file,
    }
    out: List[Dict] = []
    raw_rows = data.get("rows") or []
    if not isinstance(raw_rows, list):
        return []
    for i, r in enumerate(raw_rows):
        if not isinstance(r, dict):
            continue
        row = {**header}
        for k, v in r.items():
            row[k] = _s(v)
        row.setdefault("split_number", i + 1)
        row.setdefault("page", 0)
        out.append(row)
    return out


def _s(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v).strip()
