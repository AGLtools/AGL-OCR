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
    "vessel":          "regex avec UN groupe capture (...)",
    "voyage":          "regex avec UN groupe capture (...)",
    "date_of_arrival": "regex avec UN groupe capture (...)"
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

6. PIEGES CONNUS pdfplumber :
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

7. ROBUSTESSE :
   - Tolerant aux espaces multiples (`\\s+`).
   - Skippe silencieusement les blocs non-cargo (en-tetes, totaux, footers).
   - Pas d'exception non rattrapee : enrobe les conversions float/int en
     try/except si le pattern peut matcher du texte malforme.
   - JAMAIS print(), open(), exec(), eval(), pas d'acces fichier/reseau.

8. AUTO-VERIFICATION OBLIGATOIRE AVANT DE REPONDRE :
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
        # Scanned PDF — OCR via Cloud Vision (returns pages joined)
        if progress_cb:
            progress_cb("Document scanné détecté — OCR via Cloud Vision…")
        ocr_text = vision_client.ocr_pdf(
            pdf_path,
            progress_cb=lambda i, n: (
                progress_cb(f"OCR Cloud Vision page {i}/{n}…") if progress_cb else None
            ),
            cancel_check=cancel_check,
        )
        source_kind = "vision"
        debug_log.log_call(
            kind="vision_ocr",
            source_file=str(pdf_path),
            ocr_text=ocr_text,
            extra={"chars": len(ocr_text)},
        )
        if not ocr_text.strip():
            return []
        # Vision returns one big blob — split heuristically by approximate page size
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


def learn_format_from_pdf(pdf_path: str | Path, *, cancel_check=None) -> Dict:
    """Learn a manifest format from a sample PDF using ONE Gemini call.

    Sends BOTH the pdfplumber text AND a rendered image of page 1 to
    gemini-2.5-pro (with thinking enabled) so the model can correlate
    spatial layout with the actual text its parser will receive.

    Validates by compiling AND running it locally; retries once with
    feedback if it fails or returns 0 rows.
    """
    from .template_parser import run_parse_code

    pdf_path = Path(pdf_path)
    _check_cancel(cancel_check)

    # Try embedded text first. If document is scanned, fall back to Cloud
    # Vision OCR — and remember it so future docs of the same format are
    # OCR'd before the local parser runs.
    digital_text = _read_pdf_text(pdf_path, max_pages=6)
    is_scanned = len(digital_text.strip()) < 200
    if is_scanned:
        # Scanned: use OCR for both the AI sample AND the validation set.
        # We OCR up to 12 pages here -- enough variety for the parser to be
        # robust without exploding cost. The same parser will run on the
        # FULL document at parse time (via vision_client.ocr_pdf again).
        text = vision_client.ocr_pdf(pdf_path, max_pages=12, cancel_check=cancel_check)
        full_text = text  # validate against the same OCR text
    else:
        text = digital_text
        try:
            full_text = _read_pdf_text(pdf_path, max_pages=None)
        except Exception:
            full_text = text
        if len(full_text.strip()) < len(text.strip()):
            full_text = text

    sample = text[:14000]

    # Render page 1 as PNG for multimodal context (so Gemini SEES the layout).
    page1_png = _render_first_page_png(pdf_path)

    def _split_response(raw: str) -> tuple[Dict, str]:
        """Split AI response into (metadata_dict, python_code).

        Tolerates missing closing markers, missing JSON markers, or markdown
        code fences. Returns ({}, "") if nothing parseable found.
        """
        meta: Dict = {}
        code = ""
        if not raw:
            return meta, code
        # JSON block
        m_json = re.search(
            r"<<<JSON>>>\s*(\{[\s\S]*?\})\s*(?:<<<END_JSON>>>|<<<PYTHON>>>|$)",
            raw,
        )
        if m_json:
            try:
                meta = json.loads(m_json.group(1))
            except Exception:
                # try a relaxed fallback: find the largest balanced {...}
                meta = _parse_json(m_json.group(1)) or {}
        else:
            # fallback: response is pure JSON (legacy)
            meta = _parse_json(raw) or {}

        # PYTHON block
        m_py = re.search(
            r"<<<PYTHON>>>\s*(?:```(?:python)?\s*)?([\s\S]*?)(?:```\s*)?<<<END_PYTHON>>>",
            raw,
        )
        if not m_py:
            # fallback: any text after the marker until end
            m_py = re.search(r"<<<PYTHON>>>\s*(?:```(?:python)?\s*)?([\s\S]+?)$", raw)
        if m_py:
            code = m_py.group(1).strip()
            # strip trailing ``` if model added markdown fence
            code = re.sub(r"\s*```\s*$", "", code).strip()
        return meta, code

    def _validate_code(code: str) -> tuple[bool, int, str]:
        """Compile + run on FULL document. Returns (compiles_ok, n_rows, error).

        - compiles_ok=False if SyntaxError (truly broken Python).
        - n_rows: rows produced on the full PDF text.
        - error: short reason ("syntax", "no_parse_fn", "runtime", "zero_rows", "").
        """
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

    def _ask(extra_feedback: str = "") -> tuple[Dict, str, str]:
        scan_warning = ""
        if is_scanned:
            scan_warning = (
                "\n\n*** ATTENTION : ce document est un SCAN (pas de texte natif). "
                "Le TEXTE (B) ci-dessous provient de l'OCR (Cloud Vision). "
                "Il peut contenir des fautes OCR : I/1, O/0, S/5, B/8, Z/2, etc. "
                "Ton parser doit etre TOLERANT aux fautes OCR : utilise des regex "
                "souples (ex: `[A-Z0-9]{4,5}\\d{6,7}` plutot que `[A-Z]{4}\\d{7}` strict, "
                "et `[\\d,. ]+` pour les nombres). Le MEME OCR sera applique aux "
                "futurs documents avant ton parser, donc reproduit fidelement les "
                "patterns de l'OCR. ***\n"
            )
        prompt = (
            _LEARN_COMBINED_INSTRUCTIONS
            + scan_warning
            + extra_feedback
            + "\n\n--- TEXTE DU MANIFESTE ("
            + ("OCR Cloud Vision" if is_scanned else "extrait par pdfplumber")
            + ", multi-pages) ---\n"
            + sample
            + "\n--- FIN TEXTE ---\n"
            + ("\n(L'IMAGE de la page 1 est jointe ci-dessus pour le contexte spatial.)\n"
               if page1_png else "")
        )
        raw = ""
        meta: Dict = {}
        code = ""
        err = ""
        try:
            resp = generate_with_fallback(
                prompt,
                generation_config={
                    # plain text now: we use our own delimiters
                    "temperature": 0.0,
                    "max_output_tokens": 16384,
                    # Pro reasoning: keep thinking ON for hard layout analysis
                    "enable_thinking": True,
                },
                # Force the most powerful model for learning — this is a
                # one-shot per format (then cached forever), so cost is OK.
                primary="gemini-2.5-pro",
                image_bytes=page1_png,
            )
            raw = getattr(resp, "text", "") or ""
            meta, code = _split_response(raw)
        except Exception as e:
            err = str(e)
            raise
        finally:
            debug_log.log_call(
                kind="learn", source_file=str(pdf_path),
                prompt=prompt, raw_response=raw,
                parsed={"meta": meta, "code_chars": len(code)},
                error=err,
                extra={"image_attached": bool(page1_png)},
            )
        return meta, code, raw

    meta, code, _raw = _ask()
    _check_cancel(cancel_check)
    ok, n_rows, why = _validate_code(code)

    # Retry once if the parser is unusable (syntax error OR 0 rows on the FULL
    # document). 0 rows on the full PDF is a real failure -- the parser is
    # useless. We give the AI specific feedback to fix it.
    if not ok or n_rows == 0:
        if why == "syntax":
            feedback = (
                "\n\nFEEDBACK CRITIQUE : ta fonction parse() ne COMPILE PAS "
                "(SyntaxError). Re-ecris-la entierement en PYTHON STRICT "
                "(pas de /* */, pas de typedict, uniquement re)."
            )
        else:
            # Show the AI a concrete first BL block from the actual document
            # so it can SEE the real layout (pdfplumber may break lines
            # differently than the AI assumed).
            snippet = full_text[:3500]
            feedback = (
                "\n\nFEEDBACK CRITIQUE : ta fonction parse() a retourne "
                "0 ligne quand on l'execute sur le DOCUMENT COMPLET "
                "(plusieurs pages, pas seulement l'echantillon).\n\n"
                "Cause probable : tes regex supposent une mise en page "
                "(ex: label\\nvaleur) qui ne correspond pas au texte reel "
                "extrait par pdfplumber.\n\n"
                "Voici le DEBUT REEL du texte tel que pdfplumber le voit "
                "(adapte tes regex a CE format exact, attention aux espaces "
                "vs sauts de ligne) :\n"
                "----- DEBUT TEXTE REEL -----\n"
                + snippet
                + "\n----- FIN TEXTE REEL -----\n\n"
                "RE-ECRIS parse() de zero. Ta nouvelle version DOIT extraire "
                "au moins les lignes visibles dans ce snippet (au minimum "
                "les BL identifiables par 'SH:' ou 'B/L NR.')."
            )
        _check_cancel(cancel_check)
        meta2, code2, _ = _ask(feedback)
        ok2, n_rows2, _ = _validate_code(code2)
        # Accept retry if it's strictly better
        if ok2 and n_rows2 > n_rows:
            meta = meta2 or meta
            code = code2
            n_rows = n_rows2
            ok = True

    parsed: Dict = dict(meta or {})
    # Authoritative — we KNOW whether the source had embedded text or not.
    parsed["is_scanned"] = is_scanned
    parsed["parse_template"] = {
        "header_field_patterns": (meta or {}).get("header_field_patterns") or {},
        # Save the code only if it actually produces rows on the full doc.
        # An empty parse_code triggers the "no local parser" UX rather than
        # silently saving a useless one.
        "parse_code": code if (ok and n_rows > 0) else "",
        "shipowner": (meta or {}).get("shipowner") or "",
        "row_count": n_rows,          # persisted so save_learned can compare
    }
    parsed["sample_text"] = text[:4000]
    parsed.setdefault("example_rows", [])
    parsed["_local_row_count_on_sample"] = n_rows
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
