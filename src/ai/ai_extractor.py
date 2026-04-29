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


# ── Format learning prompt ─────────────────────────────────────────────────
_LEARN_INSTRUCTIONS = """Tu es un expert en classification de documents maritimes.

Voici le début d'un manifeste de cargaison. Identifie son FORMAT (transporteur émetteur).

Réponds en JSON STRICT, sans markdown :
{
  "format_name": "nom court du format en MAJUSCULES (ex: MSC, CMA_CGM, MAERSK, HAPAG_LLOYD, SAKINA, COSCO)",
  "carrier_name": "nom complet du transporteur",
  "signature_keywords": ["mot1", "mot2", "mot3"],
  "is_scanned": true,
  "extraction_hints": "particularités à retenir pour l'extraction future (mise en page, étiquettes utilisées, pièges OCR)"
}

Règles pour signature_keywords :
- 3 à 6 tokens TRÈS SPÉCIFIQUES présents dans CE document
- IMPORTANT : chaque token doit être 1 à 3 mots MAXIMUM, sans saut de ligne
  (les phrases longues comme "MEDITERRANEAN SHIPPING COMPANY S.A., GENEVA" ne
  matcheront PAS car les PDF éclatent le texte sur plusieurs colonnes/lignes)
- Doivent permettre de reconnaître ce format à l'avenir SANS confusion avec d'autres transporteurs
- Évite les mots trop génériques : "BL", "VESSEL", "CARGO", "MANIFEST" (présents partout)
- Privilégie :
  * préfixe(s) de numéros BL (ex: "MEDU", "CMAU", "MAEU")
  * code/sigle court du transporteur (ex: "MSC", "CMA CGM", "MAERSK")
  * libellés de section spécifiques (ex: "BILL ISSUANCE", "PLACE OF RECEIPT")
  * code interne unique (ex: numéro à 5 chiffres entre parenthèses comme "(15358)")
- Casse-insensible — l'app fera .upper() pour comparer
- L'app valide la présence d'au moins 60% des tokens (mode tolérant)

is_scanned : true si le document est un scan (texte OCR avec fautes), false si c'est un PDF généré numériquement.
"""


# ── Template generation prompt (regex-based local parser recipe) ───────
_TEMPLATE_INSTRUCTIONS = """Tu es un expert en rétro-ingénierie de manifestes maritimes.

Je te donne le texte BRUT extrait par pdfplumber d'un document. Ta tâche : produire
un TEMPLATE DE PARSING REGEX qui permettra à mon code Python d'extraire les mêmes
informations LOCALEMENT (sans IA) lors des prochaines extractions de ce même format.

Réponds en JSON STRICT, sans markdown :
{
  "header_field_patterns": {
    "vessel":          "regex avec UN groupe capturé",
    "voyage":          "regex avec UN groupe capturé",
    "date_of_arrival": "regex avec UN groupe capturé"
  },
  "row_patterns": [
    "regex avec GROUPES NOMMÉS (?P<bl_number>...) (?P<container_number>...) etc."
  ],
  "shipowner": "valeur littérale (ex: MSC)"
}

RÈGLES STRICTES :
1. row_patterns : chaque regex DOIT capturer une ligne typique de cargaison du document.
   - Utilise les groupes nommés Python (?P<nom>...) avec ces noms uniquement :
     bl_number, bl_type, container_number, container_type, weight, weight_unit,
     pack_qty, pack_unit, volume, volume_unit, seal1, shipper, consignee,
     port_of_loading, port_of_discharge, place_of_delivery, description.
   - PAS de groupes anonymes (...) sans nom — seuls les groupes nommés sont conservés.
   - Si la même info se répète sur plusieurs lignes (ex: BL puis conteneurs), donne
     2-3 patterns distincts plutôt qu'un seul géant.
   - Échappe correctement (\\d, \\s, \\(, etc.) — c'est du JSON donc DOUBLE backslash.
   - Insensible à la casse (re.IGNORECASE est appliqué par le code).
2. header_field_patterns : chaque regex doit avoir UN SEUL groupe capturé (...) qui isole
   la valeur (sans le label). Le code prendra le premier match dans tout le document.
3. Privilégie la SPÉCIFICITÉ : ancres sur des préfixes uniques (ex: ^MEDU\\d+ pour MSC,
   ^[A-Z]{4}\\d{7} pour conteneur ISO).
4. Si tu ne sais pas extraire un champ avec une regex fiable, OMETS-le — ne mets pas
   un pattern hasardeux qui ferait du bruit.

Le code testera chaque ligne du PDF contre chaque pattern — le PREMIER qui matche
sur une ligne donnée produit la sortie. Évite donc les patterns trop laxistes.
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


def learn_format_from_pdf(pdf_path: str | Path, *, cancel_check=None) -> Dict:
    """Analyze a sample document, classify it AND extract an example.

    Returns a dict containing:
      - format_name, carrier_name, signature_keywords, is_scanned, extraction_hints
      - example_rows : list of rows the AI extracted from THIS document
      - sample_text : first 4000 chars of source text (for few-shot at re-use)

    The caller saves it via format_registry.save_learned() — those example_rows
    will then be injected into future extraction prompts as a few-shot reference,
    making subsequent extractions of the same format much more reliable
    (and effectively the « learned extractor » that no longer needs guessing).
    """
    pdf_path = Path(pdf_path)
    _check_cancel(cancel_check)
    text = _read_pdf_text(pdf_path, max_pages=2)
    if len(text.strip()) < 200:
        # Scanned — OCR first 2 pages only via Vision (keep cost low)
        text = vision_client.ocr_pdf(pdf_path, max_pages=2, cancel_check=cancel_check)

    # ── Steps 1, 2 & 3 run in PARALLEL ────────────────────────────
    # Step 1: classify format  |  Step 2: extract example rows  |  Step 3: regex template
    _check_cancel(cancel_check)

    classify_prompt = (
        _LEARN_INSTRUCTIONS
        + "\n\n--- DÉBUT DU MANIFESTE ---\n" + text[:10000] + "\n--- FIN ---"
    )

    def _task_classify() -> Dict:
        raw_cls = ""
        cls_parsed: Dict = {}
        err = ""
        try:
            resp = generate_with_fallback(
                classify_prompt,
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": 0.0,
                },
            )
            raw_cls = getattr(resp, "text", "") or ""
            cls_parsed = _parse_json(raw_cls) or {}
        except Exception as e:
            err = str(e)
            raise
        finally:
            debug_log.log_call(
                kind="learn", source_file=str(pdf_path),
                prompt=classify_prompt, raw_response=raw_cls,
                parsed=cls_parsed, error=err,
            )
        return cls_parsed

    def _task_example() -> List[Dict]:
        try:
            return extract_rows_from_text(text, source_file=str(pdf_path), extra_hints="")
        except Exception:
            return []

    def _task_template() -> Dict:
        return _learn_parse_template(text, source_file=str(pdf_path))

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_classify = pool.submit(_task_classify)
        f_example  = pool.submit(_task_example)
        f_template = pool.submit(_task_template)

        # Wait for classify first (raises on error → whole learn fails cleanly)
        cls_parsed   = f_classify.result()
        _check_cancel(cancel_check)
        example_rows = f_example.result()
        _check_cancel(cancel_check)
        parse_template = f_template.result()

    cls_parsed["example_rows"] = example_rows
    cls_parsed["sample_text"]  = text[:4000]
    if parse_template:
        cls_parsed["parse_template"] = parse_template
    return cls_parsed


def _learn_parse_template(text: str, *, source_file: str) -> Dict:
    """Ask Gemini for a regex-based parse template. Returns {} on failure."""
    prompt = (
        _TEMPLATE_INSTRUCTIONS
        + "\n\n--- TEXTE DU MANIFESTE (échantillon) ---\n"
        + text[:12000]
        + "\n--- FIN ---\n\nJSON :"
    )
    raw = ""
    parsed: Dict = {}
    err = ""
    try:
        resp = generate_with_fallback(
            prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
            },
        )
        raw = getattr(resp, "text", "") or ""
        out = _parse_json(raw)
        if isinstance(out, dict):
            parsed = out
    except Exception as e:
        err = str(e)
    finally:
        debug_log.log_call(
            kind="learn_template", source_file=source_file,
            prompt=prompt, raw_response=raw, parsed=parsed, error=err,
        )
    return parsed


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
