"""Google Cloud Vision OCR client (REST, no SDK dependency).

Used for high-volume OCR of scanned PDFs — much cheaper and faster than
sending PDFs to Gemini's File API. The same Google API key works for both
services as long as the user has enabled "Cloud Vision API" in their GCP
project.

Strategy for PDFs:
1. Convert pages to PNG locally with pdf2image (bundled poppler).
2. Send each page to Vision `documentTextDetection` (REST, synchronous).
3. Concatenate the extracted text.
4. Caller then sends that text to Gemini for structured extraction.

Pricing (April 2026): Vision DOCUMENT_TEXT_DETECTION = $1.50/1000 pages
after the first 1000 free pages/month. With a $300 GCP credit a user gets
~200k pages before paying anything.
"""
from __future__ import annotations
import base64
import json
import time
from pathlib import Path
from typing import List, Optional, Callable
from urllib import request as urlrequest, error as urlerror

from .gemini_client import get_api_key, get_vision_api_key  # vision key fallback


VISION_ENDPOINT = "https://vision.googleapis.com/v1/images:annotate"
DEFAULT_DPI = 200          # OCR-grade resolution; 300 is overkill for typeset text
HTTP_TIMEOUT = 60          # seconds per page request
MAX_RETRIES = 3


class VisionError(RuntimeError):
    """Raised when Cloud Vision returns an error or is misconfigured."""


def _resolve_key() -> str:
    """Use the dedicated Vision key if set, else fall back to the Gemini key."""
    key = (get_vision_api_key() or "").strip()
    if key:
        return key
    key = (get_api_key() or "").strip()
    if not key:
        raise VisionError(
            "Aucune clé API configurée pour Cloud Vision.\n"
            "Menu IA → Configurer la clé. La même clé Google fonctionne pour Vision "
            "à condition d'avoir activé 'Cloud Vision API' dans la console GCP."
        )
    return key


def _post_vision(payload: dict, key: str) -> dict:
    """POST to Vision REST endpoint with retry on transient errors."""
    body = json.dumps(payload).encode("utf-8")
    url = f"{VISION_ENDPOINT}?key={key}"
    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        req = urlrequest.Request(
            url, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urlerror.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            # Retry on 429 / 5xx
            if e.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                last_err = e
                continue
            raise VisionError(f"Vision API HTTP {e.code} : {err_body[:500]}") from e
        except urlerror.URLError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                last_err = e
                continue
            raise VisionError(f"Erreur réseau Vision : {e}") from e
    raise VisionError(f"Échec après {MAX_RETRIES} tentatives : {last_err}")


def ocr_image_bytes(image_bytes: bytes, *, language_hints: Optional[List[str]] = None) -> str:
    """OCR a single image (PNG/JPEG bytes) and return the full text."""
    key = _resolve_key()
    payload = {
        "requests": [
            {
                "image": {"content": base64.b64encode(image_bytes).decode("ascii")},
                "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
                "imageContext": {"languageHints": language_hints or ["en", "fr"]},
            }
        ]
    }
    data = _post_vision(payload, key)
    responses = data.get("responses") or []
    if not responses:
        return ""
    r0 = responses[0]
    if "error" in r0:
        raise VisionError(f"Vision: {r0['error'].get('message', 'unknown error')}")
    full = r0.get("fullTextAnnotation", {}) or {}
    return full.get("text", "") or ""


def ocr_pdf(
    pdf_path: str | Path,
    *,
    dpi: int = DEFAULT_DPI,
    language_hints: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    max_pages: Optional[int] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> str:
    """OCR every page of a PDF and return the concatenated text.

    Uses bundled poppler via pdf2image for the rasterization step.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise VisionError(f"Fichier introuvable : {pdf_path}")

    try:
        from pdf2image import convert_from_path
    except ImportError as e:
        raise VisionError("pdf2image n'est pas installé.") from e

    # Locate bundled poppler if any (same logic as ocr_engine)
    from ..paths import poppler_bin
    poppler_path = poppler_bin()

    images = convert_from_path(
        str(pdf_path),
        dpi=dpi,
        poppler_path=poppler_path,
        fmt="png",
    )
    if max_pages:
        images = images[:max_pages]

    total = len(images)
    parts: List[str] = []
    import io
    for i, img in enumerate(images):
        if cancel_check is not None and cancel_check():
            from .ai_extractor import AICancelled
            raise AICancelled("OCR annulé par l'utilisateur")
        if progress_cb:
            progress_cb(i + 1, total)
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        text = ocr_image_bytes(buf.getvalue(), language_hints=language_hints)
        parts.append(f"\n\n=== PAGE {i + 1} ===\n{text}")
    return "".join(parts).strip()


def is_configured() -> bool:
    """True if a usable API key is available (Vision-specific or Gemini fallback)."""
    try:
        _resolve_key()
        return True
    except VisionError:
        return False
