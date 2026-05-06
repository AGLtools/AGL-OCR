"""Gemini API client wrapper — uses the current `google-genai` SDK.

Stores API key + model preference in a gitignored JSON file under the
writable data dir. No env-var requirement: the user configures it once
via Menu IA → Configurer la clé.

Public API (unchanged from callers' perspective):
    generate_with_fallback(prompt, *, generation_config, primary)
    has_api_key(), get_api_key(), set_api_key()
    has_vision_key(), get_vision_api_key(), set_vision_api_key()
    get_model_name(), set_model_name()
    AVAILABLE_MODELS, DEFAULT_MODEL
"""
from __future__ import annotations
import json
import os
import threading
from pathlib import Path
from typing import Optional

from ..paths import app_data_dir


class GeminiNotConfigured(RuntimeError):
    """Raised when the API key is missing or the SDK is not installed."""


# ── Available models (cheapest / fastest first) ──────────────────────
AVAILABLE_MODELS = [
    "gemini-2.5-flash",           # défaut — rapide, bon marché, JSON mode
    "gemini-2.5-flash-lite",      # encore plus léger
    "gemini-2.5-pro",             # plus lourd, plus précis
    "gemini-2.0-flash",           # génération précédente
    "gemini-2.0-flash-lite",      # génération précédente légère
    "gemini-flash-latest",        # alias stable
    "gemini-pro-latest",          # alias stable pro
]
DEFAULT_MODEL = AVAILABLE_MODELS[0]

_FALLBACK_CHAIN = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
]


# ── Config storage ───────────────────────────────────────────────────
def _config_path() -> Path:
    p = app_data_dir() / "data" / "ai_config.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load_cfg() -> dict:
    p = _config_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cfg(cfg: dict) -> None:
    _config_path().write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ── Gemini key ──────────────────────────────────────────────────────────
def get_api_key() -> Optional[str]:
    env = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if env:
        return env.strip()
    return (_load_cfg().get("gemini_api_key") or "").strip() or None


def set_api_key(key: str) -> None:
    cfg = _load_cfg()
    cfg["gemini_api_key"] = (key or "").strip()
    _save_cfg(cfg)


def has_api_key() -> bool:
    return bool(get_api_key())


# ── Cloud Vision key ─────────────────────────────────────────────────
def get_vision_api_key() -> Optional[str]:
    env = os.environ.get("GOOGLE_VISION_API_KEY")
    if env:
        return env.strip()
    return (_load_cfg().get("vision_api_key") or "").strip() or None


def set_vision_api_key(key: str) -> None:
    cfg = _load_cfg()
    cfg["vision_api_key"] = (key or "").strip()
    _save_cfg(cfg)


def has_vision_key() -> bool:
    return bool(get_vision_api_key()) or bool(get_api_key())


# ── DeepSeek key (OpenAI-compatible API) ─────────────────────────────
def get_deepseek_api_key() -> Optional[str]:
    env = os.environ.get("DEEPSEEK_API_KEY")
    if env:
        return env.strip()
    return (_load_cfg().get("deepseek_api_key") or "").strip() or None


def set_deepseek_api_key(key: str) -> None:
    cfg = _load_cfg()
    cfg["deepseek_api_key"] = (key or "").strip()
    _save_cfg(cfg)


def has_deepseek_key() -> bool:
    return bool(get_deepseek_api_key())


def get_deepseek_model() -> str:
    return _load_cfg().get("deepseek_model") or "deepseek-chat"


def set_deepseek_model(name: str) -> None:
    cfg = _load_cfg()
    cfg["deepseek_model"] = name
    _save_cfg(cfg)


# ── Multi-provider learning preference ──────────────────────────────
# List of provider IDs participating in ensemble format-learning.
# Each provider produces a candidate parser; the one with the most
# rows on the validation text wins.
def get_learning_providers() -> list:
    cfg = _load_cfg()
    val = cfg.get("learning_providers")
    if isinstance(val, list) and val:
        return [str(v) for v in val]
    return ["gemini"]  # safe default


def set_learning_providers(providers: list) -> None:
    cfg = _load_cfg()
    cfg["learning_providers"] = list(providers or ["gemini"])
    _save_cfg(cfg)


# ── Model preference ───────────────────────────────────────────────────
def get_model_name() -> str:
    return _load_cfg().get("model") or DEFAULT_MODEL


def set_model_name(name: str) -> None:
    cfg = _load_cfg()
    cfg["model"] = name
    _save_cfg(cfg)


# ── OCR engine preference for scanned PDFs ──────────────────────────
# "cloud_vision" → Google Cloud Vision API (faster, better quality)
# "local"        → local pytesseract (offline, no API cost)
# ""             → ask each time
def get_ocr_engine() -> str:
    return _load_cfg().get("ocr_engine") or ""


def set_ocr_engine(value: str) -> None:
    cfg = _load_cfg()
    cfg["ocr_engine"] = value
    _save_cfg(cfg)


# ── New SDK client (google.genai) ───────────────────────────────────────
_client = None
_client_key: Optional[str] = None
_client_lock = threading.Lock()


def _get_client():
    """Return a configured google.genai.Client, creating it once per key (thread-safe)."""
    global _client, _client_key
    key = get_api_key()
    if not key:
        raise GeminiNotConfigured(
            "Clé API Gemini non configurée.\nMenu IA → Configurer la clé API."
        )
    with _client_lock:
        if _client_key != key:
            try:
                from google import genai  # type: ignore
            except ImportError as e:
                raise GeminiNotConfigured(
                    "Le package 'google-genai' n'est pas installé.\n\n"
                    "Installez-le avec :\n  pip install google-genai"
                ) from e
            _client = genai.Client(api_key=key)
            _client_key = key
        return _client


# ── Core generation helper ─────────────────────────────────────────────
def _call_model(model_name: str, prompt: str, generation_config: dict, image_bytes: Optional[bytes] = None):
    """Single call with the new SDK. Returns response with .text attribute.

    If `image_bytes` (PNG) is provided, sends a multimodal request so the
    model can also SEE the page layout (critical for learning column-based
    formats where pdfplumber merges columns onto one line).
    """
    try:
        from google.genai import types  # type: ignore
    except ImportError as e:
        raise GeminiNotConfigured(
            "Le package 'google-genai' n'est pas installe.\n\n"
            "Cette fonctionnalite (apprentissage IA / extraction Gemini) "
            "necessite la bibliotheque Google Gemini.\n\n"
            "Solution : lancez 'AGL OCR Updater' pour installer les nouvelles "
            "dependances, ou executez manuellement :\n"
            "  pip install google-genai"
        ) from e
    client = _get_client()

    mime = generation_config.get("response_mime_type", "text/plain")
    temperature = generation_config.get("temperature", 0.1)
    max_tokens = generation_config.get("max_output_tokens", 8192)  # callers override for large extractions

    cfg_kwargs = dict(
        response_mime_type=mime,
        temperature=temperature,
        max_output_tokens=max_tokens,
    )
    # Gemini 2.5 has an internal "thinking" mode that consumes the output
    # budget BEFORE the visible response. We disable it by default to get
    # the full JSON, but callers doing hard reasoning (e.g. LEARN with Pro)
    # can opt-in by setting "enable_thinking": True.
    if "2.5" in model_name and not generation_config.get("enable_thinking"):
        try:
            cfg_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=0)
        except Exception:
            pass  # Older SDK without ThinkingConfig — ignore

    config = types.GenerateContentConfig(**cfg_kwargs)

    if image_bytes:
        contents = [
            types.Part.from_bytes(data=image_bytes, mime_type="image/png"),
            prompt,
        ]
    else:
        contents = prompt

    return client.models.generate_content(
        model=model_name,
        contents=contents,
        config=config,
    )


def generate_with_fallback(
    prompt: str,
    *,
    generation_config: Optional[dict] = None,
    primary: Optional[str] = None,
    image_bytes: Optional[bytes] = None,
):
    """Generate content with automatic model fallback on 429/quota errors.

    Optional `image_bytes` (PNG) enables multimodal input.

    Returns the response object (has .text attribute).
    """
    primary = primary or get_model_name()
    seen: set = set()
    order = [primary] + [m for m in _FALLBACK_CHAIN if m != primary]
    cfg = generation_config or {}
    last_err: Optional[Exception] = None
    for name in order:
        if name in seen:
            continue
        seen.add(name)
        try:
            return _call_model(name, prompt, cfg, image_bytes=image_bytes)
        except Exception as e:
            msg = str(e).lower()
            # Retry on genuine rate-limit / quota errors
            is_quota = (
                "429" in msg
                or "quota" in msg
                or "resource_exhausted" in msg
                or "rate limit" in msg
                or "rate_limit" in msg
                or "too many requests" in msg
            )
            # Also skip models that don't exist for this API version/account
            is_not_found = "404" in msg and ("not_found" in msg or "not found" in msg)
            if is_quota or is_not_found:
                if last_err is None or is_quota:
                    last_err = e  # prefer showing quota error over 404
                continue
            raise
    if last_err:
        msg = str(last_err).lower()
        if "429" in msg or "quota" in msg or "resource_exhausted" in msg:
            raise RuntimeError(
                "Quota API Gemini dépassé pour tous les modèles disponibles.\n\n"
                "Solutions :\n"
                "• Attendez quelques minutes (limite par minute)\n"
                "• Activez la facturation sur votre projet GCP pour augmenter les quotas\n"
                "• Utilisez une clé API d'un projet GCP avec facturation activée"
            ) from last_err
        raise last_err
    raise RuntimeError("Aucun modèle Gemini disponible.")


# ── Legacy shims (kept so nothing else needs changing) ──────────────────
def get_model(model_name: Optional[str] = None):
    """Deprecated shim — callers should use generate_with_fallback() directly."""
    # Returns a lightweight proxy so old call sites still work
    return _ModelProxy(model_name or get_model_name())


class _ModelProxy:
    """Thin wrapper so legacy code calling model.generate_content() still works."""
    def __init__(self, name: str):
        self.name = name

    def generate_content(self, prompt, *, generation_config=None):
        return generate_with_fallback(
            prompt,
            generation_config=generation_config or {},
            primary=self.name,
        )

