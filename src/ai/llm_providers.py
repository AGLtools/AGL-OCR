"""Multi-LLM provider abstraction for ensemble format learning.

Adapters wrap heterogeneous SDKs (google-genai, OpenAI-compatible) behind
a single `LLMProvider.generate(prompt, *, image_bytes, ...)` -> str method.

Used by `ai_extractor.learn_format_from_pdf` to query several providers in
parallel and pick the one whose generated parser extracts the most rows.
"""
from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Optional, List

from . import gemini_client


class LLMError(RuntimeError):
    """Raised when a provider call fails (network, auth, quota)."""


@dataclass
class LLMResponse:
    provider_id: str
    model: str
    text: str


class LLMProvider:
    """Abstract base. Subclasses implement .generate()."""
    id: str = ""
    display_name: str = ""
    supports_vision: bool = False

    def is_configured(self) -> bool:
        raise NotImplementedError

    def generate(
        self,
        prompt: str,
        *,
        image_bytes: Optional[bytes] = None,
        max_tokens: int = 16384,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> LLMResponse:
        raise NotImplementedError


# ── Gemini (Google) ────────────────────────────────────────────────────
class GeminiProvider(LLMProvider):
    id = "gemini"
    display_name = "Google Gemini"
    supports_vision = True

    def __init__(self, model: Optional[str] = None):
        # Use the most powerful model by default for learning.
        self.model = model or "gemini-2.5-pro"

    def is_configured(self) -> bool:
        return gemini_client.has_api_key()

    def generate(self, prompt, *, image_bytes=None, max_tokens=16384,
                 temperature=0.0, json_mode=False):
        cfg = {
            "temperature": temperature,
            "max_output_tokens": max_tokens,
            "enable_thinking": True,  # Pro reasoning for hard layout analysis
        }
        if json_mode:
            cfg["response_mime_type"] = "application/json"
        try:
            resp = gemini_client.generate_with_fallback(
                prompt,
                generation_config=cfg,
                primary=self.model,
                image_bytes=image_bytes,
            )
        except Exception as e:
            raise LLMError(f"Gemini: {e}") from e
        return LLMResponse(
            provider_id=self.id,
            model=self.model,
            text=getattr(resp, "text", "") or "",
        )


# ── DeepSeek (OpenAI-compatible REST) ─────────────────────────────────
class DeepSeekProvider(LLMProvider):
    """DeepSeek chat — text-only, OpenAI-compatible API.

    Endpoint: https://api.deepseek.com/v1/chat/completions
    Available models: deepseek-chat, deepseek-reasoner
    Vision: not supported by deepseek-chat (image is silently dropped).
    """
    id = "deepseek"
    display_name = "DeepSeek"
    supports_vision = False
    BASE_URL = "https://api.deepseek.com/v1/chat/completions"

    def __init__(self, model: Optional[str] = None):
        self.model = model or gemini_client.get_deepseek_model()

    def is_configured(self) -> bool:
        return gemini_client.has_deepseek_key()

    def generate(self, prompt, *, image_bytes=None, max_tokens=16384,
                 temperature=0.0, json_mode=False):
        # DeepSeek caps at 8192 output tokens for most models.
        max_tokens = min(max_tokens, 8000)
        api_key = gemini_client.get_deepseek_api_key()
        if not api_key:
            raise LLMError("DeepSeek: clé API non configurée.")
        try:
            import urllib.request
            import urllib.error
        except ImportError as e:
            raise LLMError(f"DeepSeek: urllib indisponible ({e})") from e

        body = {
            "model": self.model,
            "messages": [
                {"role": "system",
                 "content": "Tu es un expert en analyse de documents."},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}

        req = urllib.request.Request(
            self.BASE_URL,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                payload = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                detail = e.read().decode("utf-8")[:400]
            except Exception:
                detail = ""
            raise LLMError(f"DeepSeek HTTP {e.code}: {detail}") from e
        except Exception as e:
            raise LLMError(f"DeepSeek: {e}") from e

        choices = payload.get("choices") or []
        if not choices:
            raise LLMError("DeepSeek: réponse vide.")
        text = (choices[0].get("message") or {}).get("content") or ""
        return LLMResponse(provider_id=self.id, model=self.model, text=text)


# ── Registry ───────────────────────────────────────────────────────────
class GeminiFlashProvider(GeminiProvider):
    """Cheaper / faster Gemini variant. Used for patch/upgrade calls
    where the prompt is small (< 4 KB) and reasoning isn't needed.
    """
    id = "gemini_flash"
    display_name = "Google Gemini Flash"

    def __init__(self, model: Optional[str] = None):
        super().__init__(model or "gemini-2.5-flash")


_REGISTRY = {
    "gemini": GeminiProvider,
    "gemini_flash": GeminiFlashProvider,
    "deepseek": DeepSeekProvider,
}


def all_provider_ids() -> List[str]:
    return list(_REGISTRY.keys())


def get_provider(provider_id: str) -> Optional[LLMProvider]:
    cls = _REGISTRY.get(provider_id)
    if not cls:
        return None
    return cls()


def call_single(
    provider_id: str,
    prompt: str,
    *,
    image_bytes: Optional[bytes] = None,
    max_tokens: int = 4000,
    temperature: float = 0.0,
    json_mode: bool = False,
) -> LLMResponse:
    """Run a single LLM call against ``provider_id``.

    Used by the patch / upgrade pipelines where the prompt is small and
    one model is enough. Raises :class:`LLMError` if the provider is
    missing or not configured.
    """
    prov = get_provider(provider_id)
    if prov is None:
        raise LLMError(f"Provider inconnu: {provider_id!r}")
    if not prov.is_configured():
        raise LLMError(f"Provider {provider_id!r} non configure (cle API manquante).")
    img = image_bytes if prov.supports_vision else None
    return prov.generate(
        prompt,
        image_bytes=img,
        max_tokens=max_tokens,
        temperature=temperature,
        json_mode=json_mode,
    )


def configured_learning_providers() -> List[LLMProvider]:
    """Return the list of providers actually usable for ensemble learning.

    Filters out any provider the user enabled but whose API key is missing.
    """
    out = []
    for pid in gemini_client.get_learning_providers():
        prov = get_provider(pid)
        if prov is None:
            continue
        if prov.is_configured():
            out.append(prov)
    if not out:
        # Always fall back to Gemini if at least its key is set.
        g = GeminiProvider()
        if g.is_configured():
            out.append(g)
    return out
