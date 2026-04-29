"""AI module — Gemini-powered universal manifest extraction, format learning,
validation and auto-correction.

Public API:
    from src.ai import (
        get_api_key, set_api_key, has_api_key,        # config
        extract_rows_from_pdf, learn_format_from_pdf, # extraction / learning
        ai_fix_row,                                    # correction
        validate_rows, validate_row,                   # validation
        list_learned, save_learned, detect_learned,    # registry
    )
"""
from .gemini_client import (
    get_api_key, set_api_key, has_api_key,
    DEFAULT_MODEL, AVAILABLE_MODELS, get_model_name, set_model_name,
    get_ocr_engine, set_ocr_engine,
    GeminiNotConfigured,
)
from .ai_extractor import (
    extract_rows_from_pdf, extract_rows_from_text,
    learn_format_from_pdf, ai_fix_row,
)
from .validators import validate_rows, validate_row
from .format_registry import list_learned, save_learned, detect_learned, delete_learned

__all__ = [
    "get_api_key", "set_api_key", "has_api_key",
    "DEFAULT_MODEL", "AVAILABLE_MODELS", "get_model_name", "set_model_name",
    "GeminiNotConfigured",
    "extract_rows_from_pdf", "extract_rows_from_text",
    "learn_format_from_pdf", "ai_fix_row",
    "validate_rows", "validate_row",
    "list_learned", "save_learned", "detect_learned", "delete_learned",
]
