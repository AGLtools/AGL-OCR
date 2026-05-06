"""Light housekeeping utilities (cache pruning, log rotation, …).

Designed to be called at startup. Failures must NEVER prevent the app
from launching — every operation is wrapped in best-effort try/except.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable

# Subdirectories of DATA_DIR considered safe to prune by age.
# Keep this conservative — never touch user-authored data
# (learned_formats, corrections, exports).
_PRUNABLE_SUBDIRS = ("cache", "logs", "tmp")


def _clean_old_files(directory: Path, max_days: int) -> int:
    """Delete files older than ``max_days`` under ``directory``. Returns
    the number of files removed. Silently swallows per-file errors.
    """
    if not directory.exists():
        return 0
    cutoff = time.time() - max_days * 86400
    removed = 0
    try:
        iterator = directory.rglob("*")
    except OSError:
        return 0
    for f in iterator:
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
                removed += 1
        except OSError:
            continue
    return removed


def run_cleanup(
    data_dir: Path,
    *,
    max_days: int = 30,
    extra_subdirs: Iterable[str] = (),
) -> dict:
    """Prune old files in well-known subfolders of ``data_dir``.

    Returns a dict ``{subdir: nb_removed}`` for diagnostics. Never
    raises — safe to call at app startup before the UI is up.
    """
    report: dict = {}
    if not data_dir:
        return report
    base = Path(data_dir)
    targets = list(_PRUNABLE_SUBDIRS) + [s for s in extra_subdirs if s]
    for sub in targets:
        try:
            n = _clean_old_files(base / sub, max_days)
            if n:
                report[sub] = n
        except Exception:
            # Maintenance must never break the app.
            continue
    # Purge legacy feedback screenshots (replaced by SpatialDiff).
    try:
        from .ai.format_registry import purge_old_attachments
        n = purge_old_attachments(max_age_days=14)
        if n:
            report["feedback_attachments"] = n
    except Exception:
        pass
    return report
