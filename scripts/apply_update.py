"""
apply_update.py — pulls a tarball of the AGL-OCR repo at a given ref
and syncs the whitelisted files into the install dir.

This is invoked by UPDATE.bat (CLI) and is functionally identical to
what the GUI updater does. Keeping a CLI version means power users can
run updates from a scheduled task or remote shell.
"""
from __future__ import annotations

import shutil
import ssl
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path

REPO_OWNER     = "AGLtools"
REPO_NAME      = "AGL-OCR"
DEFAULT_BRANCH = "main"

SYNC_INCLUDES = {
    "app.py",
    "src",
    "config",
    "requirements.txt",
    "Lancer_app.bat",
    "PREPARE_ENV.bat",
    "UPDATE.bat",
    "scripts",
    "README.md",
}
IGNORE_DIR_NAMES = {"__pycache__", ".git", ".pytest_cache"}
IGNORE_SUFFIXES  = {".pyc", ".pyo"}


def _download(url: str, dest: Path) -> None:
    print(f"[*] Downloading {url}")
    req = urllib.request.Request(
        url, headers={"User-Agent": "AGL-OCR-Updater/1.0"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
        total = int(r.headers.get("Content-Length") or 0)
        downloaded = 0
        last_pct = -1
        with open(dest, "wb") as f:
            while True:
                chunk = r.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = int(downloaded * 100 / total)
                    if pct != last_pct and pct % 10 == 0:
                        print(f"    {pct}% ({downloaded // 1024} KB)")
                        last_pct = pct
    print(f"[OK] {dest.stat().st_size // 1024} KB downloaded.")


def _sync(src_root: Path, dst_root: Path) -> int:
    count = 0
    for top in SYNC_INCLUDES:
        src = src_root / top
        if not src.exists():
            continue
        dst = dst_root / top
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            count += 1
        else:
            for f in src.rglob("*"):
                rel = f.relative_to(src)
                if any(part in IGNORE_DIR_NAMES for part in rel.parts):
                    continue
                if f.suffix in IGNORE_SUFFIXES:
                    continue
                target = dst / rel
                if f.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, target)
                    count += 1
    return count


def main(argv):
    ref = argv[1] if len(argv) > 1 else DEFAULT_BRANCH
    app_dir = Path(__file__).resolve().parent.parent
    print(f"[*] Install dir: {app_dir}")
    print(f"[*] Ref:         {ref}")

    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/tarball/{ref}"
    with tempfile.TemporaryDirectory(prefix="agl_ocr_upd_") as tmp:
        tmp_path = Path(tmp)
        tar_path = tmp_path / "src.tar.gz"
        _download(url, tar_path)

        print("[*] Extracting archive...")
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()
        with tarfile.open(tar_path, "r:gz") as tf:
            tf.extractall(extract_dir)

        roots = [p for p in extract_dir.iterdir() if p.is_dir()]
        if not roots:
            print("[ERROR] empty tarball")
            return 2
        src_root = roots[0]
        sha = src_root.name.rsplit("-", 1)[-1]
        print(f"[*] Source: {src_root.name}")

        print("[*] Syncing files...")
        n = _sync(src_root, app_dir)
        print(f"[OK] {n} file(s) updated.")

        # Update version file
        try:
            (app_dir / ".current_version").write_text(
                f"{sha}\nref={ref}\nat={time.strftime('%Y-%m-%d %H:%M:%S')}\n",
                encoding="utf-8",
            )
        except Exception as e:
            print(f"[WARN] could not write .current_version: {e}")

        # Update Python deps
        venv_py = app_dir / "venv_agl" / "Scripts" / "python.exe"
        req = app_dir / "requirements.txt"
        if venv_py.exists() and req.exists():
            print("[*] Updating Python packages...")
            import subprocess
            wheelhouse = app_dir / "wheelhouse"
            cmd = [str(venv_py), "-m", "pip", "install", "--upgrade",
                   "--disable-pip-version-check", "-r", str(req)]
            if wheelhouse.exists():
                cmd += ["--find-links", str(wheelhouse)]
            rc = subprocess.call(cmd, cwd=str(app_dir))
            if rc != 0:
                print(f"[WARN] pip exited {rc}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except urllib.error.HTTPError as e:  # type: ignore[name-defined]
        print(f"[ERROR] HTTP {e.code} - {e.reason}")
        sys.exit(3)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
