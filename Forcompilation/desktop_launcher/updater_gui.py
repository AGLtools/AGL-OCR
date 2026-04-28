"""
AGL OCR — Update Manager
========================
Tkinter GUI that lets the user pick a GitHub commit / tag / branch
and pulls the latest source files into the install directory.

Strategy: download the repo as a tarball from GitHub
    https://api.github.com/repos/{owner}/{repo}/tarball/{ref}
extract to a temp folder, then sync ONLY the whitelisted paths
(app.py, src/, config/, requirements.txt, .py modules) into the install
dir. Heavy folders (python_portable, tesseract, poppler, venv_agl) are
NEVER touched, so updates remain very small (a few hundred KB).

After files are synced, the updater also runs `pip install -r requirements.txt`
inside venv_agl so any new Python deps land too.

Build:
    pyinstaller --noconfirm --onefile --windowed ^
        --icon icon.ico --add-data "icon.ico;." ^
        --name "AGL OCR Updater" updater_gui.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import ssl
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

# ─────────────────────────────────────────────────────────────────────
APP_NAME    = "Mise à jour AGL OCR"
APP_VERSION = "1.0.0"

REPO_OWNER      = "AGLtools"
REPO_NAME       = "AGL-OCR"
DEFAULT_BRANCH  = "main"

# Paths inside the repo we want to sync. Anything else is ignored.
SYNC_INCLUDES = {
    "app.py",
    "src",
    "config",
    "requirements.txt",
    "Lancer_app.bat",
    "PREPARE_ENV.bat",
    "UPDATE.bat",
    "README.md",
}
# Inside the synced dirs, ignore these patterns
SYNC_IGNORE_DIR_NAMES = {"__pycache__", ".git", ".pytest_cache"}
SYNC_IGNORE_SUFFIXES  = {".pyc", ".pyo"}

# Brand
NAVY  = "#1A4076"
BLUE  = "#1E90E0"
WHITE = "#FFFFFF"
DARK  = "#0F1F38"
GREEN = "#2A7A2A"
RED   = "#C00000"


def _set_app_user_model_id(appid: str = "AGL.OCR.Updater.1") -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
    except Exception:
        pass


def _no_window_flags() -> int:
    return subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def _si_hidden():
    if os.name != "nt":
        return None
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0
    return si


def resource_path(name: str) -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        p = Path(meipass) / name
        if p.exists():
            return p
    if getattr(sys, "frozen", False):
        p = Path(sys.executable).parent / name
        if p.exists():
            return p
    return Path(__file__).resolve().parent / name


def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parents[2]


def _http_json(url: str, timeout: int = 20):
    req = urllib.request.Request(
        url, headers={"User-Agent": "AGL-OCR-Updater/1.0",
                      "Accept": "application/vnd.github+json"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_download(url: str, dest: Path, log_cb=None):
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
                if log_cb and total:
                    pct = int(downloaded * 100 / total)
                    if pct != last_pct and pct % 10 == 0:
                        log_cb(f"  ↓ {pct}% ({downloaded // 1024} KB)\n")
                        last_pct = pct
        if log_cb:
            log_cb(f"  ✓ downloaded {downloaded // 1024} KB\n")


# ─────────────────────────────────────────────────────────────────────
class UpdaterGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.app_dir = get_app_dir()
        self.versions: list[tuple[str, str, str]] = []  # (label, ref, desc)

        root.title("AGL OCR — Mise à jour")
        root.geometry("780x560")
        root.configure(bg=NAVY)
        root.minsize(700, 500)
        try:
            ico = resource_path("icon.ico")
            if ico.exists():
                root.iconbitmap(default=str(ico))
        except Exception:
            pass

        # ── header ─────────────────────────────────────────────
        header = tk.Frame(root, bg=NAVY)
        header.pack(fill=tk.X, padx=0, pady=(14, 6))
        tk.Label(header, text="AGL OCR — Mise à jour",
                 bg=NAVY, fg=WHITE, font=("Segoe UI", 18, "bold")
                 ).pack(side=tk.LEFT, padx=18)
        self.version_pill = tk.Label(
            header, text=self._read_current_version() or "(aucune version installée)",
            bg=BLUE, fg=WHITE, font=("Segoe UI", 9, "bold"),
            padx=10, pady=3,
        )
        self.version_pill.pack(side=tk.RIGHT, padx=18)

        tk.Frame(root, bg=BLUE, height=2).pack(fill=tk.X)

        # ── selector ───────────────────────────────────────────
        sel = tk.Frame(root, bg=WHITE)
        sel.pack(fill=tk.X, padx=18, pady=14)

        tk.Label(sel, text="Choisir une version à installer :",
                 bg=WHITE, fg=NAVY, font=("Segoe UI", 11, "bold")
                 ).grid(row=0, column=0, sticky=tk.W, pady=(0, 6))

        self.version_var = tk.StringVar(value="Chargement des versions depuis GitHub…")
        self.combo = ttk.Combobox(sel, textvariable=self.version_var,
                                  state="readonly", width=80)
        self.combo.grid(row=1, column=0, sticky=tk.EW, padx=(0, 8))
        self.combo.bind("<<ComboboxSelected>>", self._on_select_version)

        self.refresh_btn = tk.Button(
            sel, text=" ⟳ ", bg="#E0E0E0", fg=NAVY,
            font=("Segoe UI", 11, "bold"), relief=tk.FLAT,
            padx=10, pady=2, cursor="hand2",
            command=self._load_versions_async,
        )
        self.refresh_btn.grid(row=1, column=1, sticky=tk.E)

        sel.columnconfigure(0, weight=1)

        self.desc_var = tk.StringVar(value="")
        tk.Label(sel, textvariable=self.desc_var,
                 bg=WHITE, fg="#666", font=("Segoe UI", 9, "italic"),
                 wraplength=720, justify=tk.LEFT
                 ).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))

        # ── update button ──────────────────────────────────────
        btn_row = tk.Frame(root, bg=WHITE)
        btn_row.pack(fill=tk.X, padx=18)
        self.btn = tk.Button(
            btn_row,
            text="  ⬇  INSTALLER LA MISE À JOUR  ",
            bg=BLUE, fg=WHITE, font=("Segoe UI", 12, "bold"),
            relief=tk.FLAT, padx=18, pady=10, cursor="hand2",
            state=tk.DISABLED,
            command=self.start_update,
        )
        self.btn.pack(side=tk.RIGHT)

        # ── log ────────────────────────────────────────────────
        tk.Label(root, text="Journal d'activité :",
                 bg=NAVY, fg=WHITE, font=("Segoe UI", 9, "bold")
                 ).pack(anchor=tk.W, padx=18, pady=(14, 2))

        log_frame = tk.Frame(root, bg=NAVY)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=18, pady=(0, 8))
        self.log = scrolledtext.ScrolledText(
            log_frame, bg=DARK, fg="#C8E6C9", insertbackground=WHITE,
            font=("Consolas", 9), relief=tk.FLAT, wrap=tk.WORD,
            state=tk.DISABLED, height=14,
        )
        self.log.pack(fill=tk.BOTH, expand=True)

        # ── status bar ─────────────────────────────────────────
        bar = tk.Frame(root, bg="#F0F0F0")
        bar.pack(fill=tk.X, side=tk.BOTTOM)
        self.status = tk.Label(
            bar, text="Prêt.", bg="#F0F0F0", fg=DARK,
            font=("Segoe UI", 9), anchor=tk.W, padx=10, pady=4,
        )
        self.status.pack(fill=tk.X)

        # kick off
        self.root.after(200, self._load_versions_async)

    # ── helpers ────────────────────────────────────────────────
    def _read_current_version(self) -> str:
        try:
            return (self.app_dir / ".current_version").read_text(encoding="utf-8").strip()
        except Exception:
            return ""

    def _write_current_version(self, sha: str, ref: str):
        try:
            (self.app_dir / ".current_version").write_text(
                f"{sha}\nref={ref}\nat={time.strftime('%Y-%m-%d %H:%M:%S')}\n",
                encoding="utf-8")
            self.version_pill.configure(text=sha[:7])
        except Exception:
            pass

    def write_log(self, text: str):
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, text)
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def set_status(self, text: str):
        self.status.configure(text=text)

    # ── load versions ──────────────────────────────────────────
    def _load_versions_async(self):
        self.refresh_btn.configure(state=tk.DISABLED)
        self.btn.configure(state=tk.DISABLED)
        self.version_var.set("Chargement des versions depuis GitHub…")
        self.desc_var.set("")
        self.set_status("Récupération de la liste des versions…")
        threading.Thread(target=self._load_versions, daemon=True).start()

    def _load_versions(self):
        items: list[tuple[str, str, str]] = [
            (f"★ Dernière version (branche {DEFAULT_BRANCH})", DEFAULT_BRANCH,
             f"Pointe toujours sur le dernier commit de la branche '{DEFAULT_BRANCH}'."),
        ]
        try:
            tags = _http_json(
                f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/tags?per_page=20")
            for t in tags:
                name = t.get("name", "")
                sha = ((t.get("commit") or {}).get("sha") or "")[:7]
                if name:
                    items.append((f"Tag : {name}  ({sha})", name,
                                  f"Tag Git \u00ab {name} \u00bb — commit {sha}")
                    )
        except Exception as e:
            self.root.after(0, self.write_log, f"[AVERT] tags : {e}\n")

        try:
            commits = _http_json(
                f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits"
                f"?sha={DEFAULT_BRANCH}&per_page=15")
            for c in commits:
                sha = (c.get("sha") or "")[:7]
                full = c.get("sha") or ""
                msg = ((c.get("commit", {}) or {}).get("message", "") or "").splitlines()[0][:80]
                date = ((c.get("commit", {}) or {}).get("author", {}) or {}).get("date", "")[:10]
                author = ((c.get("commit", {}) or {}).get("author", {}) or {}).get("name", "")
                if sha:
                    items.append((
                        f"Commit {sha}  [{date}]  {msg}",
                        full,
                        f"{author} — {date} — {msg}",
                    ))
        except Exception as e:
            self.root.after(0, self.write_log, f"[AVERT] commits : {e}\n")

        self.root.after(0, self._populate_versions, items)

    def _populate_versions(self, items):
        self.versions = items
        labels = [it[0] for it in items]
        self.combo["values"] = labels
        if labels:
            self.combo.current(0)
            self._on_select_version()
            self.btn.configure(state=tk.NORMAL)
            self.set_status(f"{len(items)} version(s) disponible(s).")
        else:
            self.version_var.set("(aucune version disponible — vérifiez la connexion)")
            self.set_status("Impossible de récupérer la liste. Réessayez plus tard.")
        self.refresh_btn.configure(state=tk.NORMAL)

    def _on_select_version(self, _evt=None):
        idx = self.combo.current()
        if 0 <= idx < len(self.versions):
            self.desc_var.set(self.versions[idx][2])

    # ── do update ──────────────────────────────────────────────
    def start_update(self):
        idx = self.combo.current()
        if not (0 <= idx < len(self.versions)):
            return
        ref = self.versions[idx][1]
        self.btn.configure(state=tk.DISABLED, text="  ⏳  MISE À JOUR EN COURS…  ")
        self.refresh_btn.configure(state=tk.DISABLED)
        self.combo.configure(state=tk.DISABLED)
        self.set_status(f"Installation de '{ref}'…")
        self.write_log(f"\n>>> Cible : {ref}\n")
        threading.Thread(target=self._run_update, args=(ref,), daemon=True).start()

    def _run_update(self, ref: str):
        try:
            url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/tarball/{ref}"
            self.root.after(0, self.write_log, f"[1/4] Téléchargement {url}\n")
            with tempfile.TemporaryDirectory(prefix="agl_ocr_upd_") as tmp:
                tmp_path = Path(tmp)
                tar_path = tmp_path / "src.tar.gz"
                _http_download(url, tar_path,
                               log_cb=lambda l: self.root.after(0, self.write_log, l))

                self.root.after(0, self.write_log, "[2/4] Extraction…\n")
                extract_dir = tmp_path / "extract"
                extract_dir.mkdir()
                with tarfile.open(tar_path, "r:gz") as tf:
                    tf.extractall(extract_dir)
                # tarball contains a single top-level folder "<owner>-<repo>-<sha>/"
                roots = [p for p in extract_dir.iterdir() if p.is_dir()]
                if not roots:
                    raise RuntimeError("Empty tarball")
                src_root = roots[0]
                self.root.after(0, self.write_log,
                                f"  source root: {src_root.name}\n")
                # The directory name ends with the commit SHA (last segment after '-')
                sha = src_root.name.rsplit("-", 1)[-1]

                self.root.after(0, self.write_log,
                                "[3/4] Synchronisation des fichiers sources…\n")
                count = self._sync_files(src_root, self.app_dir)
                self.root.after(0, self.write_log,
                                f"  ✓ {count} fichier(s) mis à jour\n")

                self.root.after(0, self.write_log,
                                "[4/4] Mise à jour des paquets Python…\n")
                self._pip_install_requirements()

                self._write_current_version(sha, ref)
                self.root.after(0, self._update_done, 0, ref, sha)
        except urllib.error.HTTPError as e:
            self.root.after(0, self.write_log,
                            f"[ERREUR] HTTP {e.code} — {e.reason}\n")
            self.root.after(0, self._update_done, 1, ref, "")
        except Exception as e:
            self.root.after(0, self.write_log, f"[ERREUR] {e}\n")
            self.root.after(0, self._update_done, 1, ref, "")

    def _sync_files(self, src_root: Path, dst_root: Path) -> int:
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
                    if any(part in SYNC_IGNORE_DIR_NAMES for part in rel.parts):
                        continue
                    if f.suffix in SYNC_IGNORE_SUFFIXES:
                        continue
                    target = dst / rel
                    if f.is_dir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(f, target)
                        count += 1
        return count

    def _pip_install_requirements(self):
        venv_py = self.app_dir / "venv_agl" / "Scripts" / "python.exe"
        req = self.app_dir / "requirements.txt"
        if not venv_py.exists():
            self.root.after(0, self.write_log,
                            "  (ignoré : venv_agl absent — relancez PREPARE_ENV.bat)\n")
            return
        if not req.exists():
            self.root.after(0, self.write_log, "  (ignoré : requirements.txt absent)\n")
            return
        wheelhouse = self.app_dir / "wheelhouse"
        cmd = [str(venv_py), "-m", "pip", "install", "--upgrade",
               "--disable-pip-version-check", "-r", str(req)]
        if wheelhouse.exists():
            cmd += ["--no-index", "--find-links", str(wheelhouse)]
        try:
            proc = subprocess.Popen(
                cmd, cwd=str(self.app_dir),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                creationflags=_no_window_flags(), startupinfo=_si_hidden(),
            )
            for line in proc.stdout:
                self.root.after(0, self.write_log, "  " + line)
            proc.wait()
            if proc.returncode == 0:
                self.root.after(0, self.write_log, "  ✓ pip install OK\n")
            else:
                self.root.after(0, self.write_log,
                                f"  [AVERT] pip a quitté avec le code {proc.returncode}\n")
        except Exception as e:
            self.root.after(0, self.write_log, f"  [AVERT] pip a échoué : {e}\n")

    def _update_done(self, rc: int, ref: str, sha: str):
        self.combo.configure(state="readonly")
        self.refresh_btn.configure(state=tk.NORMAL)
        if rc == 0:
            self.set_status(f"Mise à jour terminée — installé : {sha[:7] or ref}.")
            self.btn.configure(text="  ✓  TERMINÉ — FERMER  ",
                               bg=GREEN, fg=WHITE, state=tk.NORMAL,
                               command=self.root.destroy)
        else:
            self.set_status(f"Mise à jour échouée (code {rc}).")
            self.btn.configure(text="  ✗  ÉCHEC — RÉESSAYER  ",
                               bg=RED, fg=WHITE, state=tk.NORMAL,
                               command=self.start_update)


def main() -> int:
    _set_app_user_model_id()
    root = tk.Tk()
    UpdaterGUI(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
