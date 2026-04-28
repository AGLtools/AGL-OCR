"""
AGL OCR — Desktop Launcher
==========================
A small Tkinter splash that bootstraps the Python environment if needed,
then runs `app.py` (the PyQt5 main window) using the bundled portable Python.

Build:
    pyinstaller --noconfirm --onefile --windowed ^
        --icon icon.ico --add-data "icon.ico;." ^
        --name "AGL OCR" launcher.py

Layout expected at runtime (the .exe sits in this folder):
    AGL OCR.exe        ← this launcher
    AGL OCR Updater.exe
    icon.ico
    app.py
    src/
    config/
    requirements.txt
    PREPARE_ENV.bat
    Lancer_app.bat
    python_portable/
    poppler/
    tesseract/
    venv_agl/          (created on first launch by PREPARE_ENV.bat)
"""
from __future__ import annotations

import os
import sys
import time
import subprocess
import threading
import tempfile
from pathlib import Path

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

APP_NAME    = "AGL OCR"
APP_VERSION = "1.0.0"

# AGL brand colours (match logo)
NAVY  = "#1A4076"
BLUE  = "#1E90E0"
WHITE = "#FFFFFF"
GOLD  = "#E5A823"
DARK  = "#0F1F38"
GREEN = "#2A7A2A"
RED   = "#C00000"

# ── optional high-res icon via PIL ──────────────────────────────────
try:
    from PIL import Image, ImageTk as _ImageTk
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

LOG_FILE = Path(tempfile.gettempdir()) / "agl_ocr_launch.log"


def _set_app_user_model_id(appid: str = "AGL.OCR.Launcher.1") -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
    except Exception:
        pass


def _si_hidden() -> "subprocess.STARTUPINFO | None":
    if os.name != "nt":
        return None
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0  # SW_HIDE
    return si


def _no_window_flags() -> int:
    return subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def _set_tk_icon(root: "tk.Tk") -> None:
    """Set high-res window + taskbar icon (PIL preferred for sharpness)."""
    if _PIL_OK:
        for candidate in ["AGL_logo.png"]:
            p = resource_path(candidate)
            try:
                if p.exists():
                    img = Image.open(str(p)).convert("RGBA")
                    # provide 256×256 → Windows picks the best size for taskbar
                    img256 = img.resize((256, 256), Image.LANCZOS)
                    photo = _ImageTk.PhotoImage(img256)
                    root.iconphoto(True, photo)
                    root._agl_icon_ref = photo  # keep reference, prevent GC
                    return
            except Exception:
                pass
    # Fallback: .ico embedded by PyInstaller
    ico = resource_path("icon.ico")
    try:
        if ico.exists():
            root.iconbitmap(default=str(ico))
    except Exception:
        pass


def resource_path(name: str) -> Path:
    """Locate a bundled resource (PyInstaller _MEIPASS or dev folder)."""
    candidates = []
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.append(Path(meipass) / name)
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).parent / name)
    candidates.append(Path(__file__).resolve().parent / name)
    for c in candidates:
        try:
            if c.exists():
                return c
        except Exception:
            pass
    return candidates[-1]


def get_app_dir() -> Path:
    """Folder that holds app.py / venv_agl / python_portable."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    # Dev mode — assume launcher.py lives in <repo>/Forcompilation/desktop_launcher/
    return Path(__file__).resolve().parents[2]


def find_python(app_dir: Path) -> Path:
    """Pick the best available Python: venv > portable > system."""
    candidates = [
        app_dir / "venv_agl" / "Scripts" / "python.exe",
        app_dir / "python_portable" / "python.exe",
        app_dir / ".venv" / "Scripts" / "python.exe",  # dev fallback
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        "No Python interpreter found.\n"
        "Run PREPARE_ENV.bat to install the environment."
    )


def env_is_ready(app_dir: Path) -> bool:
    """Check whether venv_agl exists AND has PyQt5 installed."""
    venv_py = app_dir / "venv_agl" / "Scripts" / "python.exe"
    if not venv_py.exists():
        return False
    try:
        r = subprocess.run(
            [str(venv_py), "-c", "import PyQt5, pdfplumber, pytesseract, openpyxl"],
            capture_output=True, text=True, timeout=15,
            creationflags=_no_window_flags(),
            startupinfo=_si_hidden(),
        )
        return r.returncode == 0
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────
#  GUI
# ─────────────────────────────────────────────────────────────────────
class LauncherGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.app_dir = get_app_dir()
        self.proc: subprocess.Popen | None = None

        root.title(APP_NAME)
        root.geometry("520x340")
        root.configure(bg=NAVY)
        root.resizable(False, False)
        _set_tk_icon(root)

        # Header band
        header = tk.Frame(root, bg=NAVY)
        header.pack(fill=tk.X, pady=(20, 0))
        tk.Label(header, text="AGL OCR", bg=NAVY, fg=WHITE,
                 font=("Segoe UI", 28, "bold")).pack()
        tk.Label(header,
                 text="Intelligence documentaire maritime",
                 bg=NAVY, fg=BLUE,
                 font=("Segoe UI", 11, "italic")).pack(pady=(2, 6))
        tk.Frame(root, bg=BLUE, height=2).pack(fill=tk.X, pady=(8, 0))

        # Body
        body = tk.Frame(root, bg=WHITE)
        body.pack(fill=tk.BOTH, expand=True)

        self.status_var = tk.StringVar(value="Initialisation…")
        tk.Label(body, textvariable=self.status_var,
                 bg=WHITE, fg=NAVY, font=("Segoe UI", 11)).pack(pady=(20, 8))

        self.bar = ttk.Progressbar(body, mode="indeterminate", length=380)
        self.bar.pack(pady=(0, 14))
        self.bar.start(12)

        self.detail_var = tk.StringVar(value="")
        tk.Label(body, textvariable=self.detail_var,
                 bg=WHITE, fg="#666", font=("Segoe UI", 9)).pack()

        btn_row = tk.Frame(body, bg=WHITE)
        btn_row.pack(pady=14)
        self.show_log_btn = tk.Button(
            btn_row, text="Voir le journal", bg="#E0E0E0", fg=NAVY,
            font=("Segoe UI", 9), relief=tk.FLAT, padx=10, pady=4,
            cursor="hand2", command=self._open_log_window,
        )
        self.show_log_btn.pack(side=tk.LEFT, padx=4)
        self.cancel_btn = tk.Button(
            btn_row, text="Annuler", bg="#F0F0F0", fg=DARK,
            font=("Segoe UI", 9), relief=tk.FLAT, padx=10, pady=4,
            cursor="hand2", command=self._cancel,
        )
        self.cancel_btn.pack(side=tk.LEFT, padx=4)

        # Footer
        footer = tk.Frame(root, bg="#F0F0F0", height=24)
        footer.pack(fill=tk.X, side=tk.BOTTOM)
        tk.Label(footer, text=f"v{APP_VERSION}", bg="#F0F0F0", fg="#888",
                 font=("Segoe UI", 8)).pack(side=tk.LEFT, padx=8)
        tk.Label(footer, text="© Africa Global Logistics",
                 bg="#F0F0F0", fg="#888", font=("Segoe UI", 8)).pack(side=tk.RIGHT, padx=8)

        self._log_lines: list[str] = []
        self._log_window: tk.Toplevel | None = None
        self._log_widget: scrolledtext.ScrolledText | None = None

        # Start the launch chain
        self.root.after(150, lambda: threading.Thread(
            target=self._launch_chain, daemon=True).start())

    # ── log helpers ─────────────────────────────────────────────
    def _log(self, line: str):
        self._log_lines.append(line.rstrip())
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line.rstrip() + "\n")
        except Exception:
            pass
        if self._log_widget is not None:
            try:
                self._log_widget.configure(state=tk.NORMAL)
                self._log_widget.insert(tk.END, line.rstrip() + "\n")
                self._log_widget.see(tk.END)
                self._log_widget.configure(state=tk.DISABLED)
            except Exception:
                pass

    def _open_log_window(self):
        if self._log_window and self._log_window.winfo_exists():
            self._log_window.lift()
            return
        w = tk.Toplevel(self.root)
        w.title(f"{APP_NAME} — journal")
        _set_tk_icon(w)
        w.geometry("780x420")
        w.configure(bg=DARK)
        sc = scrolledtext.ScrolledText(
            w, bg=DARK, fg="#C8E6C9", insertbackground=WHITE,
            font=("Consolas", 9), relief=tk.FLAT, wrap=tk.WORD,
        )
        sc.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        sc.insert(tk.END, "\n".join(self._log_lines) + "\n")
        sc.see(tk.END)
        sc.configure(state=tk.DISABLED)
        self._log_window = w
        self._log_widget = sc

    def _set_status(self, status: str, detail: str = ""):
        def _do():
            self.status_var.set(status)
            self.detail_var.set(detail)
        self.root.after(0, _do)

    def _cancel(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
            except Exception:
                pass
        self.root.destroy()

    # ── launch sequence ─────────────────────────────────────────
    def _launch_chain(self):
        try:
            self._set_status("Vérification de l'environnement…", str(self.app_dir))
            self._log(f"Dossier app : {self.app_dir}")
            app_py = self.app_dir / "app.py"
            if not app_py.exists():
                self._fail(
                    "app.py introuvable",
                    f"Attendu à {app_py}\n\n"
                    "L'installation est incomplète. Réinstallez AGL OCR."
                )
                return

            if not env_is_ready(self.app_dir):
                self._set_status("Premier lancement — installation des paquets…",
                                 "Cela peut prendre quelques minutes (opération unique)")
                self._log("venv_agl absent ou incomplet — lancement de PREPARE_ENV.bat")
                self._run_prepare_env()
                if not env_is_ready(self.app_dir):
                    self._fail(
                        "Echec de la configuration",
                        "Impossible d'installer les paquets Python. "
                        "Consultez le journal pour plus de détails."
                    )
                    return

            # Lance l'application PyQt5
            self._set_status("Démarrage d'AGL OCR…", "")
            self._launch_app()
        except Exception as e:
            self._log(f"[FATAL] {e}")
            self._fail("Launch failed", str(e))

    def _run_prepare_env(self):
        bat = self.app_dir / "PREPARE_ENV.bat"
        if not bat.exists():
            raise FileNotFoundError(f"PREPARE_ENV.bat introuvable : {bat}")
        proc = subprocess.Popen(
            ["cmd.exe", "/c", str(bat)],
            cwd=str(self.app_dir),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            creationflags=_no_window_flags(),
            startupinfo=_si_hidden(),
        )
        for line in proc.stdout:
            self._log(line)
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"PREPARE_ENV.bat exited with code {proc.returncode}")
        self._log("[OK] Environnement prêt.")

    def _launch_app(self):
        py = find_python(self.app_dir)
        self._log(f"Python: {py}")
        cmd = [str(py), str(self.app_dir / "app.py")]
        # Detach so the GUI doesn't block; suppress console window
        try:
            self.proc = subprocess.Popen(
                cmd, cwd=str(self.app_dir),
                creationflags=_no_window_flags(),
                startupinfo=_si_hidden(),
            )
        except Exception as e:
            self._fail("Impossible de démarrer app.py", str(e))
            return
        # Laisse Qt le temps d'afficher la fenêtre avant de fermer le splash
        self._set_status("Chargement de l'interface…", "")
        time.sleep(2.0)
        self.root.after(0, self.root.destroy)

    def _fail(self, title: str, message: str):
        def _show():
            try:
                self.bar.stop()
            except Exception:
                pass
            self.status_var.set("✗  " + title)
            self.detail_var.set(message[:120])
            self._log(f"[ECHEC] {title}: {message}")
            self._open_log_window()
            messagebox.showerror(APP_NAME, f"{title}\n\n{message}")
        self.root.after(0, _show)


def main() -> int:
    _set_app_user_model_id()
    # Reset log file at each launch
    try:
        LOG_FILE.write_text("", encoding="utf-8")
    except Exception:
        pass
    root = tk.Tk()
    LauncherGUI(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
