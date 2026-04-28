# AGL OCR — Build & Deployment

Everything in this folder produces the final installer
`Forcompilation\dist\AGL_OCR_Setup_<version>.exe`.

## One-click build

```
BUILD_ALL.bat
```

This chains three steps:

| Step | What it does                                      | Script                                |
| ---- | ------------------------------------------------- | ------------------------------------- |
| 1    | Build `AGL OCR.exe` + `AGL OCR Updater.exe`       | `desktop_launcher\build_exe.bat`      |
| 2    | Re-download every Python dependency wheel offline | `installer\make_wheelhouse.bat`       |
| 3    | Compile the Inno Setup installer                  | `installer\AGL_OCR.iss`               |

### Prerequisites

- **Python 3.11+** in `C:\AGL_OCR\.venv\` (used to build the .exe)
- **Inno Setup 6** installed at the default location, or path set in `.build_config.json`
- The folders `..\tesseract\` and `..\poppler\` must exist (they get bundled into the installer)
- `..\python_portable\` is shipped via `Forcompilation\python_portable\` (already present)

## Architecture

The installed app under `C:\AGL_tools\AGL OCR\` looks like:

```
AGL OCR.exe                  ← launcher (PyInstaller, ~10 MB)
AGL OCR Updater.exe          ← updater  (PyInstaller, ~10 MB)
icon.ico
app.py                       ← plain .py — updateable via the Updater
src\                         ← plain .py — updateable
config\                      ← YAML — updateable
scripts\apply_update.py
PREPARE_ENV.bat              ← runs once at install time
UPDATE.bat                   ← used by the Updater (or manually)
Lancer_app.bat               ← fallback manual launcher
python_portable\             ← bundled Python 3.14 (~50 MB)
venv_agl\                    ← created by PREPARE_ENV.bat (~150 MB)
wheelhouse\                  ← offline .whl cache
poppler\                     ← bundled binaries (~50 MB)
tesseract\                   ← bundled binaries (~150 MB)
.current_version             ← installed git SHA (set by Updater)
```

### Why is `app.py` not bundled inside the .exe?

Because we want **hot updates from GitHub** without re-installing.
Push a fix here in this repo → users click **AGL OCR Updater** → the
updater downloads the latest tarball from GitHub and overwrites the
`.py` files in-place. The next launch picks them up. No PyInstaller
rebuild needed.

The `.exe` files only need to change when the launcher / updater
logic itself changes.

## Updater details

`AGL OCR Updater.exe` calls the GitHub REST API to list:

- `★ Latest (main branch)` — always the most recent commit
- All published tags
- The 15 most recent commits on `main`

It downloads `https://api.github.com/repos/AGLtools/AGL-OCR/tarball/<ref>`,
extracts it, and copies these whitelisted paths into the install dir:

- `app.py`
- `src/`
- `config/`
- `scripts/`
- `requirements.txt`
- `Lancer_app.bat`, `PREPARE_ENV.bat`, `UPDATE.bat`
- `README.md`

Then it runs `pip install --upgrade -r requirements.txt` inside `venv_agl`
(using the local wheelhouse first) so any new Python deps land too.

Heavy folders (`python_portable/`, `tesseract/`, `poppler/`, `venv_agl/`)
are **never** touched by the updater.

## Brand

| Token | Value     | Where                             |
| ----- | --------- | --------------------------------- |
| Navy  | `#1A4076` | App background, header, status bar |
| Blue  | `#1E90E0` | Toolbar accent, focus, buttons     |
| White | `#FFFFFF` | Splash text, log foreground        |
| Gold  | `#E5A823` | Reserved for warnings / call-outs  |

## Repository on GitHub

- Owner : `AGLtools`
- Repo  : `AGL-OCR`
- Branch: `main`

After cloning fresh, run:

```
git remote add origin https://github.com/AGLtools/AGL-OCR.git
git push -u origin main
```
