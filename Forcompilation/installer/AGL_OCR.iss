; ============================================================
;   AGL OCR - Inno Setup installer
;
;   Layout produced by BUILD_ALL.bat in <repo>\Forcompilation\:
;     desktop_launcher\dist\AGL OCR.exe
;     desktop_launcher\dist\AGL OCR Updater.exe
;     installer\wheelhouse\*.whl
;     python_portable\
;     ..\app.py, ..\src\, ..\config\, ..\requirements.txt
;     ..\poppler\, ..\tesseract\
;
;   Output:  Forcompilation\dist\AGL_OCR_Setup_<version>.exe
; ============================================================

#define MyAppName        "AGL OCR"
#define MyAppVersion     "1.0.0"
#define MyAppPublisher   "Africa Global Logistics"
#define MyAppExeName     "AGL OCR.exe"
#define MyUpdaterExeName "AGL OCR Updater.exe"

[Setup]
AppId={{A6F2C1B9-2D7E-4C8E-9B11-AGLOCR0000A1}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL=https://github.com/AGLtools/AGL-OCR
AppSupportURL=https://github.com/AGLtools/AGL-OCR/issues
AppUpdatesURL=https://github.com/AGLtools/AGL-OCR/releases
DefaultDirName=C:\AGL_tools\AGL OCR
DefaultGroupName=AGL OCR
DisableProgramGroupPage=yes
DisableDirPage=no
OutputDir=dist
OutputBaseFilename=AGL_OCR_Setup_{#MyAppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64compatible
ArchitecturesAllowed=x64compatible
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
WizardStyle=modern
WizardSizePercent=110
SetupIconFile=icon.ico
UninstallDisplayIcon={app}\{#MyAppExeName}
DisableWelcomePage=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "french";  MessagesFile: "compiler:Languages\French.isl"

[Tasks]
Name: "desktopicon";   Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "updatericon";   Description: "Add 'AGL OCR Updater' shortcut on Desktop"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; --- main executables (built by build_exe.bat, output to desktop_launcher\dist\) ---
Source: "..\desktop_launcher\dist\AGL OCR.exe";          DestDir: "{app}"; Flags: ignoreversion
Source: "..\desktop_launcher\dist\AGL OCR Updater.exe";  DestDir: "{app}"; Flags: ignoreversion
Source: "..\icon.ico";                                    DestDir: "{app}"; Flags: ignoreversion

; --- Python source (kept as plain .py so updater can hot-swap) ---
Source: "..\..\app.py";                              DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\requirements.txt";                    DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\src\*";                               DestDir: "{app}\src";     Flags: ignoreversion recursesubdirs createallsubdirs
Source: "..\..\config\*";                            DestDir: "{app}\config";  Flags: ignoreversion recursesubdirs createallsubdirs
Source: "..\..\scripts\*";                           DestDir: "{app}\scripts"; Flags: ignoreversion recursesubdirs createallsubdirs

; --- helper scripts (same folder as the .iss) ---
Source: "PREPARE_ENV.bat";              DestDir: "{app}"; Flags: ignoreversion
Source: "UPDATE.bat";                   DestDir: "{app}"; Flags: ignoreversion
Source: "Lancer_app.bat";              DestDir: "{app}"; Flags: ignoreversion

; --- portable Python (large, lives in Forcompilation\) ---
Source: "..\python_portable\*";                      DestDir: "{app}\python_portable"; Flags: ignoreversion recursesubdirs createallsubdirs

; --- offline wheelhouse (lives in installer\wheelhouse\) ---
Source: "wheelhouse\*";                              DestDir: "{app}\wheelhouse";      Flags: ignoreversion recursesubdirs createallsubdirs skipifsourcedoesntexist

; --- bundled Tesseract (~150 MB, repo root) ---
Source: "..\..\tesseract\*";                         DestDir: "{app}\tesseract";       Flags: ignoreversion recursesubdirs createallsubdirs skipifsourcedoesntexist

; --- bundled Poppler (~50 MB, repo root) ---
Source: "..\..\poppler\*";                           DestDir: "{app}\poppler";         Flags: ignoreversion recursesubdirs createallsubdirs skipifsourcedoesntexist

[Icons]
Name: "{group}\{#MyAppName}";              Filename: "{app}\{#MyAppExeName}";        IconFilename: "{app}\icon.ico"
Name: "{group}\{#MyAppName} Updater";      Filename: "{app}\{#MyUpdaterExeName}";    IconFilename: "{app}\icon.ico"
Name: "{group}\Uninstall {#MyAppName}";    Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}";        Filename: "{app}\{#MyAppExeName}";        IconFilename: "{app}\icon.ico"; Tasks: desktopicon
Name: "{autodesktop}\{#MyAppName} Updater"; Filename: "{app}\{#MyUpdaterExeName}";   IconFilename: "{app}\icon.ico"; Tasks: updatericon

[Run]
; Step 1: build venv + install packages from wheelhouse (silent, no pause)
; We invoke cmd /c to set AGL_SILENT before calling the bat.
Filename: "cmd.exe"; Parameters: "/c set AGL_SILENT=1 && ""{app}\PREPARE_ENV.bat"""; \
    StatusMsg: "Setting up Python environment (this can take a few minutes)..."; \
    Flags: runhidden waituntilterminated

; Step 2: optional first launch
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#MyAppName}}"; \
    Flags: nowait postinstall skipifsilent

[UninstallDelete]
; Wipe generated artefacts so reinstalls start clean
Type: filesandordirs; Name: "{app}\venv_agl"
Type: filesandordirs; Name: "{app}\__pycache__"
Type: files;          Name: "{app}\.current_version"


