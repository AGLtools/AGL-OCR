; ============================================================
;  AGL OCR — Inno Setup installer script
;  Build the .exe folder first:  python -m PyInstaller --noconfirm packaging\AGL_OCR.spec
;  Then compile this .iss with Inno Setup Compiler -> produces dist\AGL_OCR_Setup.exe
; ============================================================

#define MyAppName        "AGL OCR"
#define MyAppVersion     "1.0.0"
#define MyAppPublisher   "Africa Global Logistics"
#define MyAppExeName     "AGL_OCR.exe"

[Setup]
AppId={{B6A1F8E2-2C8B-4A6E-9C4D-AGL0CR000001}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\AGL OCR
DefaultGroupName=AGL OCR
DisableProgramGroupPage=yes
OutputDir=..\dist
OutputBaseFilename=AGL_OCR_Setup_{#MyAppVersion}
Compression=lzma2/ultra
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64
ArchitecturesAllowed=x64
PrivilegesRequired=admin
WizardStyle=modern
SetupIconFile=agl.ico
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "french";  MessagesFile: "compiler:Languages\French.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Whole PyInstaller output folder
Source: "..\dist\AGL_OCR\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}";              Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}";    Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}";        Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#MyAppName}}"; Flags: nowait postinstall skipifsilent
