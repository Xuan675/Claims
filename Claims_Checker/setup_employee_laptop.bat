@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d %~dp0

echo ==============================================
echo Claims Checker - One Shot Installer
echo ==============================================
echo.

set "HAS_WINGET=1"
where winget >nul 2>&1
if errorlevel 1 (
  set "HAS_WINGET=0"
  echo [WARN] winget is not available on this laptop.
  echo        Auto-install of Python/Tesseract via winget will be skipped.
  echo.
)

echo [1/6] Installing Python 3.10 (user scope)...
py -3.10 --version >nul 2>&1
if errorlevel 1 (
  if "%HAS_WINGET%"=="1" (
    winget install -e --id Python.Python.3.10 --scope user --accept-package-agreements --accept-source-agreements
    if errorlevel 1 (
      echo [ERROR] Failed to install Python 3.10 via winget.
      exit /b 1
    )
  ) else (
    echo [ERROR] Python 3.10 is not installed and winget is unavailable.
    echo        Ask IT to install Python 3.10 x64 from:
    echo        https://www.python.org/downloads/windows/
    exit /b 1
  )
) else (
  echo Python 3.10 already installed.
)

set "PY_EXE="
py -3.10 --version >nul 2>&1
if not errorlevel 1 set "PY_EXE=py -3.10"
if not defined PY_EXE (
  if exist "%LocalAppData%\Programs\Python\Python310\python.exe" set "PY_EXE=%LocalAppData%\Programs\Python\Python310\python.exe"
)
if not defined PY_EXE (
  echo [ERROR] Python 3.10 executable not found after install.
  exit /b 1
)

echo [2/6] Installing Tesseract OCR...
set "TESS_EXE="
where tesseract >nul 2>&1
if not errorlevel 1 (
  for /f "delims=" %%I in ('where tesseract') do (
    if not defined TESS_EXE set "TESS_EXE=%%I"
  )
)
if not defined TESS_EXE (
  if exist "%ProgramFiles%\Tesseract-OCR\tesseract.exe" set "TESS_EXE=%ProgramFiles%\Tesseract-OCR\tesseract.exe"
)
if not defined TESS_EXE (
  if exist "%LocalAppData%\Programs\Tesseract-OCR\tesseract.exe" set "TESS_EXE=%LocalAppData%\Programs\Tesseract-OCR\tesseract.exe"
)

if defined TESS_EXE (
  echo Tesseract already installed: %TESS_EXE%
) else (
  if "%HAS_WINGET%"=="1" (
    winget install -e --id UB-Mannheim.TesseractOCR --scope user --accept-package-agreements --accept-source-agreements
    if errorlevel 1 (
      echo Primary Tesseract package not available. Trying alternate package...
      winget install -e --id Tesseract-OCR.Tesseract --scope user --accept-package-agreements --accept-source-agreements
    )
  ) else (
    echo winget unavailable. Downloading Tesseract installer directly...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $urls=@('https://digi.bib.uni-mannheim.de/tesseract/tesseract-ocr-w64-setup-5.5.0.20241111.exe','https://github.com/tesseract-ocr/tesseract/releases/download/5.5.0/tesseract-ocr-w64-setup-5.5.0.20241111.exe'); $dst=Join-Path $env:TEMP 'tesseract-ocr-setup.exe'; $ok=$false; foreach($u in $urls){ try{ Invoke-WebRequest -Uri $u -OutFile $dst -UseBasicParsing; if((Get-Item $dst).Length -gt 0){ $ok=$true; break } } catch {} }; if(-not $ok){ throw 'Could not download Tesseract installer.' }; Start-Process -FilePath $dst -ArgumentList '/S' -Wait; Remove-Item $dst -Force -ErrorAction SilentlyContinue"
    if errorlevel 1 (
      echo [WARN] Direct Tesseract download/install failed.
    )
  )
)

set "TESS_EXE="
where tesseract >nul 2>&1
if not errorlevel 1 (
  for /f "delims=" %%I in ('where tesseract') do (
    if not defined TESS_EXE set "TESS_EXE=%%I"
  )
)
if not defined TESS_EXE (
  if exist "%ProgramFiles%\Tesseract-OCR\tesseract.exe" set "TESS_EXE=%ProgramFiles%\Tesseract-OCR\tesseract.exe"
)
if not defined TESS_EXE (
  if exist "%LocalAppData%\Programs\Tesseract-OCR\tesseract.exe" set "TESS_EXE=%LocalAppData%\Programs\Tesseract-OCR\tesseract.exe"
)
if not defined TESS_EXE (
  echo [WARN] Tesseract install not detected. Tool can still run for text-based PDFs.
  echo        Scanned/image PDFs may fail until Tesseract is installed.
  echo        If corporate policy blocks downloads, ask IT to install Tesseract OCR.
)

echo [3/6] Creating virtual environment (.venv310)...
if exist .venv310 (
  echo Existing .venv310 found. Reusing it.
) else (
  call %PY_EXE% -m venv .venv310
  if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    exit /b 1
  )
)

echo [4/6] Installing Python dependencies...
call .venv310\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
  echo [ERROR] Failed to upgrade pip/setuptools/wheel.
  exit /b 1
)

call .venv310\Scripts\python.exe -m pip install streamlit pandas openpyxl pymupdf pytesseract pypdf extract-msg msoffcrypto-tool xlrd pyxlsb pillow
if errorlevel 1 (
  echo [ERROR] Failed to install required Python packages.
  exit /b 1
)

echo [5/6] Validating imports...
call .venv310\Scripts\python.exe -c "import streamlit,pandas,openpyxl,fitz,pytesseract,pypdf,extract_msg,msoffcrypto,xlrd,pyxlsb,PIL; print('Dependency check: OK')"
if errorlevel 1 (
  echo [ERROR] Dependency validation failed.
  exit /b 1
)

echo [6/6] Done.
echo.
echo Run the tool with:
echo   run_app.bat
echo.
echo If scanned PDFs are not detected, restart the laptop once to refresh PATH for Tesseract.
exit /b 0
