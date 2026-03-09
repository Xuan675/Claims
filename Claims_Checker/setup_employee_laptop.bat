@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d %~dp0

echo ==============================================
echo Claims Checker - One Shot Installer
echo ==============================================
echo.

where winget >nul 2>&1
if errorlevel 1 (
  echo [ERROR] winget is not available on this laptop.
  echo Install App Installer from Microsoft Store, then rerun this script.
  exit /b 1
)

echo [1/6] Installing Python 3.10 (user scope)...
py -3.10 --version >nul 2>&1
if errorlevel 1 (
  winget install -e --id Python.Python.3.10 --scope user --accept-package-agreements --accept-source-agreements
  if errorlevel 1 (
    echo [ERROR] Failed to install Python 3.10 via winget.
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
winget install -e --id UB-Mannheim.TesseractOCR --scope user --accept-package-agreements --accept-source-agreements
if errorlevel 1 (
  echo Primary Tesseract package not available. Trying alternate package...
  winget install -e --id Tesseract-OCR.Tesseract --scope user --accept-package-agreements --accept-source-agreements
  if errorlevel 1 (
    echo [WARN] Tesseract install failed. Tool can still run for text-based PDFs.
    echo        Scanned/image PDFs may fail until Tesseract is installed.
  )
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
