@echo off
setlocal

:: Switch to the script's directory
cd /d "%~dp0"

echo ======================================================
echo    Data Discovery System - One Click Run
echo ======================================================

:: Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH.
    echo Please install Python 3.10+ and try again.
    pause
    exit /b 1
)

:: Create virtual environment if it doesn't exist
if not exist "env" (
    echo Creating virtual environment...
    python -m venv env
)

:: Activate environment
call env\Scripts\activate

:: Install requirements
echo Installing dependencies...
python -m pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo Failed to install dependencies.
    pause
    exit /b 1
)

:: Download Spacy model
echo Checking/Downloading Spacy model...
python -m spacy download en_core_web_lg

:: Run Application
echo Starting Backend (FastAPI)...
start "Data Discovery Backend" cmd /k "python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload"

echo Starting Dashboard (Streamlit)...
:: Wait a bit for backend
timeout /t 5 >nul
python -m streamlit run dashboard\app.py

echo.
echo Dashboard closed.
pause
