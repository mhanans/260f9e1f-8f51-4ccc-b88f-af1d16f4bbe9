
@echo off
echo --- Data Discovery Backend Setup ---

IF NOT EXIST venv (
    echo Creating virtual environment...
    python -m venv venv
)

call venv\Scripts\activate

echo Installing dependencies...
pip install -r requirements.txt

IF NOT EXIST .env (
    echo ----------------------------------------------------------------
    echo Creating .env from .env.example...
    copy .env.example .env
    echo !!! IMPORTANT: A new .env file has been created. !!!
    echo !!! Please edit .env with your database credentials before proceeding. !!!
    echo ----------------------------------------------------------------
    pause
)

echo --- Starting API Server ---
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
