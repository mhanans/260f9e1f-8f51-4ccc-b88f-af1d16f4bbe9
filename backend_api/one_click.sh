
#!/bin/bash
echo "--- Data Discovery Backend Setup ---"

# Strict error handling
set -e

# Check if python is installed
if ! command -v python3 &> /dev/null
then
    echo "Error: Python3 could not be found."
    exit 1
fi

# Create venv if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env if not exists
if [ ! -f ".env" ]; then
    echo "----------------------------------------------------------------"
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo "!!! IMPORTANT: A new .env file has been created. !!!"
    echo "!!! Please edit .env with your database credentials before proceeding. !!!"
    echo "----------------------------------------------------------------"
    read -p "Press Enter to continue after you have edited the .env file (or Ctrl+C to exit)..."
fi

echo "--- Starting API Server ---"
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
