
#!/bin/bash
echo "--- Data Discovery Backend Setup ---"

# Check if python is installed
if ! command -v python3 &> /dev/null
then
    echo "Python3 could not be found"
    exit
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
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo "!! Please edit .env with your database credentials !!"
fi

echo "--- Starting API Server ---"
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
