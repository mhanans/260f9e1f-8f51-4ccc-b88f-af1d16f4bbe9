#!/bin/bash

# functions for better code organization
function print_highlight() {
    local message="${1}"
    echo "" && echo "******************************************************"
    echo $message
    echo "******************************************************" && echo ""
}

function check_disk_space() {
    # Check for at least 1GB free space (approx needed for venv + libs + spacy lg model)
    local required_space_kb=1048576 # 1GB in KB
    # Use df -k . to get available space in KB for current directory
    local available_space_kb=$(df -k . | awk 'NR==2 {print $4}')

    if [ "$available_space_kb" -lt "$required_space_kb" ]; then
        echo "WARNING: Low disk space detected!"
        echo "Available: $((available_space_kb/1024)) MB"
        echo "Required: ~1000 MB (for dependencies + Spacy large model)"
        echo "Installation might fail. Proceeding anyway..."
        sleep 3
    else
        echo "Disk space check passed. Available: $((available_space_kb/1024)) MB"
    fi
}

function setup_python_env() {
    # 1. Check Python installation
    if ! command -v python3 &> /dev/null; then
        echo "Error: python3 could not be found."
        echo "Attempting to install python3..."
        if command -v apt-get &> /dev/null; then
             if [ "$EUID" -ne 0 ]; then SUDO="sudo"; else SUDO=""; fi
             $SUDO apt-get update && $SUDO apt-get install -y python3
        else
             echo "Please install python3 manually."
             exit 1
        fi
    fi

    local env_dir="env"

    # Check for broken environment (dir exists but activate script missing)
    if [ -d "$env_dir" ] && [ ! -f "$env_dir/bin/activate" ]; then
        echo "Detected broken virtual environment (missing activate script)."
        echo "Removing broken '$env_dir' directory..."
        rm -rf "$env_dir"
    fi

    if [ ! -d "$env_dir" ]; then
        print_highlight "Creating virtual environment in $env_dir..."
        
        # Try creating venv; if it fails, try to install the missing venv package
        if ! python3 -m venv "$env_dir"; then
            echo "----------------------------------------------------------------"
            echo "Venv creation failed. It seems 'python3-venv' is missing."
            echo "Attempting to install it automatically..."
            
            # Get python version MAJOR.MINOR (e.g. 3.12)
            PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
            PKG_NAME="python${PY_VER}-venv"
            
            echo "Detected Python version: $PY_VER"
            echo "Installing packages: $PKG_NAME python3-venv"

            if command -v apt-get &> /dev/null; then
                 # Check for root
                 if [ "$EUID" -ne 0 ]; then
                    SUDO="sudo"
                 else
                    SUDO=""
                 fi
                 
                 echo "Updating apt cache..."
                 $SUDO apt-get update
                 
                 echo "Installing venv package..."
                 # Try specific version first, then generic, or both
                 $SUDO apt-get install -y "$PKG_NAME" python3-venv
                 
                 echo "Retrying venv creation..."
                 python3 -m venv "$env_dir" || {
                     echo "Failed again. Please manually run: $SUDO apt install $PKG_NAME"
                     exit 1
                 }
            else
                echo "Package manager (apt-get) not found. Please manually install python3-venv."
                exit 1
            fi
        fi
    else
        echo "Virtual environment already exists in $env_dir."
    fi

    # 3. Activate environment
    source "$env_dir/bin/activate" || {
        echo "Failed to activate virtual environment."
        exit 1
    }
    
    echo "Environment activated: $VIRTUAL_ENV"
    
    # 4. Upgrade pip
    pip install --upgrade pip
}

function install_dependencies() {
    print_highlight "Installing requirements from requirements.txt..."
    pip install -r requirements.txt || {
        echo "Failed to install dependencies."
        exit 1
    }
    
    # Check if spacy model is already installed to avoid re-downloading huge file
    if ! python3 -c "import spacy; spacy.load('en_core_web_lg')" 2>/dev/null; then
        print_highlight "Downloading Spacy model (en_core_web_lg)..."
        python -m spacy download en_core_web_lg || {
            echo "Failed to download Spacy model. You might be out of disk space."
            echo "Try checking space and running again."
            exit 1
        }
    else
        echo "Spacy model 'en_core_web_lg' already found. Skipping download."
    fi
    
    print_highlight "Installation finished successfully."
}

function launch_app() {
    print_highlight "Launching Data Discovery System"
    
    echo "Starting Backend (FastAPI)..."
    # Run uvicorn in the background
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
    BACKEND_PID=$!
    
    echo "Waiting for backend to start..."
    sleep 5
    
    echo "Starting Dashboard (Streamlit)..."
    # Run streamlit in the foreground
    python -m streamlit run dashboard/app.py
    
    # When streamlit exits, kill the backend
    kill $BACKEND_PID
}

# Main script execution

# Ensure we are in the script's directory
cd "$(dirname "${BASH_SOURCE[0]}")"

check_disk_space
setup_python_env
install_dependencies
launch_app
