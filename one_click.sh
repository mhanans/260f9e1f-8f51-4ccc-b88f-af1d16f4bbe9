#!/bin/bash

# functions for better code organization
function print_highlight() {
    local message="${1}"
    echo "" && echo "******************************************************"
    echo $message
    echo "******************************************************" && echo ""
}

function check_disk_space() {
    # Check for at least 1GB free space
    local required_space_kb=1048576 # 1GB in KB
    local available_space_kb=$(df -k . | awk 'NR==2 {print $4}')

    if [ "$available_space_kb" -lt "$required_space_kb" ]; then
        echo "WARNING: Low disk space detected!"
        echo "Available: $((available_space_kb/1024)) MB"
        echo "Required: ~1000 MB"
        echo "Installation might fail. Proceeding anyway..."
        sleep 3
    else
        echo "Disk space check passed. Available: $((available_space_kb/1024)) MB"
    fi
}

function setup_python_env() {
    local target_ver="3.13"
    local python_exec="python${target_ver}"
    local venv_pkg="${python_exec}-venv"

    print_highlight "Checking for Python ${target_ver}..."

    # 1. Check Python installation
    if ! command -v "$python_exec" &> /dev/null; then
        echo "Python ${target_ver} not found. Attempting to install via PPA..."
        
        if command -v apt-get &> /dev/null; then
             # Suggest sudo if not root
             if [ "$EUID" -ne 0 ]; then SUDO="sudo"; else SUDO=""; fi
             
             # Install software-properties-common for add-apt-repository
             $SUDO apt-get update
             $SUDO apt-get install -y software-properties-common
             
             # Add deadsnakes PPA which usually has the bleeding edge python versions
             echo "Adding deadsnakes PPA..."
             $SUDO add-apt-repository -y ppa:deadsnakes/ppa
             $SUDO apt-get update
             
             echo "Installing ${python_exec} and ${venv_pkg}..."
             $SUDO apt-get install -y "$python_exec" "$venv_pkg" || {
                 echo "Failed to install ${python_exec}. It might not be available for your distro release yet."
                 echo "Falling back to system default python3..."
                 python_exec="python3"
             }
        else
             echo "Apt not found. Cannot auto-install Python ${target_ver}."
             echo "Falling back to system default python3..."
             python_exec="python3"
        fi
    fi

    local env_dir="env"

    # Check for broken environment (dir exists but activate script missing)
    if [ -d "$env_dir" ] && [ ! -f "$env_dir/bin/activate" ]; then
        echo "Detected broken virtual environment. Recreating..."
        rm -rf "$env_dir"
    fi
    
    if [ ! -d "$env_dir" ]; then
        print_highlight "Creating virtual environment in $env_dir using $python_exec..."
        
        if ! "$python_exec" -m venv "$env_dir"; then
             echo "Failed to create venv. Attempting to install venv package for $python_exec..."
             if [ "$EUID" -ne 0 ]; then SUDO="sudo"; else SUDO=""; fi
             
             # Try to install the venv package matching the chosen python
             # Extract version if we fell back to 'python3'
             if [ "$python_exec" == "python3" ]; then
                 PY_FULL=$($python_exec -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
                 venv_pkg="python${PY_FULL}-venv"
             fi
             
             $SUDO apt-get install -y "$venv_pkg"
             
             echo "Retrying..."
             "$python_exec" -m venv "$env_dir" || {
                 echo "Failed to create virtual environment. Aborting."
                 exit 1
             }
        fi
    else
        echo "Virtual environment already exists."
    fi

    # 3. Activate environment
    source "$env_dir/bin/activate" || {
        echo "Failed to activate virtual environment."
        exit 1
    }
    
    echo "Environment activated: $VIRTUAL_ENV"
    
    # 4. Upgrade pip
    print_highlight "Upgrading pip to latest..."
    pip install --upgrade pip
}

function install_docker() {
    print_highlight "Checking Docker installation..."
    
    if command -v docker &> /dev/null; then
        echo "Docker is already installed."
        return 0
    fi
    
    echo "Docker not found. Attempting to install..."
    
    if command -v apt-get &> /dev/null; then
        if [ "$EUID" -ne 0 ]; then SUDO="sudo"; else SUDO=""; fi
        
        echo "Updating apt..."
        $SUDO apt-get update
        
        echo "Installing docker.io and docker-compose..."
        $SUDO apt-get install -y docker.io docker-compose || {
            echo "Failed to install docker via apt."
            return 1
        }
        
        # Add current user to docker group to avoid sudo requirements later
        # (Only relevant if not running as root)
        if [ "$EUID" -ne 0 ]; then
             echo "Adding user $USER to docker group..."
             $SUDO usermod -aG docker $USER
             echo "NOTE: You may need to logout and login again for group changes to take effect."
        fi
        
        echo "Docker installed successfully."
    else
        echo "Package manager not supported (apt-get not found). Please install Docker manually."
        exit 1
    fi
}

function start_infrastructure() {
    install_docker

    print_highlight "Starting Infrastructure (Docker)..."
    
    # Check if docker daemon is running
    if ! docker info &> /dev/null; then
        echo "Docker daemon is not running. Attempting to start..."
        if [ "$EUID" -ne 0 ]; then SUDO="sudo"; else SUDO=""; fi
        
        # Try service command first
        if command -v service &> /dev/null; then
            $SUDO service docker start
        elif command -v systemctl &> /dev/null; then
            $SUDO systemctl start docker
        else
            # WSL 2 fallback mostly
            if [ -f "/etc/init.d/docker" ]; then
                 $SUDO /etc/init.d/docker start
            fi
        fi
        
        sleep 3
        
        if ! docker info &> /dev/null; then
            echo "Error: Failed to start Docker daemon automatically."
            echo "Please ensure Docker Desktop is running (if on Windows/WSL) or run 'sudo service docker start'."
            exit 1
        fi
    fi
    
    # Start DB and Redis
    echo "Starting PostgreSQL and Redis..."
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d db redis
    else
        docker compose up -d db redis
    fi
    
    # Wait for DB to be ready
    echo "Waiting for Database to be ready..."
    until docker exec $(docker ps -qf "name=db") pg_isready -U admin > /dev/null 2>&1; do
        echo -n "."
        sleep 2
    done
    echo " Database is ready!"
}

function install_dependencies() {
    print_highlight "Installing latest dependencies..."
    # -U upgrades everything to the latest version found in PyPI
    pip install -U -r requirements.txt || {
        echo "Failed to install dependencies."
        exit 1
    }
    
    # Spacy model
    if ! python -c "import spacy; spacy.load('en_core_web_lg')" 2>/dev/null; then
        print_highlight "Downloading Spacy model (en_core_web_lg)..."
        python -m spacy download en_core_web_lg || {
            echo "Failed to download Spacy model."
            exit 1
        }
    else
        echo "Spacy model already installed."
    fi
    
    print_highlight "Installation finished successfully."
}

function launch_app() {
    print_highlight "Launching Data Discovery System"
    
    echo "Starting Backend (FastAPI)..."
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
    BACKEND_PID=$!
    
    echo "Waiting for backend to start..."
    sleep 5
    
    echo "Starting Dashboard (Streamlit)..."
    python -m streamlit run dashboard/app.py
    
    kill $BACKEND_PID
}

# Main script execution
cd "$(dirname "${BASH_SOURCE[0]}")"

check_disk_space
setup_python_env
start_infrastructure
install_dependencies
launch_app
