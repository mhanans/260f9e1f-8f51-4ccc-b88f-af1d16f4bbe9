#!/bin/bash

# functions for better code organization
function check_path_for_spaces() {
    if [[ $PWD =~ \  ]]; then
        echo "The current workdir has whitespace which can lead to unintended behaviour. Please modify your path and continue later."
        exit 1
    fi
}

function install_miniconda() {
    # Miniconda installer is limited to two main architectures: x86_64 and arm64
    local sys_arch=$(uname -m)
    case "${sys_arch}" in
    x86_64*) sys_arch="x86_64" ;;
    arm64*) sys_arch="aarch64" ;;
    aarch64*) sys_arch="aarch64" ;;
    *) {
        echo "Unknown system architecture: ${sys_arch}! This script runs only on x86_64 or arm64"
        exit 1
    } ;;
    esac

    # if miniconda has not been installed, download and install it
    if ! "${conda_root}/bin/conda" --version &>/dev/null; then
        if [ ! -d "$install_dir/miniconda_installer.sh" ]; then
            echo "Downloading Miniconda from $miniconda_url"
            local miniconda_url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${sys_arch}.sh"

            mkdir -p "$install_dir"
            curl -Lk "$miniconda_url" >"$install_dir/miniconda_installer.sh"
        fi

        echo "Installing Miniconda to $conda_root"
        chmod u+x "$install_dir/miniconda_installer.sh"
        bash "$install_dir/miniconda_installer.sh" -b -p "$conda_root"
        rm -rf "$install_dir/miniconda_installer.sh"
    fi
    echo "Miniconda is installed at $conda_root"

    # test conda
    echo "Conda version: "
    "$conda_root/bin/conda" --version || {
        echo "Conda not found. Will exit now..."
        exit 1
    }
}

function create_conda_env() {
    local python_version="${1}"

    if [ ! -d "${env_dir}" ]; then
        echo "Creating conda environment with python=$python_version in $env_dir"
        "${conda_root}/bin/conda" create -y -k --prefix "$env_dir" python="$python_version" || {
            echo "Failed to create conda environment."
            echo "Will delete the ${env_dir} (if exist) and exit now..."
            rm -rf $env_dir
            exit 1
        }
    else
        echo "Conda environment exists at $env_dir"
    fi
}

function activate_conda_env() {
    # deactivate the current env(s) to avoid conflicts
    { conda deactivate && conda deactivate && conda deactivate; } 2>/dev/null

    # check if conda env is broken (because of interruption during creation)
    if [ ! -f "$env_dir/bin/python" ]; then
        echo "Conda environment appears to be broken. You may need to remove $env_dir and run the installer again."
        exit 1
    fi

    source "$conda_root/etc/profile.d/conda.sh" # conda init
    conda activate "$env_dir" || {
        echo "Failed to activate environment. Please remove $env_dir and run the installer again."
        exit 1
    }
    echo "Active conda environment: $CONDA_PREFIX"
}

function deactivate_conda_env() {
    # Conda deactivate if we are in the right env
    if [ "$CONDA_PREFIX" == "$env_dir" ]; then
        conda deactivate
        echo "Deactivate conda environment at $env_dir"
    fi
}

function install_dependencies() {
    echo "Installing dependencies from requirements.txt..."
    python -m pip install -r requirements.txt
    
    echo "Downloading Spacy model (en_core_web_lg)..."
    python -m spacy download en_core_web_lg
    
    print_highlight "Installation finished successfully."
}

function launch_app() {
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

function print_highlight() {
    local message="${1}"
    echo "" && echo "******************************************************"
    echo $message
    echo "******************************************************" && echo ""
}

# Main script execution

# Ensure we are in the script's directory
cd "$(dirname "${BASH_SOURCE[0]}")"

install_dir="$(pwd)/install_dir"
conda_root="${install_dir}/conda"
env_dir="${install_dir}/env"
python_version="3.10"

check_path_for_spaces

print_highlight "Step 1: Setting up Miniconda"
install_miniconda

print_highlight "Step 2: Creating conda environment"
create_conda_env "$python_version"
activate_conda_env

print_highlight "Step 3: Installing requirements"
install_dependencies

print_highlight "Step 4: Launching Data Discovery System"
launch_app

deactivate_conda_env

read -p "Press enter to exit"
