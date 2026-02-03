import requests
import argparse
import sys
import os

# --- Backend Script for External API / Parallel Execution ---
# Usage: python local_scanner_api.py --file "path/to/file.txt" --host "http://localhost:8000"

def scan_file_via_api(file_path: str, host_url: str):
    """
    Submits a file to the API for scanning.
    This mimics an external system hitting the Scan API.
    """
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    url = f"{host_url}/api/v1/scan/file"
    # Assuming standard admin login for this script or internal bypass
    # For simplicity in this script, we assume the endpoint might be open or we pass a token if we had one.
    # In the current implementation, it requires Auth. Let's assume we use the 'token' obtained via login.
    
    # 1. Login to get token (Simulated)
    auth_url = f"{host_url}/api/v1/auth/token"
    try:
        auth_res = requests.post(auth_url, data={"username": "admin@example.com", "password": "password"})
        if auth_res.status_code != 200:
            print("Failed to authenticate script.")
            return
        token = auth_res.json()["access_token"]
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    headers = {"Authorization": f"Bearer {token}"}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            print(f"Submitting {file_path} to {url}...")
            response = requests.post(url, headers=headers, files=files)
            
            if response.status_code == 200:
                data = response.json()
                print("\n--- Scan Results ---")
                print(f"File: {data.get('filename')}")
                print(f"Encrypted: {data.get('is_encrypted')}")
                print(f"Findings: {len(data.get('results', []))}")
                for res in data.get("results", []):
                    print(f" - [{res['type']}] {res['text']} (Score: {res['score']:.2f})")
            else:
                print(f"Error: {response.status_code} - {response.text}")
                
    except Exception as e:
        print(f"Request Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="External Scanner Client")
    parser.add_argument("--file", required=True, help="Path to file to scan")
    parser.add_argument("--host", default="http://localhost:8000", help="API Host URL")
    
    args = parser.parse_args()
    scan_file_via_api(args.file, args.host)
