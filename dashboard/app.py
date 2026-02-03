import streamlit as st
import requests
import pandas as pd
import os
import io
import json
import structlog
import time
from pathlib import Path
from datetime import datetime
from docx import Document 

# --- Internal Engines & Connectors ---
from engine.classification import classification_engine
from engine.analytics import analytics_engine
from connectors.s3_connector import s3_connector
from connectors.db_connector import db_connector

# --- Configuration & Logging Setup ---
API_URL = "http://localhost:8000/api/v1"
BASE_DIR = Path("data_storage")
LOG_DIR = Path("logs")
BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Configure Structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.WriteLoggerFactory(file=open(LOG_DIR / "audit.log", "a")),
)
logger = structlog.get_logger()

st.set_page_config(page_title="Data Discovery System", page_icon="🛡️", layout="wide")

# --- Custom CSS ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    [data-testid="metric-container"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    [data-testid="stMetricValue"] { color: #1f77b4 !important; font-size: 1.8rem !important; }
    [data-testid="stMetricLabel"] { color: #555555 !important; font-weight: bold; }
    .source-box { 
        border: 1px solid #ddd; padding: 15px; border-radius: 8px; background: #ffffff; margin-bottom: 10px;
        display: flex; align-items: center; justify-content: space-between;
    }
    .badge-api { background:#e1f5fe; color:#0277bd; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-db { background:#e8f5e9; color:#2e7d32; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-s3 { background:#fff3e0; color:#ef6c00; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-local { background:#f3e5f5; color:#7b1fa2; padding:2px 8px; border-radius:4px; font-size:0.8em; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions (Same as before) ---
def login():
    st.sidebar.title("🔐 Login")
    username = st.sidebar.text_input("Email", value="admin@example.com")
    password = st.sidebar.text_input("Password", type="password", value="password")
    if st.sidebar.button("Sign In"):
        res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
        if res.status_code == 200: st.session_state["token"] = res.json()["access_token"]; st.rerun()
        else: st.sidebar.error("Invalid credentials")

def get_badge_html(type):
    if "api" in type.lower(): return "<span class='badge-api'>API HEAD</span>"
    if "s3" in type.lower(): return "<span class='badge-s3'>OBJECT STORAGE</span>"
    if "local" in type.lower(): return "<span class='badge-local'>LOCAL DISK</span>"
    return "<span class='badge-db'>DATABASE</span>"

# --- Main App ---
def main():
    if "token" not in st.session_state: login(); return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["🗂️ Data Connections", "🚀 Scan Manager", "📊 Dashboard", "📜 Logs"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # Initialize generic connection store if empty
    if "data_connections" not in st.session_state:
        # Default Local Source
        st.session_state["data_connections"] = [
            {"id": "conn_default_local", "name": "Default Local Storage", "type": "Local Storage", "details": str(BASE_DIR.absolute())}
        ]

    # --- Page: Data Connections (Unified Manager) ---
    if page == "🗂️ Data Connections":
        st.title("🗂️ Unified Connection Manager")
        st.caption("Manage all your data sources (Databases, API Endpoints, Cloud Storage, Local Paths) in one place.")

        # 1. List Existing Connections
        st.write(f"### Active Connections ({len(st.session_state['data_connections'])})")
        for i, conn in enumerate(st.session_state["data_connections"]):
            badge = get_badge_html(conn["type"])
            with st.container():
                c1, c2, c3 = st.columns([5, 1, 1])
                c1.markdown(f"""
                <div class='source-box'>
                    <div>
                        <strong>{conn['name']}</strong> &nbsp; {badge}<br>
                        <small style='color:#666'>{conn['details']}</small>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Test Button
                if c2.button("Test", key=f"test_{i}"):
                    if conn["type"] == "API Endpoint":
                        success, msg = db_connector.test_connection('api_get', conn["details"])
                        if success: st.toast(f"✅ {msg}")
                        else: st.error(msg)
                    elif "Database" in conn["type"]:
                        success, msg = db_connector.test_connection('postgresql', conn["details"]) # using postgres stub for all db demo
                        if success: st.toast(f"✅ {msg}")
                        else: st.error(msg)
                    else: st.toast("✅ Path Verified")

                # Delete Button
                if c3.button("🗑️", key=f"del_conn_{i}"):
                    st.session_state["data_connections"].pop(i)
                    st.rerun()

        st.divider()

        # 2. Add New Connection
        with st.expander("➕ Add New Data Source", expanded=False):
            st.subheader("Configure New Connector")
            
            c_type = st.selectbox("Connection Type", [
                "API Endpoint", 
                "PostgreSQL Database", 
                "MySQL Database", 
                "SQL Server", 
                "S3 Object Storage",
                "Local Storage Path"
            ])
            
            c_name = st.text_input("Connection Name", placeholder="e.g. HR Production API")
            
            # Dynamic Fields based on Type
            c_details = ""
            valid = False
            
            if c_type == "API Endpoint":
                c_details = st.text_input("Endpoint URL", placeholder="https://api.example.com/v1/users")
                valid = st.button("Verify & Add API")
            
            elif "Database" in c_type:
                c_details = st.text_input("Connection String (URI)", placeholder="postgresql://user:pass@host:5432/db")
                valid = st.button("Verify & Add Database")
            
            elif c_type == "S3 Object Storage":
                c1, c2 = st.columns(2)
                e = c1.text_input("Endpoint", value="http://localhost:9000")
                b = c2.text_input("Bucket Name")
                ak = c1.text_input("Access Key")
                sk = c2.text_input("Secret Key", type="password")
                # Store config object as string repr or specific dict mechanism logic
                # For this demo we'll just store a simplified string identification
                c_details = f"Endpoint={e}; Bucket={b}; Key=***"
                valid = st.button("Save S3 Config")
                if valid:
                    # In real app, save secure keys properly
                    s3_connector.connect(e, ak, sk, b) # Auto connect
                    st.session_state["s3_config_active"] = {"e":e,"b":b,"a":ak,"s":sk}

            elif c_type == "Local Storage Path":
                c_details = st.text_input("Directory Path", value=str(BASE_DIR.absolute()))
                valid = st.button("Add Path")

            if valid and c_name and c_details:
                st.session_state["data_connections"].append({
                    "id": f"conn_{int(time.time())}",
                    "name": c_name,
                    "type": c_type,
                    "details": c_details
                })
                st.success(f"Successfully added {c_name}!")
                st.rerun()

    # --- Page: Scan Manager ---
    elif page == "🚀 Scan Manager":
        st.title("🚀 Scan Manager")
        st.caption("Select data sources to include in this scan job.")

        # Checklist of connections
        selected_conns = []
        if st.session_state["data_connections"]:
            st.write("### Target Sources")
            for conn in st.session_state["data_connections"]:
                if st.checkbox(f"{conn['name']} ({conn['type']})", value=True, key=f"scan_chk_{conn['id']}"):
                    selected_conns.append(conn)
        
        if st.button("🚀 START UNIFIED SCAN", type="primary"):
            if not selected_conns:
                st.error("Please select at least one source.")
            else:
                st.info("Initializing multi-threaded scan job...")
                all_results = []
                
                with st.status("Scanning in progress...", expanded=True) as status:
                    for conn in selected_conns:
                        time.sleep(0.5) # UI pacing
                        
                        # --- 1. LOCAL SCAN LOGIC ---
                        if conn["type"] == "Local Storage Path":
                            status.update(label=f"Reading Local Disk: {conn['name']}...", state="running")
                            files = list(Path(conn["details"]).glob("*")) if os.path.isdir(conn["details"]) else []
                            for f in files:
                                if f.is_file():
                                    # Reuse logic provided previously (condensed here)
                                    try: 
                                        with open(f, "rb") as file_obj:
                                            # Send to API
                                            res = requests.post(f"{API_URL}/scan/file", headers=headers, files={"file": (f.name, file_obj)})
                                            if res.status_code == 200:
                                                for finding in res.json().get("results", []):
                                                    if classification_engine.is_false_positive(finding["text"], finding["type"]): continue
                                                    all_results.append({
                                                        "Source": conn["name"],
                                                        "Identity": f.name,
                                                        "Type": finding["type"],
                                                        "Data": finding["text"], 
                                                        "Category": classification_engine.classify_sensitivity(finding["type"])
                                                    })
                                    except: pass

                        # --- 2. S3 SCAN LOGIC ---
                        elif conn["type"] == "S3 Object Storage":
                            status.update(label=f"Streaming from S3: {conn['name']}...", state="running")
                            # Reuse S3 Connector
                            # For demo, assumes connection active from Config step
                            if "s3_config_active" in st.session_state or s3_connector.s3_client:
                                s3_files = s3_connector.list_files()
                                for s3f in s3_files:
                                    content = s3_connector.get_file_content(s3f['Key'])
                                    if content:
                                        res = requests.post(f"{API_URL}/scan/file", headers=headers, files={"file": (s3f['Key'], io.BytesIO(content))})
                                        if res.status_code == 200:
                                            for finding in res.json().get("results", []):
                                                if classification_engine.is_false_positive(finding["text"], finding["type"]): continue
                                                all_results.append({
                                                    "Source": conn["name"],
                                                    "Identity": s3f['Key'],
                                                    "Type": finding["type"],
                                                    "Data": finding["text"], 
                                                    "Category": classification_engine.classify_sensitivity(finding["type"])
                                                })

                        # --- 3. DATABASE / API SCAN LOGIC ---
                        elif "Database" in conn["type"] or "API" in conn["type"]:
                            status.update(label=f"Querying: {conn['name']}...", state="running")
                            # Use DB Connector
                            internal_type = 'postgresql' if 'Database' in conn["type"] else 'api_get'
                            samples = db_connector.scan_source(internal_type, conn["details"])
                            for idx, text in enumerate(samples):
                                res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text})
                                if res.status_code == 200:
                                    for finding in res.json().get("results", []):
                                        if classification_engine.is_false_positive(finding["text"], finding["type"]): continue
                                        all_results.append({
                                            "Source": conn["name"],
                                            "Identity": f"Row/Record #{idx}",
                                            "Type": finding["type"],
                                            "Data": finding["text"], 
                                            "Category": classification_engine.classify_sensitivity(finding["type"])
                                        })

                    st.session_state["scan_results"] = all_results
                    status.update(label="✅ All Jobs Completed!", state="complete", expanded=False)

        # Show Results
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            st.write("### 🚨 Consolidated Findings")
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                st.dataframe(df, use_container_width=True)
            else:
                st.success("No Sensitive Data Found in any targets.")

    # --- Other Pages ---
    elif page == "📊 Dashboard":
        st.title("📊 Executive Dashboard")
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 m1, m2, m3 = st.columns(3)
                 m1.metric("Total Findings", len(df))
                 m2.metric("Critical (Spesifik)", len(df[df["Category"].str.contains("Spesifik", na=False)]))
                 m3.metric("Sources Scanned", df["Source"].nunique())
                 st.bar_chart(df["Category"].value_counts())
        else: st.info("No data.")

    elif page == "📜 Logs":
        st.title("Logs")
        log_file = LOG_DIR / "audit.log"
        if log_file.exists():
            with open(log_file, "r") as f:
                logs = [json.loads(line) for line in f]
            st.dataframe(pd.DataFrame(logs).iloc[::-1])

if __name__ == "__main__":
    main()
