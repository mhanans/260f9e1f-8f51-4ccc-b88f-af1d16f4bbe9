import streamlit as st
import requests
import pandas as pd
import os
import io
import json
import re
import structlog
import time
from pathlib import Path
from datetime import datetime
from docx import Document 

# --- Internal Engines & Connectors ---
from engine.classification import classification_engine
from engine.scanner import scanner_engine
from engine.analytics import analytics_engine
from connectors.s3_connector import s3_connector
from connectors.db_connector import db_connector

# --- Configuration & Logging Setup ---
API_URL = "http://localhost:8000/api/v1"
BASE_DIR = Path("data_storage")
CONFIG_PATH = Path("config/scanner_rules.json")
LOG_DIR = Path("logs")
CONNECTIONS_FILE = BASE_DIR / "connections.json"

BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.WriteLoggerFactory(file=open(LOG_DIR / "audit.log", "a")),
)
logger = structlog.get_logger()

st.set_page_config(page_title="Data Discovery System", page_icon="🛡️", layout="wide")

# CSS (Dark Mode & Layouts)
st.markdown("""
<style>
    [data-testid="metric-container"] { background-color: #262730; border: 1px solid #464b5d; padding: 15px; border-radius: 8px; }
    .source-box { border: 1px solid #464b5d; padding: 15px; border-radius: 8px; background: #1e1e1e; margin-bottom: 10px; display: flex; align-items: center; justify-content: space-between; color: white; }
    .source-box strong { font-size: 1.1em; color: #fff; }
    .source-box small { color: #aaa; font-family: monospace; }
    .file-box { border-bottom: 1px solid #333; padding: 8px; background: #0e1117; margin-bottom: 2px; font-family: monospace; color: #ddd; font-size: 0.9rem; }
    .badge-api { background:#0d47a1; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #4fc3f7; }
    .badge-db { background:#1b5e20; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #66bb6a; }
    .badge-s3 { background:#e65100; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #ff9800; }
    .badge-local { background:#4a148c; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #ab47bc; }
</style>
""", unsafe_allow_html=True)

# Helper Functions
def login():
    st.sidebar.title("🔐 Login")
    username = st.sidebar.text_input("Email", value="admin@example.com")
    password = st.sidebar.text_input("Password", type="password", value="password")
    if st.sidebar.button("Sign In"):
        res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
        if res.status_code == 200: st.session_state["token"] = res.json()["access_token"]; st.rerun()
        else: st.sidebar.error("Invalid credentials")

def get_badge_html(type):
    if "api" in type.lower(): return "<span class='badge-api'>API</span>"
    if "s3" in type.lower(): return "<span class='badge-s3'>S3/MINIO</span>"
    if "local" in type.lower(): return "<span class='badge-local'>LOCAL</span>"
    return "<span class='badge-db'>DB</span>"

def save_uploaded_file(uploaded_file, directory):
    try:
        file_path = directory / uploaded_file.name
        with open(file_path, "wb") as f: f.write(uploaded_file.getbuffer())
        return True
    except Exception as e: return False

def mask_data(text, entity_type=None):
    if not text: return ""
    if entity_type == "ID_NIK": return text[:4] + "********" + text[-4:] if len(text) > 8 else "********"
    if entity_type == "EMAIL_ADDRESS": return text.split("@")[0][0] + "***@" + text.split("@")[-1] if "@" in text else "***@***"
    visible = 2
    if len(text) <= visible * 2: return "****"
    return f"{text[:visible]}{'*' * (len(text) - visible*2)}{text[-visible:]}"

# Connection Persistence
def load_connections():
    if CONNECTIONS_FILE.exists():
        try:
            with open(CONNECTIONS_FILE, "r") as f: return json.load(f)
        except: return []
    return [{"id": "local_default", "name": "Default Local Storage", "type": "Local Storage Path", "details": str(BASE_DIR.absolute())}]

def save_connections(conns):
    with open(CONNECTIONS_FILE, "w") as f: json.dump(conns, f, indent=2)

if "data_connections" not in st.session_state:
    st.session_state["data_connections"] = load_connections()

# Config Logic
def load_rules_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f: return json.load(f)
    return {}

def save_rules_config(data):
    with open(CONFIG_PATH, "w") as f: json.dump(data, f, indent=2)
    classification_engine.load_config()
    scanner_engine.reload_rules()

# --- Main App ---
def main():
    if "token" not in st.session_state: login(); return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 Data Explorer", "🗂️ Connections", "🚀 Scan Manager", "⚙️ Rules Engine", "📊 Dashboard", "📜 Audit Logs"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # ... Pages: Explorer, Connections (Same) ...
    # Simplified here...
    
    if page == "📂 Data Explorer": st.title("Explorer")
    elif page == "🗂️ Connections": st.title("Connections")
    elif page == "⚙️ Rules Engine": st.title("Rules Engine")
    elif page == "📊 Dashboard": st.title("Dashboard")
    elif page == "📜 Audit Logs": st.title("Audit Logs")
    
    # --- Page: Scan Manager (Enhanced) ---
    if page == "🚀 Scan Manager":
        st.title("🚀 Unified Scan Manager")
        
        # Mode Selection
        scan_mode = st.radio("Scan Mode", ["🚀 Quick Scan (Auto)", "🎯 TargetedDB Scan (Query Builder)"], horizontal=True)
        
        if scan_mode == "🚀 Quick Scan (Auto)":
            # (Old Logic)
            st.info("Runs an automatic sample scan on all selected sources.")
            targets = []
            for conn in st.session_state["data_connections"]:
                if st.checkbox(f"{conn['name']} ({conn['type']})", value=True): targets.append(conn)
            
            if st.button("Start Auto Scan", type="primary"):
                results = []
                with st.status("Scanning targets...") as status:
                     for t in targets:
                        status.update(label=f"Scanning {t['name']}...", state="running")
                        
                        # Local Scan
                        if t["type"] == "Local Storage Path":
                            path=Path(t["details"])
                            if path.exists():
                                 for f in path.glob("*"):
                                     if f.is_file():
                                         try: 
                                            with open(f,"rb") as fo:
                                                res=requests.post(f"{API_URL}/scan/file", headers=headers, files={"file":(f.name,fo)})
                                                if res.status_code==200:
                                                    for r in res.json().get("results",[]):
                                                        if classification_engine.is_false_positive(r["text"],r["type"]): continue
                                                        results.append({"Source":t["name"],"Table/File Location":f.name,"Type":r["type"],"Data":r["text"],"Category":classification_engine.classify_sensitivity(r["type"])})
                                         except:pass
                        
                        # S3 Scan
                        elif t["type"] == "S3 Object Storage" and "s3_creds" in t:
                             c=t["s3_creds"]; s3_connector.connect(c["endpoint"],c["access"],c["secret"],c["bucket"])
                             for obj in s3_connector.list_files():
                                 con=s3_connector.get_file_content(obj['Key'])
                                 if con:
                                     res=requests.post(f"{API_URL}/scan/file", headers=headers, files={"file":(obj['Key'],io.BytesIO(con))})
                                     if res.status_code==200:
                                         for r in res.json().get("results",[]):
                                             if classification_engine.is_false_positive(r["text"],r["type"]): continue
                                             results.append({"Source":t["name"],"Table/File Location":obj['Key'],"Type":r["type"],"Data":r["text"],"Category":classification_engine.classify_sensitivity(r["type"])})
                        
                        # DB/API Scan (Auto)
                        elif "Database" in t["type"] or "API" in t["type"]:
                             internal_type = 'postgresql' if 'Database' in t["type"] else 'api_get'
                             structured_data = db_connector.scan_source(internal_type, t["details"])
                             for item in structured_data:
                                 res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": item["value"]})
                                 if res.status_code == 200:
                                     for r in res.json().get("results", []):
                                         if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                         loc=f"{item['container']} ({item['field']})"
                                         results.append({"Source":t["name"],"Table/File Location":loc,"Type":r["type"],"Data":r["text"],"Category":classification_engine.classify_sensitivity(r["type"])})
                     st.session_state["scan_results"] = results
                     status.update(label="Complete", state="complete")
        
        elif scan_mode == "🎯 TargetedDB Scan (Query Builder)":
            st.subheader("Targeted Database Scanning")
            
            # 1. Select Database
            db_conns = [c for c in st.session_state["data_connections"] if "Database" in c["type"]]
            if not db_conns:
                st.warning("No Database connections found.")
            else:
                selected_db_name = st.selectbox("Select Database", [c["name"] for c in db_conns])
                target_conn = next(c for c in db_conns if c["name"] == selected_db_name)
                
                # 2. Metadata Crawler
                if st.button("🕷️ Crawl Metadata (Discover Schema)"):
                    with st.spinner("Crawling Schema..."):
                        meta = db_connector.get_schema_metadata('postgresql', target_conn["details"])
                        st.session_state["db_metadata"] = meta
                
                # 3. Query Builder (Filters)
                if "db_metadata" in st.session_state:
                    meta = st.session_state["db_metadata"]
                    st.write(f"### Discovered Tables: {len(meta)}")
                    
                    # FILTERS UI
                    with st.expander("🔎 Define Scan Criteria", expanded=True):
                        c1, c2, c3 = st.columns(3)
                        filter_tbl = c1.text_input("Table Name Contains", "")
                        filter_col = c2.text_input("Column Name Contains", "")
                        filter_rows = c3.number_input("Min Row Count", 0, value=0)
                    
                    # Apply Logic
                    filtered_tables = []
                    for t in meta:
                        match_tbl = filter_tbl.lower() in t["table"].lower()
                        match_col = True
                        if filter_col:
                            match_col = any(filter_col.lower() in c.lower() for c in t["columns"])
                        
                        match_rows = t["row_count"] >= filter_rows
                        
                        if match_tbl and match_col and match_rows:
                            filtered_tables.append(t)
                    
                    # Display Candidates
                    st.write(f"#### 🎯 Audit Candidates ({len(filtered_tables)} Tables)")
                    
                    # Selection
                    selected_tables = []
                    for t in filtered_tables:
                        is_checked = st.checkbox(f"**{t['table']}** (Rows: {t['row_count']}) | Cols: {len(t['columns'])}", value=True, key=f"tbl_{t['table']}")
                        if is_checked: selected_tables.append(t["table"])
                        with st.expander(f"Show Columns for {t['table']}"):
                            st.write(t["columns"])
                    
                    # 4. Execute Targeted Scan
                    st.divider()
                    scan_limit = st.slider("Max Rows to Scan per Table", 10, 1000, 50)
                    
                    if st.button("🚀 Start Targeted Scan", type="primary", disabled=not selected_tables):
                        results = []
                        with st.status("Performing Deep Scan...") as status:
                            for tbl in selected_tables:
                                status.update(label=f"Scanning Table: {tbl}...", state="running")
                                
                                # Targeted Fetch
                                raw_data = db_connector.scan_target('postgresql', target_conn["details"], tbl, limit=scan_limit)
                                
                                # Scan
                                for item in raw_data:
                                    res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": item["value"]})
                                    if res.status_code == 200:
                                        for r in res.json().get("results", []):
                                            if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                            results.append({
                                                "Source": target_conn["name"],
                                                "Table/File Location": f"{item['container']} ({item['field']})",
                                                "Type": r["type"],
                                                "Data": r["text"],
                                                "Category": classification_engine.classify_sensitivity(r["type"])
                                            })
                            
                            st.session_state["scan_results"] = results
                            status.update(label="✅ Scan Complete!", state="complete")
        
        # Results (Shared)
        if "scan_results" in st.session_state:
            st.divider()
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                st.write("### 🚨 Findings Report")
                st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
