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
    .file-box { 
        border-bottom: 1px solid #eee; padding: 8px; background: white; margin-bottom: 2px;
        font-family: monospace; color: #333; font-size: 0.9rem;
    }
    .badge-api { background:#e1f5fe; color:#0277bd; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-db { background:#e8f5e9; color:#2e7d32; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-s3 { background:#fff3e0; color:#ef6c00; padding:2px 8px; border-radius:4px; font-size:0.8em; }
    .badge-local { background:#f3e5f5; color:#7b1fa2; padding:2px 8px; border-radius:4px; font-size:0.8em; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
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

def save_uploaded_file(uploaded_file, directory):
    try:
        file_path = directory / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        logger.info("file_upload", filename=uploaded_file.name, size=uploaded_file.size)
        return True
    except Exception as e:
        logger.error("file_upload_error", error=str(e))
        return False

def mask_data(text, entity_type=None):
    if not text: return ""
    if entity_type == "ID_NIK":
        if len(text) > 8: return text[:4] + "********" + text[-4:]
        return "********"
    if entity_type == "EMAIL_ADDRESS":
        if "@" in text:
            parts = text.split("@")
            return parts[0][0] + "***@" + parts[-1]
        return "***@***"
    visible = 2
    if len(text) <= visible * 2: return "****"
    return f"{text[:visible]}{'*' * (len(text) - visible*2)}{text[-visible:]}"

# --- Main App ---
def main():
    if "token" not in st.session_state: login(); return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 Data Explorer", "🗂️ Connections", "🚀 Scan Manager", "📊 Dashboard"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # Initialize connections
    if "data_connections" not in st.session_state:
        st.session_state["data_connections"] = [
            {"id": "conn_default_local", "name": "Default Local Storage", "type": "Local Storage Path", "details": str(BASE_DIR.absolute())}
        ]

    # --- Page: Data Explorer ---
    if page == "📂 Data Explorer":
        st.title("📂 Data Explorer")
        st.caption("Browse files, upload data, and view source contents.")
        
        conn_options = {c["name"]: c for c in st.session_state["data_connections"] if "Storage" in c["type"]}
        selected_conn_name = st.selectbox("Select Storage Source", list(conn_options.keys()))
        
        if selected_conn_name:
            conn = conn_options[selected_conn_name]
            
            # LOCAL
            if conn["type"] == "Local Storage Path":
                path = Path(conn["details"])
                with st.expander(f"📤 Upload to {conn['name']}", expanded=True):
                    up_files = st.file_uploader("Drag & Drop", accept_multiple_files=True)
                    if up_files:
                        for f in up_files:
                            save_uploaded_file(f, path)
                            st.toast(f"Saved: {f.name}")
                        st.rerun()

                files = list(path.glob("*")) if path.exists() else []
                st.write(f"### {len(files)} Items in {conn['name']}")
                
                if not files: st.info("Directory is empty.")
                else:
                    for f in files:
                        c1, c2, c3 = st.columns([6, 1, 1])
                        c1.markdown(f"<div class='file-box'>📄 {f.name} <span style='color:#888;font-size:0.8em'>({f.stat().st_size/1024:.1f} KB)</span></div>", unsafe_allow_html=True)
                        if c2.button("🗑️", key=f"d_{f.name}"): os.remove(f); st.rerun()
                        if c3.button("🚫", key=f"b_{f.name}"): st.toast("Blocked")

            # S3
            elif conn["type"] == "S3 Object Storage":
                if "s3_config_active" in st.session_state:
                    if st.button("🔄 Refresh Bucket List"): st.rerun()
                    files = s3_connector.list_files()
                    st.write(f"### {len(files)} Objects")
                    with st.expander("☁️ Upload to Cloud", expanded=False):
                        up_s3 = st.file_uploader("Upload S3", accept_multiple_files=True)
                        if up_s3:
                            for f in up_s3: s3_connector.upload_file(f, f.name)
                            st.rerun()
                    for f in files:
                        c1, c2 = st.columns([6, 1])
                        c1.markdown(f"<div class='file-box'>☁️ {f['Key']} <span style='color:#888;font-size:0.8em'>({f['Size']/1024:.1f} KB)</span></div>", unsafe_allow_html=True)
                        if c2.button("🗑️", key=f"ds3_{f['Key']}"): 
                            s3_connector.delete_file(f['Key'])
                            st.rerun()
                else: st.warning("S3 Not Checked/Active. Please verify in Connections.")

        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            st.write("### 🚨 Latest Scan Results")
            mask_enabled = st.toggle("🔒 Privacy Mode", value=True)
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                display_df = df.copy()
                if mask_enabled:
                    display_df["Data"] = display_df.apply(lambda row: mask_data(row["Data"], row["Type"]), axis=1)
                st.dataframe(display_df, use_container_width=True)

    # --- Page: Connections Manager (UPDATED) ---
    elif page == "🗂️ Connections":
        st.title("🗂️ Unified Connection Manager")
        
        # Add New
        with st.expander("➕ Add New Data Source", expanded=True):
            cols = st.columns([1, 2])
            c_type = cols[0].selectbox("Connection Type", ["API Endpoint", "PostgreSQL Database", "S3 Object Storage", "Local Storage Path"])
            c_name = cols[1].text_input("Connection Name (Alias)")
            
            c_det = ""
            is_valid = False

            # === S3 INPUTS ===
            if c_type == "S3 Object Storage":
                 c1, c2 = st.columns(2)
                 e = c1.text_input("Endpoint (Host)", placeholder="http://localhost:9000")
                 b = c2.text_input("Bucket Name", placeholder="pii-bucket")
                 c3, c4 = st.columns(2)
                 k = c3.text_input("Access Key", placeholder="minioadmin")
                 s = c4.text_input("Secret Key", type="password", placeholder="minioadmin")
                 
                 if st.button("Test & Save S3"):
                     s3_connector.connect(e, k, s, b)
                     st.session_state["s3_config_active"] = True
                     c_det = f"Endpoint={e}; Bucket={b}"
                     is_valid = True
            
            # === DATABASE INPUTS (NEW MULTI-FIELD) ===
            elif "Database" in c_type:
                 c1, c2 = st.columns(2)
                 db_host = c1.text_input("Host", value="localhost")
                 db_port = c2.text_input("Port", value="5432")
                 
                 c3, c4 = st.columns(2)
                 db_user = c3.text_input("Username", value="postgres")
                 db_pass = c4.text_input("Password", type="password")
                 
                 db_name = st.text_input("Database Name", value="postgres")
                 
                 if st.button("Test & Save Database"):
                     # Construct URI: postgresql://user:pass@host:port/dbname
                     scheme = "postgresql" # Default for now
                     uri = f"{scheme}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
                     
                     # Simple logic: Verify with connector
                     success, msg = db_connector.test_connection('postgresql', uri)
                     if success:
                         c_det = uri
                         is_valid = True
                         st.toast("✅ Database Connected")
                     else:
                         st.error(f"Failed: {msg}")

            # === API INPUTS (NEW MULTI-FIELD) ===
            elif c_type == "API Endpoint":
                 c1, c2 = st.columns([3, 1])
                 api_base = c1.text_input("Base URL", placeholder="https://api.example.com/v1")
                 api_method = c2.selectbox("Method", ["GET", "POST"])
                 
                 api_auth_key = st.text_input("Authorization Header (Optional)", placeholder="Bearer ...")
                 
                 if st.button("Test & Save API"):
                     # For simplicity, we just store URL. 
                     # DB Connector currently handles just GET URL. 
                     # Future: Pass Headers map.
                     success, msg = db_connector.test_connection('api_get', api_base)
                     if success:
                         c_det = api_base
                         is_valid = True
                         st.toast("✅ API Reachable")
                     else:
                         st.error(f"API Error: {msg}")

            # === LOCAL ===
            elif c_type == "Local Storage Path":
                p = st.text_input("Filesystem Path", value=str(BASE_DIR.absolute()))
                if st.button("Verify Path"):
                    if os.path.isdir(p):
                        c_det = p
                        is_valid = True
                        st.toast("✅ Valid Directory")
                    else:
                        st.error("Invalid PATH")

            if is_valid and c_name:
                st.session_state["data_connections"].append({
                    "id": str(time.time()),
                    "name": c_name,
                    "type": c_type,
                    "details": c_det
                })
                st.success(f"Added {c_name}!")
                st.rerun()

        # List
        st.write(f"### Active Sources ({len(st.session_state['data_connections'])})")
        for i, conn in enumerate(st.session_state["data_connections"]):
            badge = get_badge_html(conn["type"])
            # Mask details if it looks like a URI with password
            display_details = conn['details']
            if "://" in display_details and "@" in display_details:
                # Mask password in URI
                try: 
                    pre, post = display_details.split("@")
                    scheme_user = pre.split(":")[0] + ":****" 
                    display_details = f"{scheme_user}@{post}"
                except: pass
                
            st.markdown(f"<div class='source-box'><div><strong>{conn['name']}</strong> {badge}<br><small style='color:#666'>{display_details}</small></div></div>", unsafe_allow_html=True)

    # --- Page: Scan Manager (Unified) ---
    elif page == "🚀 Scan Manager":
        st.title("🚀 Unified Scan Manager")
        
        targets = []
        for conn in st.session_state["data_connections"]:
            if st.checkbox(f"{conn['name']} ({conn['type']})", value=True):
                targets.append(conn)
        
        if st.button("🚀 START SCAN", type="primary"):
            results = []
            with st.status("Scanning targets...") as status:
                for t in targets:
                    status.update(label=f"Scanning {t['name']}...", state="running")
                    
                    if t["type"] == "Local Storage Path":
                        path = Path(t["details"])
                        if path.exists():
                            for f in path.glob("*"):
                                if f.is_file():
                                    try: 
                                        with open(f, "rb") as fo:
                                            res = requests.post(f"{API_URL}/scan/file", headers=headers, files={"file":(f.name, fo)})
                                            if res.status_code == 200:
                                                for r in res.json().get("results", []):
                                                    if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                                    results.append({"Source":t["name"], "Identity":f.name, "Type":r["type"], "Data":r["text"], "Category":classification_engine.classify_sensitivity(r["type"])})
                                    except: pass
                    
                    elif "Database" in t["type"] or "API" in t["type"]:
                        internal_type = 'postgresql' if 'Database' in t["type"] else 'api_get'
                        data = db_connector.scan_source(internal_type, t["details"])
                        for d in data:
                             res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text":d})
                             if res.status_code == 200:
                                 for r in res.json().get("results", []):
                                     if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                     results.append({"Source":t["name"], "Identity":"Record", "Type":r["type"], "Data":r["text"], "Category":classification_engine.classify_sensitivity(r["type"])})

                    elif t["type"] == "S3 Object Storage":
                        if s3_connector.s3_client:
                             for obj in s3_connector.list_files():
                                 content = s3_connector.get_file_content(obj['Key'])
                                 if content:
                                     res = requests.post(f"{API_URL}/scan/file", headers=headers, files={"file":(obj['Key'], io.BytesIO(content))})
                                     if res.status_code == 200:
                                         for r in res.json().get("results", []):
                                             if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                             results.append({"Source":t["name"], "Identity":obj['Key'], "Type":r["type"], "Data":r["text"], "Category":classification_engine.classify_sensitivity(r["type"])})

                st.session_state["scan_results"] = results
                status.update(label="✅ Completed", state="complete")
        
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            df = pd.DataFrame(st.session_state["scan_results"])
            st.dataframe(df, use_container_width=True)

    elif page == "📊 Dashboard":
        st.title("📊 Security Dashboard")
        if "scan_results" in st.session_state:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 c1, c2 = st.columns(2)
                 c1.metric("Total Findings", len(df))
                 c2.metric("Critical", len(df[df["Category"].str.contains("Spesifik", na=False)]))
                 st.bar_chart(df["Category"].value_counts())
        else: st.info("No active scan results.")

if __name__ == "__main__":
    main()
