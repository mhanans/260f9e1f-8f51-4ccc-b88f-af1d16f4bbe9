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
CONNECTIONS_FILE = BASE_DIR / "connections.json" # Persistence File

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

# --- Persistence Manager ---
def load_connections():
    if CONNECTIONS_FILE.exists():
        try:
            with open(CONNECTIONS_FILE, "r") as f:
                return json.load(f)
        except: return []
    return [{"id": "local_default", "name": "Default Local Storage", "type": "Local Storage Path", "details": str(BASE_DIR.absolute())}]

def save_connections(conns):
    with open(CONNECTIONS_FILE, "w") as f:
        json.dump(conns, f, indent=2)

# Load on startup
if "data_connections" not in st.session_state:
    st.session_state["data_connections"] = load_connections()

# --- Custom CSS (Dark Mode Compatible) ---
st.markdown("""
<style>
    /* Metric Cards */
    [data-testid="metric-container"] {
        background-color: #262730; /* Darker Streamlit Card */
        border: 1px solid #464b5d;
        padding: 15px;
        border-radius: 8px;
    }
    
    /* Connection Cards */
    .source-box { 
        border: 1px solid #464b5d; 
        padding: 15px; 
        border-radius: 8px; 
        background: #1e1e1e; /* Dark Bg */
        margin-bottom: 10px;
        display: flex; 
        align-items: center; 
        justify-content: space-between;
        color: white; /* Force White Text */
    }
    .source-box strong { font-size: 1.1em; color: #fff; }
    .source-box small { color: #aaa; font-family: monospace; }
    
    /* File Lists */
    .file-box { 
        border-bottom: 1px solid #333; 
        padding: 8px; 
        background: #0e1117; 
        margin-bottom: 2px;
        font-family: monospace; 
        color: #ddd; 
        font-size: 0.9rem;
    }
    
    /* Badges */
    .badge-api { background:#0d47a1; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #4fc3f7; }
    .badge-db { background:#1b5e20; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #66bb6a; }
    .badge-s3 { background:#e65100; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #ff9800; }
    .badge-local { background:#4a148c; color:white; padding:2px 8px; border-radius:4px; font-size:0.8em; border: 1px solid #ab47bc; }
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
    if "api" in type.lower(): return "<span class='badge-api'>API</span>"
    if "s3" in type.lower(): return "<span class='badge-s3'>S3/MINIO</span>"
    if "local" in type.lower(): return "<span class='badge-local'>LOCAL</span>"
    return "<span class='badge-db'>DB</span>"

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

    # --- Page: Data Explorer ---
    if page == "📂 Data Explorer":
        st.title("📂 Data Explorer")
        st.caption("Browse files, upload data, and view source contents.")
        
        conn_options = {c["name"]: c for c in st.session_state["data_connections"] if "Storage" in c["type"]}
        selected_conn_name = st.selectbox("Select Storage Source", list(conn_options.keys()) if conn_options else [])
        
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

            # S3 / Minio
            elif conn["type"] == "S3 Object Storage":
                # Ensure Connected
                c_details = conn.get("s3_creds", {})
                if not c_details:
                     st.error("Missing credentials for this S3 connection. Please delete and re-add.")
                else:
                    # Attempt Connect (if not already)
                    if s3_connector.bucket_name != c_details.get("bucket"):
                        s3_connector.connect(c_details["endpoint"], c_details["access"], c_details["secret"], c_details["bucket"])
                    
                    if st.button("🔄 Refresh Bucket List"): st.rerun()
                    
                    files = s3_connector.list_files()
                    st.write(f"### {len(files)} Objects in `{c_details['bucket']}`")
                    
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

    # --- Page: Connections Manager ---
    elif page == "🗂️ Connections":
        st.title("🗂️ Unified Connection Manager")
        
        # 1. Active Connections List (Restored Interactivity)
        st.subheader(f"Active Sources ({len(st.session_state['data_connections'])})")
        
        conns = st.session_state["data_connections"]
        for i, conn in enumerate(conns):
            badge = get_badge_html(conn["type"])
            
            # Display Details Construction
            display_details = conn.get("details", "")
            if "s3_creds" in conn:
                display_details = f"Endpoint={conn['s3_creds']['endpoint']}; Bucket={conn['s3_creds']['bucket']}"
            
            # Card UI
            with st.container():
                cols = st.columns([0.7, 0.1, 0.1, 0.1])
                with cols[0]:
                    st.markdown(f"""
                        <div class='source-box'>
                            <div>
                                <strong>{conn['name']}</strong> &nbsp; {badge}<br>
                                <small>{display_details}</small>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                
                # Action Buttons
                with cols[1]:
                    if st.button("🔌 Test", key=f"test_{i}"):
                        if "s3_creds" in conn:
                             c = conn["s3_creds"]
                             suc, msg = s3_connector.connect(c["endpoint"], c["access"], c["secret"], c["bucket"])
                             if suc: st.toast("✅ MinIO Connected!")
                             else: st.error(f"Failed: {msg}")
                        elif "Database" in conn["type"]:
                            # Assume postgres logic for demo
                            suc, msg = db_connector.test_connection('postgresql', conn["details"])
                            if suc: st.toast("✅ DB Connected!")
                            else: st.error(msg)
                        elif "API" in conn["type"]:
                            suc, msg = db_connector.test_connection('api_get', conn["details"])
                            if suc: st.toast(f"✅ {msg}")
                            else: st.error(msg)
                        else: st.toast("✅ Local Path OK")

                with cols[2]:
                     # Placeholder for Edit (Complex to implement inline, usually separate modal)
                     st.button("✏️", key=f"edit_{i}", disabled=True, help="Edit not implemented yet")

                with cols[3]:
                    if st.button("🗑️", key=f"del_{i}"):
                        st.session_state["data_connections"].pop(i)
                        save_connections(st.session_state["data_connections"]) # PERSIST DELETE
                        st.rerun()
        
        st.divider()

        # 2. Add New Data Source (Persisted)
        with st.expander("➕ Add New Data Source", expanded=False):
            cols = st.columns([1, 2])
            c_type = cols[0].selectbox("Connection Type", ["API Endpoint", "PostgreSQL Database", "S3 Object Storage", "Local Storage Path"])
            c_name = cols[1].text_input("Connection Name (Alias)")
            
            c_det = ""
            s3_creds_obj = None
            is_valid = False

            if c_type == "S3 Object Storage":
                 c1, c2 = st.columns(2)
                 e = c1.text_input("Endpoint (Host)", value="http://localhost:9000")
                 b = c2.text_input("Bucket Name", placeholder="data-bucket")
                 c3, c4 = st.columns(2)
                 k = c3.text_input("Access Key", placeholder="admin")
                 s = c4.text_input("Secret Key", type="password", placeholder="password")
                 
                 if st.button("Test & Save S3"):
                     # Auto-fix endpoint protocol if missing
                     if not e.startswith("http"): e = "http://" + e
                     
                     suc, msg = s3_connector.connect(e, k, s, b)
                     if suc:
                         st.success(f"✅ Connected to {b}")
                         s3_creds_obj = {"endpoint":e, "bucket":b, "access":k, "secret":s}
                         is_valid = True
                     else:
                         st.error(f"Connection Failed: {msg}")
            
            elif "Database" in c_type:
                 c1, c2 = st.columns(2)
                 db_host = c1.text_input("Host", value="localhost")
                 db_port = c2.text_input("Port", value="5432")
                 c3, c4 = st.columns(2)
                 db_user = c3.text_input("Username", value="postgres")
                 db_pass = c4.text_input("Password", type="password")
                 db_name = st.text_input("Database Name", value="postgres")
                 if st.button("Test & Save Database"):
                     scheme = "postgresql"
                     uri = f"{scheme}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
                     suc, msg = db_connector.test_connection('postgresql', uri)
                     if suc:
                         c_det = uri
                         is_valid = True
                         st.toast("✅ Database Connected")
                     else: st.error(f"Failed: {msg}")

            elif c_type == "API Endpoint":
                 c1, c2 = st.columns([3, 1])
                 api_base = c1.text_input("Base URL")
                 meth = c2.selectbox("Method", ["GET", "POST"])
                 if st.button("Test & Save API"):
                     suc, msg = db_connector.test_connection('api_get', api_base)
                     if suc:
                         c_det = api_base
                         is_valid = True
                         st.toast("✅ API Reachable")
                     else: st.error(f"API Error: {msg}")

            elif c_type == "Local Storage Path":
                p = st.text_input("Filesystem Path", value=str(BASE_DIR.absolute()))
                if st.button("Verify Path"):
                    if os.path.isdir(p):
                        c_det = p
                        is_valid = True
                        st.toast("✅ Valid Directory")
                    else: st.error("Invalid PATH")

            if is_valid and c_name:
                new_conn = {
                    "id": str(time.time()),
                    "name": c_name,
                    "type": c_type,
                    "details": c_det
                }
                if s3_creds_obj: new_conn["s3_creds"] = s3_creds_obj
                
                st.session_state["data_connections"].append(new_conn)
                save_connections(st.session_state["data_connections"]) # PERSIST SAVE
                st.success(f"Saved {c_name}!")
                st.rerun()

    # --- Page: Scan Manager ---
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
                    
                    # SCAN LOCAL
                    if t["type"] == "Local Storage Path":
                        path = Path(t["details"] or t["s3_creds"]["endpoint"]) # fallback safety
                        if os.path.isdir(str(path)):
                            path = Path(str(path)) # Ensure object
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
                    
                    # SCAN S3
                    elif t["type"] == "S3 Object Storage" and "s3_creds" in t:
                        c = t["s3_creds"]
                        # Ensure active connection
                        s3_connector.connect(c["endpoint"], c["access"], c["secret"], c["bucket"])
                        for obj in s3_connector.list_files():
                             content = s3_connector.get_file_content(obj['Key'])
                             if content:
                                 res = requests.post(f"{API_URL}/scan/file", headers=headers, files={"file":(obj['Key'], io.BytesIO(content))})
                                 if res.status_code == 200:
                                     for r in res.json().get("results", []):
                                         if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                         results.append({"Source":t["name"], "Identity":obj['Key'], "Type":r["type"], "Data":r["text"], "Category":classification_engine.classify_sensitivity(r["type"])})
                    
                    # SCAN DB
                    elif "Database" in t["type"] or "API" in t["type"]:
                         internal_type = 'postgresql' if 'Database' in t["type"] else 'api_get'
                         # If it's a DB, we need to pass a query. For demo, simplified.
                         data = db_connector.scan_source(internal_type, t["details"])
                         for d in data:
                             res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text":d})
                             if res.status_code == 200:
                                 for r in res.json().get("results", []):
                                     if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                     results.append({"Source":t["name"], "Identity":"Row", "Type":r["type"], "Data":r["text"], "Category":classification_engine.classify_sensitivity(r["type"])})

                st.session_state["scan_results"] = results
                status.update(label="✅ Completed", state="complete")
        
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            df = pd.DataFrame(st.session_state["scan_results"])
            st.dataframe(df, use_container_width=True)

    elif page == "📊 Dashboard":
        st.title("📊 Security Dashboard")
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 c1, c2 = st.columns(2)
                 c1.metric("Total Findings", len(df))
                 c2.metric("Critical", len(df[df["Category"].str.contains("Spesifik", na=False)]))
                 st.bar_chart(df["Category"].value_counts())
        else: st.info("No active scan results.")

if __name__ == "__main__":
    main()
