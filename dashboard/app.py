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

# Persistent Connections
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

# CSS
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

    # Rest of Pages (Explorer, Connections) same as before...
    # Focus update on Scan Manager results logic

    # --- Page: Data Explorer ---
    if page == "📂 Data Explorer":
        st.title("📂 Data Explorer")
        conn_options = {c["name"]: c for c in st.session_state["data_connections"] if "Storage" in c["type"]}
        selected_conn_name = st.selectbox("Select Storage Source", list(conn_options.keys()) if conn_options else [])
        if selected_conn_name:
            conn = conn_options[selected_conn_name]
            # [Local Logic]
            if conn["type"] == "Local Storage Path":
                path = Path(conn["details"])
                with st.expander(f"📤 Upload to {conn['name']}", expanded=True):
                    up_files = st.file_uploader("Drag & Drop", accept_multiple_files=True)
                    if up_files:
                        for f in up_files: save_uploaded_file(f, path)
                        st.rerun()
                files = list(path.glob("*")) if path.exists() else []
                st.write(f"### {len(files)} Items in Local Storage")
                for f in files:
                    c1, c2 = st.columns([6, 1])
                    c1.markdown(f"<div class='file-box'>📄 {f.name}</div>", unsafe_allow_html=True)
                    if c2.button("🗑️", key=f"d_{f.name}"): os.remove(f); st.rerun()
            # [S3 Logic]
            elif conn["type"] == "S3 Object Storage":
                c = conn.get("s3_creds", {}) 
                if c:
                    if s3_connector.bucket_name != c.get("bucket"): s3_connector.connect(c["endpoint"], c["access"], c["secret"], c["bucket"])
                    files = s3_connector.list_files()
                    st.write(f"### {len(files)} Objects in Bucket")
                    with st.expander("☁️ Upload", expanded=False):
                        up_s3 = st.file_uploader("S3 Upload", accept_multiple_files=True)
                        if up_s3: 
                            for f in up_s3: s3_connector.upload_file(f, f.name)
                            st.rerun()
                    for f in files:
                         c1, c2 = st.columns([6, 1])
                         c1.markdown(f"<div class='file-box'>☁️ {f['Key']}</div>", unsafe_allow_html=True)
                         if c2.button("🗑️", key=f"d_{f['Key']}"): s3_connector.delete_file(f['Key']); st.rerun()

        # Shared Results
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                st.write("### 🚨 Latest Findings")
                if st.toggle("Privacy Mode", True):
                    df["Data"] = df.apply(lambda row: mask_data(row["Data"], row["Type"]), axis=1)
                st.dataframe(df, use_container_width=True)

    # --- Page: Connections ---
    elif page == "🗂️ Connections":
        st.title("🗂️ Unified Connection Manager")
        st.subheader(f"Active Sources ({len(st.session_state['data_connections'])})")
        
        conns = st.session_state["data_connections"]
        for i, conn in enumerate(conns):
            badge = get_badge_html(conn["type"])
            display_details = conn.get("details", "")
            if "s3_creds" in conn: display_details = f"Endpoint={conn['s3_creds']['endpoint']}; Bucket={conn['s3_creds']['bucket']}"
            
            with st.container():
                cols = st.columns([0.7, 0.1, 0.1, 0.1])
                cols[0].markdown(f"<div class='source-box'><div><strong>{conn['name']}</strong> {badge}<br><small>{display_details}</small></div></div>", unsafe_allow_html=True)
                if cols[1].button("🔌", key=f"t_{i}"):
                    if "s3_creds" in conn: 
                         c = conn["s3_creds"]
                         suc, msg = s3_connector.connect(c["endpoint"], c["access"], c["secret"], c["bucket"])
                         if suc: st.toast("Success") 
                         else: st.error(msg)
                    elif "Database" in conn["type"]:
                        suc, msg = db_connector.test_connection('postgresql', conn["details"])
                        if suc: st.toast("Connected")
                        else: st.error(msg)
                    elif "API" in conn["type"]:
                        suc, msg = db_connector.test_connection('api_get', conn["details"])
                        if suc: st.toast(msg)
                        else: st.error(msg)
                if cols[3].button("🗑️", key=f"d_{i}"):
                    st.session_state["data_connections"].pop(i)
                    save_connections(st.session_state["data_connections"])
                    st.rerun()

        st.divider()
        # Add New UI (Same as previous, omitted for brevity but assumed present)
        # Re-implementing the add logic briefly to ensure functionality
        with st.expander("➕ Add New Data Source"):
            cols = st.columns([1, 2])
            c_type = cols[0].selectbox("Type", ["API Endpoint", "PostgreSQL Database", "S3 Object Storage", "Local Storage Path"])
            c_name = cols[1].text_input("Name")
            c_det = ""
            s3_creds_obj = None
            valid = False
            
            if c_type == "Local Storage Path":
                p = st.text_input("Path", value=str(BASE_DIR.absolute()))
                if st.button("Add Path"): c_det=p; valid=True
            elif "Database" in c_type:
                c1,c2 = st.columns(2)
                h=c1.text_input("Host","localhost"); p=c2.text_input("Port","5432")
                u=c1.text_input("User","postgres"); w=c2.text_input("Pass", type="password")
                d=st.text_input("DB Name","postgres")
                if st.button("Add DB"): c_det=f"postgresql://{u}:{w}@{h}:{p}/{d}"; valid=True
            elif c_type == "S3 Object Storage":
                c1,c2=st.columns(2); e=c1.text_input("Endpoint"); b=c2.text_input("Bucket")
                k=c1.text_input("Key"); s=c2.text_input("Secret", type="password")
                if st.button("Add S3"): 
                    if not e.startswith("http"): e="http://"+e
                    s3_creds_obj={"endpoint":e,"bucket":b,"access":k,"secret":s}; valid=True
            elif c_type == "API Endpoint":
                u = st.text_input("URL"); 
                if st.button("Add API"): c_det=u; valid=True

            if valid and c_name:
                new_c = {"id":str(time.time()), "name":c_name, "type":c_type, "details":c_det}
                if s3_creds_obj: new_c["s3_creds"] = s3_creds_obj
                st.session_state["data_connections"].append(new_c)
                save_connections(st.session_state["data_connections"])
                st.rerun()


    # --- Page: Scan Manager (UPDATED LOGIC) ---
    elif page == "🚀 Scan Manager":
        st.title("🚀 Unified Scan Manager")
        targets = []
        for conn in st.session_state["data_connections"]:
            if st.checkbox(f"{conn['name']} ({conn['type']})", value=True): targets.append(conn)

        if st.button("🚀 START SCAN", type="primary"):
            results = []
            with st.status("Scanning targets...") as status:
                for t in targets:
                    status.update(label=f"Scanning {t['name']}...", state="running")
                    
                    # Local & S3 (File based) logic...
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
                         # CALL NEW DB LOGIC (Get Structured Dicts)
                         structured_data = db_connector.scan_source(internal_type, t["details"])
                         
                         for item in structured_data:
                             # item = {source_type, container, field, value}
                             text_to_scan = item["value"]
                             res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text_to_scan})
                             if res.status_code == 200:
                                  for r in res.json().get("results", []):
                                     if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                     
                                     # FORMAT RESULT for DataFrame: Location = Table(Column)
                                     location_str = f"{item['container']} ({item['field']})"
                                     
                                     results.append({
                                         "Source": t["name"],
                                         "Identity": location_str, # REPLACES 'Record #1'
                                         "Type": r["type"], 
                                         "Data": r["text"], 
                                         "Category": classification_engine.classify_sensitivity(r["type"])
                                     })

                st.session_state["scan_results"] = results
                status.update(label="✅ Completed", state="complete")
        
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.divider()
            st.write("### 🚨 Findings Report")
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty and "Identity" in df.columns:
                 # Rename for clarity
                 df = df.rename(columns={"Identity": "Table/File Location"})
            st.dataframe(df, use_container_width=True)

    elif page == "📊 Dashboard":
        st.title("📊 Security Dashboard")
        if "scan_results" in st.session_state:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 c1, c2 = st.columns(2); c1.metric("Findings", len(df)); c2.metric("Critical", len(df[df["Category"].str.contains("Spesifik", na=False)]))
                 st.bar_chart(df["Category"].value_counts())
        else: st.info("No active scan results.")

if __name__ == "__main__":
    main()
