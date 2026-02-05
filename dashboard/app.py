import sys
import os
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print(f"DEBUG: sys.path includes: {sys.path[:3]}")
print(f"DEBUG: CWD is: {os.getcwd()}")
print(f"DEBUG: Project Root is: {PROJECT_ROOT}")

import streamlit as st
import requests
import pandas as pd
import os
import io
import json
import re
import structlog
import time
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
PURPOSES_FILE = BASE_DIR / "purposes.json"

BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.WriteLoggerFactory(file=sys.stdout),
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

# Purpose Logic
def load_purposes():
    if PURPOSES_FILE.exists():
        try:
            with open(PURPOSES_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_purposes(data):
    with open(PURPOSES_FILE, "w") as f: json.dump(data, f, indent=2)

if "data_connections" not in st.session_state:
    st.session_state["data_connections"] = load_connections()
if "purposes" not in st.session_state:
    st.session_state["purposes"] = load_purposes()

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
    page = st.sidebar.radio("Go to", ["📂 Data Explorer", "🗂️ Connections", "🚀 Scan Manager", "🔗 Data Lineage", "⚙️ Rules Engine", "✅ Compliance Registry", "📊 Dashboard", "📜 Audit Logs"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # ... Pages: Explorer, Connections (Same) ...
    
    if page == "📂 Data Explorer": 
        # (Same as before)
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
                        for f in up_files: save_uploaded_file(f, path); st.toast(f"Saved: {f.name}")
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

    elif page == "🗂️ Connections": 
        st.title("🗂️ Connection Manager")
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
        with st.expander("➕ Add New Data Source", expanded=False):
             c1,c2 = st.columns([1,2])
             ct=c1.selectbox("Type", ["API Endpoint", "PostgreSQL Database", "S3 Object Storage", "Local Storage Path"])
             cn=c2.text_input("Name")
             is_val=False; cd=""
             s3c=None
             
             if ct=="Local Storage Path":
                 p=st.text_input("Path", str(BASE_DIR.absolute()))
                 if st.button("Add Path"): cd=p; is_val=True
             elif "Database" in ct:
                 c1,c2=st.columns(2); h=c1.text_input("Host","localhost"); p=c2.text_input("Port","5432")
                 u=c1.text_input("User","postgres"); w=c2.text_input("Pass",type="password")
                 d=st.text_input("DB","postgres")
                 if st.button("Add DB"): cd=f"postgresql://{u}:{w}@{h}:{p}/{d}"; is_val=True
             elif ct=="S3 Object Storage":
                 c1,c2=st.columns(2); e=c1.text_input("Endpoint"); b=c2.text_input("Bucket")
                 k=c1.text_input("Key"); s=c2.text_input("Secret",type="password")
                 if st.button("Add S3"): 
                     if not e.startswith("http"): e="http://"+e
                     s3c={"endpoint":e,"bucket":b,"access":k,"secret":s}; is_val=True
             elif ct=="API Endpoint":
                 u = st.text_input("URL"); 
                 if st.button("Add API"): cd=u; is_val=True

             if is_val and cn:
                 nc = {"id":str(time.time()),"name":cn,"type":ct,"details":cd}
                 if s3c: nc["s3_creds"]=s3c
                 st.session_state["data_connections"].append(nc)
                 save_connections(st.session_state["data_connections"])
                 st.rerun()

    elif page == "✅ Compliance Registry":
        st.title("✅ Compliance Registry")
        st.caption("Master list of confirmed Personal Data assets (ROPA basis).")
        
        # Load Saved Data
        try:
            res = requests.get(f"{API_URL}/compliance/", headers=headers)
            if res.status_code == 200:
                saved_data = res.json()
            else:
                st.error("Failed to load compliance data.")
                saved_data = []
        except Exception as e:
            st.error(f"API Connection Error: {e}")
            saved_data = []

        if saved_data:
            df_comp = pd.DataFrame(saved_data)
            
            # Filters
            c1, c2, c3 = st.columns(3)
            f_source = c1.multiselect("Source", df_comp["source"].unique())
            f_type = c2.multiselect("PII Type", df_comp["pii_type"].unique())
            f_status = c3.multiselect("Status", df_comp["status"].unique())
            
            if f_source: df_comp = df_comp[df_comp["source"].isin(f_source)]
            if f_type: df_comp = df_comp[df_comp["pii_type"].isin(f_type)]
            if f_status: df_comp = df_comp[df_comp["status"].isin(f_status)]
            
            # Interactive Editor for Quick Updates
            st.subheader(f"Registered Assets ({len(df_comp)})")
            
            # Layout for editor
            edited_comp = st.data_editor(
                df_comp,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "source": st.column_config.TextColumn("Source", disabled=True),
                    "location": st.column_config.TextColumn("Location", disabled=True),
                    "pii_type": st.column_config.TextColumn("Type", disabled=True),
                    "confidence_score": st.column_config.NumberColumn("Conf.", format="%.2f", disabled=True),
                    "status": st.column_config.SelectboxColumn("Status", options=["Active", "False Positive", "Resolved", "Archived"]),
                    "purpose": st.column_config.SelectboxColumn("Purpose", options=["Recruitment", "Payroll", "Marketing", "Legal", "Operational", "Other"]),
                    "sensitivity": st.column_config.SelectboxColumn("Sensitivity", options=["Spesifik", "Umum", "High", "Medium", "Low"])
                },
                use_container_width=True,
                key="compliance_editor",
                num_rows="dynamic" # Allow deletion
            )
            
            # Detect Changes
            if st.button("💾 Save Registry Changes"):
                # Ideally, we diff 'edited_comp' with 'saved_data' or assume row-by-row update
                # For simplicity, we loop and PUT updates.
                progress = st.progress(0)
                for i, row in edited_comp.iterrows():
                    # Check if modified? (Optimization skipped for prototype)
                    # We need the ID.
                    if "id" in row and row["id"]:
                         payload = {
                             "purpose": row["purpose"],
                             "status": row["status"],
                             "sensitivity": row["sensitivity"],
                             "source": row["source"], # required structure
                             "location": row["location"],
                             "pii_type": row["pii_type"],
                             "confidence_score": row["confidence_score"]
                         }
                         requests.put(f"{API_URL}/compliance/{row['id']}", headers=headers, json=payload)
                    progress.progress((i + 1) / len(edited_comp))
                st.success("Registry Updated!")
                st.rerun()

        else:
            st.info("No data registered yet. Go to 'Scan Manager' to discover and save assets.")

    # --- Page: Scan Manager ---
    elif page == "🚀 Scan Manager":
        st.title("🚀 Scan Assistant")
        # ... (Scan Logic Hidden for Brevity, implicitly preserved if not overwritten) ...
        # RE-INJECTING SCAN LOGIC as we are inside the condition
        
        # Mode Selection
        scan_mode = st.radio("Scan Mode", ["🚀 Quick Scan (Auto)", "🎯 TargetedDB Scan (Query Builder)"], horizontal=True)
        
        if scan_mode == "🚀 Quick Scan (Auto)":
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
                             
                             if structured_data:
                                 for item in structured_data:
                                     text_to_scan = item.get("value", "") 
                                     if not text_to_scan: continue
                                     
                                     res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text_to_scan})
                                     if res.status_code == 200:
                                         for r in res.json().get("results", []):
                                             if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                             loc=f"{item.get('container','?')} ({item.get('field','?')})"
                                             results.append({"Source":t["name"],"Table/File Location":loc,"Type":r["type"],"Data":r["text"],"Category":classification_engine.classify_sensitivity(r["type"])})
                     st.session_state["scan_results"] = results
                     status.update(label="Complete", state="complete")
        
        elif scan_mode == "🎯 TargetedDB Scan (Query Builder)":
            st.subheader("Targeted Database Scanning")
            db_conns = [c for c in st.session_state["data_connections"] if "Database" in c["type"]]
            if not db_conns: st.warning("No Database connections found.")
            else:
                selected_db_name = st.selectbox("Select Database", [c["name"] for c in db_conns])
                target_conn = next(c for c in db_conns if c["name"] == selected_db_name)
                
                if st.button("🕷️ Crawl Metadata (Discover Schema)"):
                    with st.spinner("Crawling Schema..."):
                        meta = db_connector.get_schema_metadata('postgresql', target_conn["details"])
                        st.session_state["db_metadata"] = meta
                
                if "db_metadata" in st.session_state:
                    meta = st.session_state["db_metadata"]
                    st.write(f"### Discovered Tables: {len(meta)}")
                    with st.expander("🔎 Define Scan Criteria", expanded=True):
                        c1, c2, c3 = st.columns(3)
                        filter_tbl = c1.text_input("Table Name Contains", "")
                        filter_col = c2.text_input("Column Name Contains", "")
                        filter_rows = c3.number_input("Min Row Count", 0, value=0)
                    filtered_tables = []
                    for t in meta:
                        match_tbl = filter_tbl.lower() in t["table"].lower()
                        match_col = True
                        if filter_col: match_col = any(filter_col.lower() in c.lower() for c in t["columns"])
                        match_rows = t["row_count"] >= filter_rows
                        if match_tbl and match_col and match_rows: filtered_tables.append(t)
                    st.write(f"#### 🎯 Audit Candidates ({len(filtered_tables)} Tables)")
                    selected_tables = []
                    for t in filtered_tables:
                        is_checked = st.checkbox(f"**{t['table']}** (Rows: {t['row_count']}) | Cols: {len(t['columns'])}", value=True, key=f"tbl_{t['table']}")
                        if is_checked: selected_tables.append(t["table"])
                    st.divider()
                    scan_limit = st.slider("Max Rows to Scan per Table", 10, 1000, 50)
                    
                    if st.button("🚀 Start Targeted Scan", type="primary", disabled=not selected_tables):
                        results = []
                        with st.status("Performing Deep Scan...") as status:
                            for tbl in selected_tables:
                                status.update(label=f"Scanning Table: {tbl}...", state="running")
                                raw_data = db_connector.scan_target('postgresql', target_conn["details"], tbl, limit=scan_limit)
                                for item in raw_data:
                                    text_to_scan = item.get("value", "")
                                    if not text_to_scan: continue

                                    res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text_to_scan})
                                    if res.status_code == 200:
                                        for r in res.json().get("results", []):
                                            if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                            results.append({
                                                "Source": target_conn["name"],
                                                "Table/File Location": f"{item.get('container','?')} ({item.get('field','?')})",
                                                "Type": r["type"],
                                                "Data": r["text"],
                                                "Category": classification_engine.classify_sensitivity(r["type"])
                                            })
                            st.session_state["scan_results"] = results
                            status.update(label="✅ Scan Complete!", state="complete")
        
        if "scan_results" in st.session_state:
            st.divider()
            results = st.session_state["scan_results"]
            df = pd.DataFrame(results)
            
            if not df.empty:
                # --- Purpose Mapping Integration ---
                purposes = st.session_state.get("purposes", {})
                display_df = df.copy()
                display_df["Purpose"] = display_df.apply(lambda x: purposes.get(f"{x['Source']}|{x['Table/File Location']}", ""), axis=1)
                display_df["Save"] = True # Add default check to save
                
                # Header & Controls
                c1, c2 = st.columns([3, 1])
                c1.write("### 🚨 Detected Data Candidates")
                c1.caption("Review findings below. Uncheck 'Save' to discard false positives or duplicates.")
                show_unmasked = c2.toggle("👁️ Show Unmasked Data", value=False)
                
                # Masking
                if not show_unmasked:
                     display_df["Data"] = display_df.apply(lambda x: mask_data(x["Data"], x.get("Type")), axis=1)
                
                # Interactive Table
                edited_df = st.data_editor(
                    display_df, 
                    column_config={
                        "Save": st.column_config.CheckboxColumn("💾 Save?", help="Check to register this finding to Compliance Registry", default=True),
                        "Source": st.column_config.TextColumn("Source", disabled=True),
                        "Table/File Location": st.column_config.TextColumn("Location", disabled=True),
                        "Type": st.column_config.TextColumn("PII Type", disabled=True),
                        "Data": st.column_config.TextColumn("Content", disabled=True),
                        "Category": st.column_config.TextColumn("Sensitivity", disabled=True),
                        "Purpose": st.column_config.SelectboxColumn("Purpose", options=["Recruitment", "Marketing", "HR", "Legal", "Other"], required=False)
                    },
                    use_container_width=True,
                    num_rows="fixed",
                    key="scan_results_editor"
                )
                
                # --- Data Saving Logic ---
                col_btn, col_info = st.columns([1, 4])
                if col_btn.button("💾 CONFIRM & SAVE", type="primary"):
                    saved_count = 0
                    with st.spinner("Registering assets..."):
                        for idx, row in edited_df.iterrows():
                            if row["Save"]:
                                # Construct Payload matching DetectedData model
                                payload = {
                                    "source": row["Source"],
                                    "location": row["Table/File Location"],
                                    "pii_type": row["Type"],
                                    "sensitivity": row["Category"],
                                    "purpose": row["Purpose"] if row["Purpose"] else "Unassigned",
                                    "confidence_score": 1.0, # Confirmed by user
                                    "status": "Active"
                                }
                                try:
                                    requests.post(f"{API_URL}/compliance/", headers=headers, json=payload)
                                    saved_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to save row {idx}: {e}")
                    
                    st.success(f"Successfully registered {saved_count} compliance assets! Check 'Compliance Registry'.")




    elif page == "⚙️ Rules Engine":         st.caption("Manage detection patterns, sensitivity classification, and ignore lists.")
         
         # Load Rules from API (Single Source of Truth)
         try:
             rules_res = requests.get(f"{API_URL}/config/rules", headers=headers)
             if rules_res.status_code == 200:
                 all_rules = rules_res.json()
             else:
                 all_rules = []
                 st.error("Failed to load rules from API")
         except:
             all_rules = []
         
         # Categorize Rules
         regex_rules = [r for r in all_rules if r["rule_type"] == "regex"]
         deny_rules = [r for r in all_rules if r["rule_type"] == "deny_list"] # String literals to ignore
         exclude_rules = [r for r in all_rules if r["rule_type"] == "exclude_entity"] # Whole entity types to ignore
         sensitivity_rules = [r for r in all_rules if r["rule_type"] == "sensitivity"] # Entity -> Sensitivity Map
         
         tab_detect, tab_class, tab_ignore = st.tabs(["🕵️ Detection Rules (Regex)", "🏷️ Sensitivity & Tags", "🚫 Ignore Lists"])
         
         # --- TAB 1: Detection Rules ---
         with tab_detect:
             st.subheader("Custom Regex Recognizers")
             
             for r in regex_rules:
                 with st.expander(f"🧩 {r['entity_type']} ({r['name']})", expanded=False):
                     c1, c2, c3 = st.columns([3, 1, 1])
                     # We can't easily edit via API PUT without implemented update endpoint for all fields, 
                     # but we can toggle active state.
                     st.text_input("Regex", value=r["pattern"], disabled=True, key=f"d_rex_{r['id']}")
                     st.slider("Score", 0.0, 1.0, r["score"], disabled=True, key=f"d_score_{r['id']}")
                     
                     is_active = st.toggle("Active", value=r["is_active"], key=f"act_{r['id']}")
                     if is_active != r["is_active"]:
                         requests.put(f"{API_URL}/config/rules/{r['id']}", headers=headers, params={"is_active": is_active})
                         st.rerun()
                         
                     if st.button("🗑️ Delete", key=f"del_{r['id']}"):
                         requests.delete(f"{API_URL}/config/rules/{r['id']}", headers=headers)
                         st.rerun()

             st.divider()
             st.write("### Add New Regex Rule")
             with st.form("add_regex"):
                c1, c2 = st.columns(2)
                new_name = c1.text_input("Rule Name (Unique)", placeholder="my_custom_rule")
                new_entity = c2.text_input("Entity Tag", placeholder="ID_CUSTOM").upper()
                new_pattern = st.text_input("Regex Pattern", placeholder=r"\b\d{5}\b")
                new_score = st.slider("Confidence Score", 0.1, 1.0, 0.5)
                new_ctx = st.text_input("Context Words (comma sep)", placeholder="header, label")
                
                if st.form_submit_button("Add Detection Rule"):
                    if new_name and new_entity and new_pattern:
                        payload = {
                            "name": new_name,
                            "rule_type": "regex",
                            "pattern": new_pattern,
                            "score": new_score,
                            "entity_type": new_entity,
                            "context_keywords": json.dumps([x.strip() for x in new_ctx.split(",") if x.strip()]),
                            "is_active": True
                        }
                        res = requests.post(f"{API_URL}/config/rules", headers=headers, json=payload)
                        if res.status_code == 200: st.success("Added!"); st.rerun()
                        else: st.error(res.text)

         # --- TAB 2: Sensitivity Map ---
         with tab_class:
             st.subheader("Sensitivity Mapping")
             st.caption("Map PII Entity Types (e.g., ID_NIK) to Classification Levels (e.g., Spesifik).")
             
             # Display current map
             if sensitivity_rules:
                 df_sens = pd.DataFrame(sensitivity_rules)
                 st.dataframe(
                     df_sens[["entity_type", "pattern"]].rename(columns={"entity_type": "PII Entity", "pattern": "Classification"}),
                     use_container_width=True
                 )
                 
                 # Delete UI requires ID lookup
                 opts = {f"{r['entity_type']} -> {r['pattern']}": r['id'] for r in sensitivity_rules}
                 del_target = st.selectbox("Select Rule to Delete", ["None"] + list(opts.keys()))
                 if del_target != "None":
                     if st.button("Remove Selected Mapping"):
                         requests.delete(f"{API_URL}/config/rules/{opts[del_target]}", headers=headers)
                         st.rerun()
                         
             st.divider()
             st.write("### Add Classification Rule")
             with st.form("add_class"):
                 c1, c2 = st.columns(2)
                 # Suggest entities from Detection Rules + Default Presidio
                 known_entities = ["ID_NIK", "ID_NPWP", "PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON", "LOCATION", "DATE_TIME"] 
                 known_entities += list(set([r['entity_type'] for r in regex_rules]))
                 
                 s_entity = c1.selectbox("Entity Type", sorted(list(set(known_entities))))
                 s_label = c2.text_input("Classification Label", "Spesifik (Confidential)")
                 
                 if st.form_submit_button("Map Entity"):
                     payload = {
                        "name": f"map_{s_entity}_{int(time.time())}", # unique logic
                        "rule_type": "sensitivity",
                        "pattern": s_label, # Storing label in 'pattern' field to reuse model
                        "entity_type": s_entity,
                        "score": 1.0, 
                        "is_active": True
                     }
                     res = requests.post(f"{API_URL}/config/rules", headers=headers, json=payload)
                     if res.status_code == 200: st.success("Mapped!"); st.rerun()
                     else: st.error(res.text)

         # --- TAB 3: Ignore Lists ---
         with tab_ignore:
             st.subheader("🚫 False Positive Management")
             
             c1, c2 = st.columns(2)
             
             with c1:
                 st.write("#### Deny List (Strings)")
                 st.caption("Exact text to always ignore (e.g., table headers).")
                 for r in deny_rules:
                     col_a, col_b = st.columns([4,1])
                     col_a.code(r["pattern"])
                     if col_b.button("❌", key=f"del_d_{r['id']}"):
                         requests.delete(f"{API_URL}/config/rules/{r['id']}", headers=headers)
                         st.rerun()
                 
                 new_deny = st.text_input("Add String to Ignore")
                 if st.button("Add to Deny List"):
                     if new_deny:
                         payload = {"name": f"deny_{int(time.time())}", "rule_type": "deny_list", "pattern": new_deny, "entity_type": "DENY", "score":1.0}
                         requests.post(f"{API_URL}/config/rules", headers=headers, json=payload)
                         st.rerun()
             
             with c2:
                 st.write("#### Exclude Entities")
                 st.caption("PII Types to completely disable scanning for.")
                 for r in exclude_rules:
                     col_a, col_b = st.columns([4,1])
                     col_a.code(r["entity_type"]) # Assuming stored in entity_type or pattern
                     if col_b.button("❌", key=f"del_e_{r['id']}"):
                         requests.delete(f"{API_URL}/config/rules/{r['id']}", headers=headers)
                         st.rerun()
                 
                 new_ex = st.selectbox("Select Entity to Disable", ["DATE_TIME", "NRP", "LOCATION", "PERSON"])
                 if st.button("Disable Entity"):
                     payload = {"name": f"ex_{new_ex}_{int(time.time())}", "rule_type": "exclude_entity", "pattern": "IGNORE", "entity_type": new_ex, "score":1.0}
                     requests.post(f"{API_URL}/config/rules", headers=headers, json=payload)
                     st.rerun()

    elif page == "📊 Dashboard": 
        # (Same as before)
        st.title("📊 Security Dashboard")
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 m1, m2, m3 = st.columns(3)
                 m1.metric("Findings", len(df))
                 m2.metric("Critical", len(df[df["Category"].str.contains("Spesifik", na=False)]))
                 m3.metric("Sources Active", len(st.session_state["data_connections"]))
                 st.bar_chart(df["Category"].value_counts())
        else: st.info("No active scan results.")
    elif page == "📜 Audit Logs": 
        # (Fetched from DB via API)
        st.title("📜 Audit Logs")
        try:
            res = requests.get(f"{API_URL}/audit/", headers=headers, params={"limit": 100})
            if res.status_code == 200:
                logs = res.json()
                if logs:
                    df_logs = pd.DataFrame(logs)
                    
                    # Optional column reordering or cleanup
                    if "id" in df_logs.columns:
                        cols = ["timestamp", "user_email", "action", "endpoint", "ip_address", "details"]
                        # Filter only existing columns
                        cols = [c for c in cols if c in df_logs.columns]
                        df_logs = df_logs[cols]
                    
                    st.dataframe(df_logs, use_container_width=True)
                else:
                    st.info("No audit logs found in database.")
            else:
                st.error(f"Failed to fetch logs: {res.status_code} - {res.text}")
        except Exception as e:
            st.error(f"Error connecting to API: {e}")
    
    # --- Page: Data Lineage ---
    elif page == "🔗 Data Lineage":
        st.title("🔗 Data Lineage & Flow")
        st.caption("Visualize data movement, transformations, and PII propagation.")
        
        from lineage.graph import lineage_engine
        
        tab1, tab2, tab3 = st.tabs(["🧩 SQL Parser (Simulation)", "🌐 Global Metadata Sync", "✍️ Manual Builder"])
        
        with tab1:
            st.subheader("Extract Lineage from SQL (Simulation Mode)")
            st.caption("Quickly visualize SQL transformations without connecting to a DB.")
            sql_input = st.text_area("Paste SQL Query", height=150, placeholder="CREATE TABLE stg_users AS SELECT id, email as user_email FROM raw_users")
            if st.button("Analyze SQL"):
                if sql_input:
                    lineage_engine.parse_sql(sql_input)
                    st.success("Parsed successfully!")
        
        with tab2:
            st.subheader("🌐 Global Metadata Sync")
            st.caption("Scan ALL active connections to build a unified Cross-System Catalog.")
            
            if st.button("🚀 Sync All Sources & Build Catalog"):
                with st.spinner("Crawling all active connections..."):
                    all_conns = st.session_state.get("data_connections", [])
                    
                    # 1. Build from Active Connections (DB, S3)
                    lineage_engine.build_global_catalog(all_connections=all_conns)
                    
                    # 2. Ingest Historical File Scans (CSV)
                    with st.spinner("Ingesting historical scan results..."):
                        lineage_engine.inject_scan_results("data.csv")
                    
                    # 3. Propagate PII Tags downstream
                    with st.spinner("Propagating compliance tags..."):
                        lineage_engine.propagate_pii_labels()
                    
                    st.success(f"Global Catalog built with {len(lineage_engine.nodes)} nodes! (Injected {len([n for n in lineage_engine.nodes if 'file' in n])} file nodes)")
                    st.rerun()
            
            st.divider()
            search_query = st.text_input("🔍 Search Catalog (Table or Column Name)", "") 
            if search_query:
                # We can filter visual graph below based on this query but for now just basic text
                st.caption(f"Filtering graph for: {search_query} (Not fully implemented in viz yet)")

        with tab3:
            st.subheader("Manual Flow Injection")
            c1, c2, c3 = st.columns(3)
            src = c1.text_input("Source Node", placeholder="Script.py")
            tgt = c2.text_input("Target Node", placeholder="Warehouse.Table")
            desc = c3.text_input("Description", placeholder="ETL Process")
            if st.button("Add Flow"):
                if src and tgt:
                    lineage_engine.add_manual_lineage(src, tgt, desc)
                    st.success(f"Added: {src} -> {tgt}")
        
        st.divider()
        st.subheader("🕸️ Lineage Graph")
        
        # --- Visualization Controls ---
        c1, c2, c3 = st.columns([1, 2, 2])
        show_cols = c1.toggle("Show Columns", value=True)
        show_pii_only = c1.toggle("Show PII Only", value=False)
        
        graph_data = lineage_engine.get_graph()
        nodes = graph_data["nodes"]
        edges = graph_data["edges"]
        
        # Filter Logic
        if show_pii_only:
            # Keep nodes that have PII OR are parents of PII nodes OR are connected to PII flow
            # Simplified: Keep PII nodes + Tables containing them
            pii_ids = {n["id"] for n in nodes if n.get("pii_present")}
            nodes = [n for n in nodes if n["id"] in pii_ids or n["type"] in ["table", "bucket", "file"]] # Keep containers for context
            edges = [e for e in edges if e["source"] in [n["id"] for n in nodes] and e["target"] in [n["id"] for n in nodes]]

        if not show_cols:
            nodes = [n for n in nodes if n["type"] != "column"]
            edges = [e for e in edges if "column" not in e["source"] and "column" not in e["target"]]
            
        # Analysis Selectors
        pii_node_ids = [n["id"] for n in nodes if n.get("pii_present")]
        selected_node = c2.selectbox("🔍 Trace Analysis (Select Node)", ["None"] + sorted([n["id"] for n in nodes]))
        
        upstream_set = set()
        downstream_set = set()
        
        if selected_node != "None":
            downstream_set = lineage_engine.get_impact_path(selected_node)
            upstream_set = lineage_engine.get_upstream_path(selected_node)
            c3.info(f"Origin: {len(upstream_set)-1} nodes | Impact: {len(downstream_set)-1} nodes")

        if not nodes and not edges:
            st.info("Graph is empty.")
        else:
            import graphviz
            dot = graphviz.Digraph(comment='Data Lineage', graph_attr={'rankdir': 'LR', 'bgcolor': '#0e1117', 'compound': 'true'})
            
            # Group nodes by System (Clustering)
            systems = {}
            for n in nodes:
                sys = n["data"].get("system", "Uncategorized")
                if sys not in systems: systems[sys] = []
                systems[sys].append(n)
                
            for sys_name, sys_nodes in systems.items():
                with dot.subgraph(name=f"cluster_{sys_name}") as c:
                    c.attr(label=sys_name, style='dashed', color='#555555', fontcolor='#aaaaaa')
                    
                    for n in sys_nodes:
                        # Styling
                        color = "#444444"
                        fill = "#1e1e1e"
                        shape = "box"
                        fontcolor = "white"
                        penwidth = "1"
                        
                        if n["type"] == "column": 
                            shape = "ellipse"; fill = "#252525"
                        elif n["type"] in ["table", "file", "bucket"]:
                            fill = "#0d47a1" # Blue for containers
                            
                        # PII
                        if n.get("pii_present"):
                            fill = "#b71c1c" # Red
                            color = "#ffcdd2"
                        
                        # Highlighting
                        if selected_node != "None":
                            if n["id"] == selected_node:
                                penwidth = "3"; color = "#00e676" # Green Origin
                            elif n["id"] in downstream_set:
                                penwidth = "2"; color = "#ffea00" # Yellow Impact
                            elif n["id"] in upstream_set:
                                penwidth = "2"; color = "#00b0ff" # Cyan Upstream
                            else:
                                fill = "#121212"; fontcolor = "#424242" # Dimmed
                        
                        label = f"{n['label']}"
                        if n.get("pii_present"): label += f"\n⚠️{n['data'].get('pii_type')[:3]}"
                        
                        c.node(n["id"], label, shape=shape, style="filled,rounded", fillcolor=fill, color=color, fontcolor=fontcolor, penwidth=penwidth)

            for e in edges:
                color = "#666666"
                if selected_node != "None":
                    if e["source"] in downstream_set and e["target"] in downstream_set:
                        color = "#ffea00"
                    elif e["source"] in upstream_set and e["target"] in upstream_set:
                        color = "#00b0ff"
                    else:
                        color = "#222222"
                
                dot.edge(e["source"], e["target"], label=e["transformation"] or "", color=color)
            
            st.graphviz_chart(dot, use_container_width=True)
            
            with st.expander("Show Raw JSON"):
                st.json(graph_data)
    
    # --- Page: Scan Manager (Fixed KeyError) ---
        
        if scan_mode == "🚀 Quick Scan (Auto)":
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
                             
                             if structured_data:
                                 for item in structured_data:
                                    # KEYERROR FIX: Use .get() with default
                                     text_to_scan = item.get("value", "") 
                                     if not text_to_scan: continue
                                     
                                     res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text_to_scan})
                                     if res.status_code == 200:
                                         for r in res.json().get("results", []):
                                             if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                             loc=f"{item.get('container','?')} ({item.get('field','?')})"
                                             results.append({"Source":t["name"],"Table/File Location":loc,"Type":r["type"],"Data":r["text"],"Category":classification_engine.classify_sensitivity(r["type"])})
                     st.session_state["scan_results"] = results
                     status.update(label="Complete", state="complete")
        
        elif scan_mode == "🎯 TargetedDB Scan (Query Builder)":
            st.subheader("Targeted Database Scanning")
            db_conns = [c for c in st.session_state["data_connections"] if "Database" in c["type"]]
            if not db_conns: st.warning("No Database connections found.")
            else:
                selected_db_name = st.selectbox("Select Database", [c["name"] for c in db_conns])
                target_conn = next(c for c in db_conns if c["name"] == selected_db_name)
                
                if st.button("🕷️ Crawl Metadata (Discover Schema)"):
                    with st.spinner("Crawling Schema..."):
                        meta = db_connector.get_schema_metadata('postgresql', target_conn["details"])
                        st.session_state["db_metadata"] = meta
                
                if "db_metadata" in st.session_state:
                    meta = st.session_state["db_metadata"]
                    st.write(f"### Discovered Tables: {len(meta)}")
                    with st.expander("🔎 Define Scan Criteria", expanded=True):
                        c1, c2, c3 = st.columns(3)
                        filter_tbl = c1.text_input("Table Name Contains", "")
                        filter_col = c2.text_input("Column Name Contains", "")
                        filter_rows = c3.number_input("Min Row Count", 0, value=0)
                    filtered_tables = []
                    for t in meta:
                        match_tbl = filter_tbl.lower() in t["table"].lower()
                        match_col = True
                        if filter_col: match_col = any(filter_col.lower() in c.lower() for c in t["columns"])
                        match_rows = t["row_count"] >= filter_rows
                        if match_tbl and match_col and match_rows: filtered_tables.append(t)
                    st.write(f"#### 🎯 Audit Candidates ({len(filtered_tables)} Tables)")
                    selected_tables = []
                    for t in filtered_tables:
                        is_checked = st.checkbox(f"**{t['table']}** (Rows: {t['row_count']}) | Cols: {len(t['columns'])}", value=True, key=f"tbl_{t['table']}")
                        if is_checked: selected_tables.append(t["table"])
                        with st.expander(f"Show Columns for {t['table']}"): st.write(t["columns"])
                    st.divider()
                    scan_limit = st.slider("Max Rows to Scan per Table", 10, 1000, 50)
                    
                    if st.button("🚀 Start Targeted Scan", type="primary", disabled=not selected_tables):
                        results = []
                        with st.status("Performing Deep Scan...") as status:
                            for tbl in selected_tables:
                                status.update(label=f"Scanning Table: {tbl}...", state="running")
                                raw_data = db_connector.scan_target('postgresql', target_conn["details"], tbl, limit=scan_limit)
                                for item in raw_data:
                                    text_to_scan = item.get("value", "")
                                    if not text_to_scan: continue

                                    res = requests.post(f"{API_URL}/scan/text", headers=headers, json={"text": text_to_scan})
                                    if res.status_code == 200:
                                        for r in res.json().get("results", []):
                                            if classification_engine.is_false_positive(r["text"], r["type"]): continue
                                            results.append({
                                                "Source": target_conn["name"],
                                                "Table/File Location": f"{item.get('container','?')} ({item.get('field','?')})",
                                                "Type": r["type"],
                                                "Data": r["text"],
                                                "Category": classification_engine.classify_sensitivity(r["type"])
                                            })
                            st.session_state["scan_results"] = results
                            status.update(label="✅ Scan Complete!", state="complete")
        
        if "scan_results" in st.session_state:
            st.divider()
            results = st.session_state["scan_results"]
            df = pd.DataFrame(results)
            
            if not df.empty:
                # --- Purpose Mapping Integration ---
                purposes = st.session_state.get("purposes", {})
                display_df = df.copy()
                
                # Add Purpose Column (Map key: Source|Location)
                display_df["Purpose"] = display_df.apply(lambda x: purposes.get(f"{x['Source']}|{x['Table/File Location']}", ""), axis=1)
                
                # Header & Controls
                c1, c2 = st.columns([3, 1])
                c1.write("### 🚨 Detected Data & Compliance")
                show_unmasked = c2.toggle("👁️ Show Unmasked Data", value=False)
                
                # Apply Masking
                if not show_unmasked:
                     display_df["Data"] = display_df.apply(lambda x: mask_data(x["Data"], x.get("Type")), axis=1)
                
                # Interactive Data Table
                edited_df = st.data_editor(
                    display_df, 
                    column_config={
                        "Source": st.column_config.TextColumn("Source", disabled=True),
                        "Table/File Location": st.column_config.TextColumn("Location", disabled=True),
                        "Type": st.column_config.TextColumn("PII Type", disabled=True),
                        "Data": st.column_config.TextColumn("Sensitive Data Sample", disabled=True),
                        "Category": st.column_config.TextColumn("Sensitivity", disabled=True),
                        "Purpose": st.column_config.SelectboxColumn(
                            "Processing Purpose (UU PDP)",
                            options=[
                                "Promosi / Marketing",
                                "Kontak Darurat / HR",
                                "Payroll / Gaji",
                                "Layanan Pelanggan",
                                "Kepatuhan Hukum",
                                "Operasional Bisnis",
                                "Analitik",
                                "Other"
                            ],
                            required=False,
                            help="Select the business purpose for collecting this data."
                        )
                    },
                    use_container_width=True,
                    num_rows="fixed",
                    key="scan_results_editor"
                )
                
                # Save Logic (Sync edited purpose back to session/file)
                new_purposes = purposes.copy()
                changes_detected = False
                
                for idx, row in edited_df.iterrows():
                    key = f"{row['Source']}|{row['Table/File Location']}"
                    current_stored_val = purposes.get(key, "")
                    new_ui_val = row["Purpose"]
                    
                    # Only update if the user explicitly changed this value in the UI
                    # This prevents unchanged rows (which hold the old value) from overwriting a change from another row with the same key
                    if new_ui_val != current_stored_val:
                        new_purposes[key] = new_ui_val
                        changes_detected = True
                
                if changes_detected:
                    st.session_state["purposes"] = new_purposes
                    save_purposes(new_purposes)
                    st.rerun()

if __name__ == "__main__":
    main()
