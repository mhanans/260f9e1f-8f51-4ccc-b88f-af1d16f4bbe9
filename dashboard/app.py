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
PURPOSES_FILE = BASE_DIR / "purposes.json"

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

# Purpose Logic
def load_purposes():
    if PURPOSES_FILE.exists():
        try:
            with open(PURPOSES_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_purposes(data):
    with open(PURPOSES_FILE, "w") as f: json.dump(data, f, indent=2)

# --- Main App ---
def main():
    if "token" not in st.session_state: login(); return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 Data Explorer", "🗂️ Connections", "🚀 Scan Manager", "⚙️ Rules Engine", "📊 Dashboard", "📜 Audit Logs"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # ... Pages: Explorer, Connections (Same) ...
    # Simplified here...
    
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
        # (Same as before)
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
    elif page == "⚙️ Rules Engine": 
        # (Same as before)
         st.title("⚙️ Rules Management")
         st.caption("Manage detection patterns. Ensure 1 rule per Entity.")
         config = load_rules_config()
         recogs = config.get("custom_recognizers", [])
         with st.expander("🧪 Test a Regex Pattern", expanded=True):
            cols = st.columns([3, 1])
            test_regex = cols[0].text_input("Enter Regex Pattern", r"\b\d{16}\b")
            test_text = st.text_area("Test String (Sample Data)", "Ini adalah NIK saya 3201123412341234 yang valid.")
            if st.button("Run Test"):
                try:
                    matches = re.finditer(test_regex, test_text)
                    results = [m.group(0) for m in matches]
                    if results: st.success(f"✅ Found {len(results)} matches: {results}")
                    else: st.warning("No matches found.")
                except Exception as e: st.error(f"Regex Error: {e}")
         st.divider()
         st.subheader("Manage Detection Rules")
         existing_entities = set([r["entity"] for r in recogs])
         for i, rec in enumerate(recogs):
            with st.container():
                with st.expander(f"🧩 {rec['entity']} ({rec['name']})", expanded=False):
                    c1, c2 = st.columns([3, 1])
                    new_regex = c1.text_input("Regex", value=rec["regex"], key=f"rex_{i}")
                    new_score = c2.slider("Score", 0.0, 1.0, rec["score"], key=f"sco_{i}")
                    new_ctx = st.text_input("Context (comma sep.)", value=",".join(rec.get("context", [])), key=f"ctx_{i}")
                    is_active = st.checkbox("Active", value=rec.get("active", True), key=f"act_{i}")
                    if st.button("🗑️ Delete Rule", key=f"del_rule_{i}"):
                        recogs.pop(i)
                        config["custom_recognizers"] = recogs
                        save_rules_config(config)
                        st.rerun()
                    rec["regex"] = new_regex
                    rec["score"] = new_score
                    rec["context"] = [x.strip() for x in new_ctx.split(",") if x.strip()]
                    rec["active"] = is_active
         st.write("### Add New Rule")
         with st.form("add_rule_form"):
            new_entity = st.text_input("Entity Name (Unique)", placeholder="ID_NEW_CARD").upper()
            new_pattern = st.text_input("Regex Pattern", placeholder=r"\b[A-Z]{3}-\d{4}\b")
            if st.form_submit_button("Add Rule"):
                if not new_entity or not new_pattern: st.error("Entity Name and Regex are required.")
                elif new_entity in [r["entity"] for r in recogs]: st.error(f"Rule for {new_entity} already exists!")
                else:
                    new_rule = {"name": f"{new_entity.lower()}_recognizer", "entity": new_entity, "regex": new_pattern, "score": 0.5, "context": [], "active": True}
                    recogs.append(new_rule)
                    config["custom_recognizers"] = recogs
                    save_rules_config(config)
                    st.success(f"Added rule for {new_entity}"); st.rerun()
         st.divider()
         if st.button("💾 SAVE ALL CHANGES", type="primary"):
            config["custom_recognizers"] = recogs
            save_rules_config(config)
            st.success("Configuration Saved & Engine Reloaded!")
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
        # (Fixed Logic)
        st.title("📜 Audit Logs")
        log_file = LOG_DIR / "audit.log"
        if log_file.exists():
            logs = []
            with open(log_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        # Handle JSON array vs Newline Delimited JSON
                        if line.startswith("[") and line.endswith("]"):
                            logs.extend(json.loads(line))
                        else:
                            logs.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass # Skip bad lines
            
            if logs:
                st.dataframe(pd.DataFrame(logs).iloc[::-1], use_container_width=True)
            else:
                st.info("No logs found.")
    
    # --- Page: Scan Manager (Fixed KeyError) ---
    if page == "🚀 Scan Manager":
        st.title("🚀 Scan Assistant")
        
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
                    current_val = row["Purpose"]
                    if new_purposes.get(key) != current_val:
                        new_purposes[key] = current_val
                        changes_detected = True
                
                if changes_detected:
                    st.session_state["purposes"] = new_purposes
                    save_purposes(new_purposes)
                    st.rerun()

if __name__ == "__main__":
    main()
