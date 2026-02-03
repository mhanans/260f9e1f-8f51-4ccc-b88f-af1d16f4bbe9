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

# --- Custom CSS (Force Contrast & Enterprise Theme) ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    
    /* Metrics Color Fix */
    [data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    [data-testid="stMetricValue"] { color: #1f77b4 !important; font-size: 1.8rem !important; }
    [data-testid="stMetricLabel"] { color: #555555 !important; font-weight: bold; }
    
    /* File Box Style */
    .file-box { 
        border: 1px solid #ddd; 
        padding: 12px; 
        border-radius: 8px; 
        background: #ffffff; 
        margin-bottom: 10px;
        color: #333;
        font-family: monospace;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] { background-color: #2c3e50; color: white; }
    .stButton>button { width: 100%; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def login():
    st.sidebar.title("🔐 Login")
    username = st.sidebar.text_input("Email", value="admin@example.com")
    password = st.sidebar.text_input("Password", type="password", value="password")
    if st.sidebar.button("Sign In"):
        try:
            res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
            if res.status_code == 200:
                st.session_state["token"] = res.json()["access_token"]
                logger.info("user_login", user=username, status="success")
                st.sidebar.success("Logged in successfully!")
                st.rerun()
            else:
                logger.warning("user_login", user=username, status="failed")
                st.sidebar.error("Invalid credentials")
        except Exception as e:
            st.sidebar.error(f"Connection Error: {e}")

def save_uploaded_file(uploaded_file):
    try:
        file_path = BASE_DIR / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        logger.info("file_upload", filename=uploaded_file.name, size=uploaded_file.size)
        return True
    except Exception as e:
        logger.error("file_upload_error", error=str(e))
        return False

def get_stored_files():
    return list(BASE_DIR.glob("*"))

def mask_data(text, entity_type=None):
    """Masks string based on entity type for Privacy (Poin 2)"""
    if not text: return ""
    
    if entity_type == "ID_NIK":
        if len(text) > 8:
            return text[:4] + "********" + text[-4:]
        return "********"
    
    if entity_type == "EMAIL_ADDRESS":
        if "@" in text:
            parts = text.split("@")
            return parts[0][0] + "***@" + parts[-1]
        return "***@***"
        
    visible = 2
    if len(text) <= visible * 2: return "****"
    return f"{text[:visible]}{'*' * (len(text) - visible*2)}{text[-visible:]}"

def read_local_file_content(file_path):
    try:
        if file_path.suffix == '.docx':
            doc = Document(file_path)
            return "\n".join([para.text for para in doc.paragraphs])
        # Add basic text support
        with open(file_path, "rb") as f:
            content_bytes = f.read()
            return content_bytes.decode('utf-8', errors='ignore')
    except Exception:
        return ""

def process_s3_file(key, headers):
    """
    Downloads file from S3, analyzes context/posture locally, and sends to API for PII scan.
    """
    content_bytes = s3_connector.get_file_content(key)
    if not content_bytes:
        return None

    # 1. Context Analysis (Local)
    # Decode for text matching (simple approximation for classification)
    try:
        content_str = content_bytes.decode('utf-8', errors='ignore')
    except:
        content_str = "" # Binary file or difficult encoding

    doc_categories = classification_engine.classify_document_category(content_str)
    
    # 2. Entropy / Security Posture
    sec_posture = "SAFE"
    if analytics_engine.check_security_posture(key, content_str[:100]) == "CRITICAL: Sensitive Column in Plain Text":
        sec_posture = "CRITICAL (Unencrypted)"

    # 3. PII Scan (API)
    files_payload = {"file": (key, io.BytesIO(content_bytes))}
    try:
        res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
        if res.status_code == 200:
            data = res.json()
            findings = []
            for finding in data.get("results", []):
                # False Positive Check
                if classification_engine.is_false_positive(finding["text"], finding["type"]):
                    continue

                sensitivity = classification_engine.classify_sensitivity(finding["type"])
                
                findings.append({
                    "File Name": key,
                    "Doc Category": ", ".join(doc_categories) or "General",
                    "PII Type": finding["type"],
                    "Kategori UU PDP": sensitivity,
                    "Detected Data": finding["text"],
                    "Confidence": finding["score"],
                    "Postur Keamanan": sec_posture
                })
            return findings
    except Exception as e:
        logger.error("s3_scan_error", key=key, error=str(e))
    
    return []

# --- Main App ---
def main():
    if "token" not in st.session_state:
        login()
        st.title("🛡️ Data Discovery System")
        st.info("Please login to access your secure workspace.")
        return

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 My Files (Storage)", "📊 Dashboard", "📜 Audit Logs", "⚙️ Configuration"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # --- Page: My Files (Data Lake) ---
    if page == "📂 My Files (Storage)":
        st.title("📂 Data Lake Management")
        
        # Storage Source Toggle
        storage_tab = st.radio("Storage Source", ["Local Storage (WSL)", "S3 Object Storage"], horizontal=True)

        # === LOCAL STORAGE MODE ===
        if storage_tab == "Local Storage (WSL)":
            st.caption(f"Physical Path: `{BASE_DIR.absolute()}`")

            # 1. Upload
            with st.expander("📤 Upload Files to Local Storage", expanded=False):
                uploaded_files = st.file_uploader(
                    "Drag and Drop files here", 
                    type=["pdf", "docx", "txt", "csv", "xlsx"], 
                    accept_multiple_files=True
                )
                if uploaded_files:
                    for f in uploaded_files:
                        if save_uploaded_file(f):
                            st.toast(f"Saved: {f.name}")
                    st.rerun()

            # 2. List
            stored_files = get_stored_files()
            if not stored_files:
                st.info("No files in WSL storage.")
            else:
                st.write(f"### Stored Files ({len(stored_files)})")
                for f in stored_files:
                    with st.container():
                        c1, c2, c3 = st.columns([6, 1, 1])
                        c1.markdown(f"<div class='file-box'>📄 {f.name}</div>", unsafe_allow_html=True)
                        if c2.button("🗑️", key=f"del_{f.name}"):
                            os.remove(f)
                            logger.info("file_deleted", filename=f.name)
                            st.rerun()
                        if c3.button("🚫", key=f"block_{f.name}"):
                            st.toast(f"{f.name} Blocked")
                
                st.divider()
                
                # 3. Scan All Local
                if st.button("🚀 SCAN ALL STORAGE", type="primary"):
                    st.info("Scanning local storage...")
                    with st.status("Running Engine...") as status:
                        all_results = []
                        for i, file_path in enumerate(stored_files):
                            status.update(label=f"Processing {file_path.name}...", state="running")
                            # --- Local Scan Context Logic ---
                            try:
                                content_str = read_local_file_content(file_path)
                                doc_categories = classification_engine.classify_document_category(content_str)
                                sec_posture = "SAFE"
                                if analytics_engine.check_security_posture(file_path.name, content_str[:100]) == "CRITICAL: Sensitive Column in Plain Text":
                                        sec_posture = "CRITICAL (Unencrypted)"
                                
                                with open(file_path, "rb") as f:
                                    files_payload = {"file": (file_path.name, f)}
                                    res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                                    if res.status_code == 200:
                                        data = res.json()
                                        for finding in data.get("results", []):
                                            if classification_engine.is_false_positive(finding["text"], finding["type"]): continue
                                            sensitivity = classification_engine.classify_sensitivity(finding["type"])
                                            
                                            all_results.append({
                                                "File Name": file_path.name,
                                                "Doc Category": ", ".join(doc_categories) or "General",
                                                "PII Type": finding["type"],
                                                "Kategori UU PDP": sensitivity,
                                                "Detected Data": finding["text"],
                                                "Confidence": finding["score"],
                                                "Postur Keamanan": sec_posture
                                            })
                            except Exception as e:
                                logger.error("scan_error", filename=file_path.name, error=str(e))
                        
                        st.session_state["scan_results"] = all_results
                        status.update(label="✅ Scan Complete!", state="complete", expanded=False)

        # === S3 STORAGE MODE ===
        elif storage_tab == "S3 Object Storage":
            if "s3_config" not in st.session_state:
                st.warning("Please configure S3 connection in 'Configuration' tab first.")
            else:
                cfg = st.session_state["s3_config"]
                
                # Check Connection if not already connected (or just reconnect safely)
                if not s3_connector.s3_client:
                    success, msg = s3_connector.connect(cfg["endpoint"], cfg["access_key"], cfg["secret_key"], cfg["bucket"])
                    if not success:
                        st.error(f"Failed to connect to MinIO: {msg}")
                    else:
                        st.success(f"Connected to Bucket: `{cfg['bucket']}`")
                
                if s3_connector.s3_client:
                    # 1. Upload to S3
                    with st.expander("☁️ Upload Files to S3", expanded=False):
                        s3_uploads = st.file_uploader("Upload to MinIO", accept_multiple_files=True, key="s3_up")
                        if s3_uploads:
                            for f in s3_uploads:
                                if s3_connector.upload_file(f, f.name):
                                    st.toast(f"Uploaded to S3: {f.name}")
                                else:
                                    st.error(f"Failed to upload {f.name}")
                            st.rerun()

                    # 2. List S3 Files
                    s3_files = s3_connector.list_files()
                    if not s3_files:
                        st.info("Bucket is empty.")
                    else:
                        st.write(f"### Objects in `{cfg['bucket']}` ({len(s3_files)})")
                        for obj in s3_files:
                            key = obj['Key']
                            size_kb = obj['Size'] / 1024
                            last_mod = obj['LastModified'].strftime('%Y-%m-%d %H:%M')
                            
                            with st.container():
                                c1, c2, c3 = st.columns([6, 1, 1])
                                c1.markdown(f"<div class='file-box'>☁️ {key} <span style='float:right;color:#888;font-size:0.8em'>{size_kb:.1f}KB | {last_mod}</span></div>", unsafe_allow_html=True)
                                
                                if c2.button("🗑️", key=f"s3_del_{key}"):
                                    s3_connector.delete_file(key)
                                    st.toast(f"Deleted {key}")
                                    st.rerun()
                                    
                        st.divider()
                        
                        # 3. Scan S3
                        if st.button("🚀 SCAN ALL S3 OBJECTS", type="primary"):
                            st.info("Downloading and scanning S3 objects...")
                            with st.status("Processing Cloud Objects...") as status:
                                s3_results = []
                                for i, obj in enumerate(s3_files):
                                    key = obj['Key']
                                    status.update(label=f"Scanning {key}...", state="running")
                                    
                                    findings = process_s3_file(key, headers)
                                    if findings:
                                        s3_results.extend(findings)
                                
                                st.session_state["scan_results"] = s3_results
                                status.update(label="✅ S3 Scan Complete!", state="complete", expanded=False)

        # --- Shared Results View ---
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.markdown("---")
            st.subheader("🚨 Classification Results")
            mask_enabled = st.toggle("🔒 Privacy Mode", value=True)
            df = pd.DataFrame(st.session_state["scan_results"])
            
            if not df.empty:
                display_df = df.copy()
                if mask_enabled:
                    display_df["Detected Data"] = display_df.apply(lambda row: mask_data(row["Detected Data"], row["PII Type"]), axis=1)
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    column_config={
                        "Kategori UU PDP": st.column_config.TextColumn("Kategori UU PDP"),
                        "Detected Data": st.column_config.TextColumn("Data Sample"),
                        "Confidence": st.column_config.ProgressColumn(format="%.2f", min_value=0, max_value=1),
                        "Postur Keamanan": st.column_config.TextColumn("Postur Keamanan")
                    }
                )
                if not st.session_state.get("audit_logged_scan"):
                    src = "S3" if storage_tab == "S3 Object Storage" else "Local"
                    logger.info("scan_viewed", user="admin", source=src, record_count=len(df), timestamp=datetime.now().isoformat())
                    st.session_state["audit_logged_scan"] = True

    # --- Page: Dashboard ---
    elif page == "📊 Dashboard":
        st.title("📊 Security Posture (UU PDP)")
        if "scan_results" not in st.session_state:
            st.info("Run a scan in 'My Files' first.")
        else:
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                m1, m2, m3 = st.columns(3)
                m1.metric("Total Findings", len(df))
                m2.metric("Critical Risk (Spesifik)", len(df[df["Kategori UU PDP"].str.contains("Spesifik", na=False)]))
                m3.metric("System Source", "Hybrid (Local + S3)")
                st.write("### Sensitivity Distribution")
                st.bar_chart(df["Kategori UU PDP"].value_counts())

    # --- Page: Audit Logs ---
    elif page == "📜 Audit Logs":
        st.title("📜 Immutable Audit Logs")
        log_file = LOG_DIR / "audit.log"
        if log_file.exists():
            logs = []
            with open(log_file, "r") as f:
                for line in f:
                    try: logs.append(json.loads(line))
                    except: continue
            st.dataframe(pd.DataFrame(logs).iloc[::-1], use_container_width=True)

    # --- Page: Configuration ---
    elif page == "⚙️ Configuration":
        st.title("⚙️ System Configuration")
        
        st.subheader("☁️ S3 / MinIO Connection")
        with st.form("s3_config_form"):
            c1, c2 = st.columns(2)
            endpoint = c1.text_input("Endpoint URL", value="http://localhost:9000")
            bucket = c2.text_input("Bucket Name", value="pii-data")
            access_key = c1.text_input("Access Key", value="minioadmin")
            secret_key = c2.text_input("Secret Key", type="password", value="minioadmin")
            
            if st.form_submit_button("Save & Connect"):
                st.session_state["s3_config"] = {
                    "endpoint": endpoint,
                    "bucket": bucket,
                    "access_key": access_key,
                    "secret_key": secret_key
                }
                success, msg = s3_connector.connect(endpoint, access_key, secret_key, bucket)
                if success:
                    st.success(f"✅ Connected to {bucket}")
                else:
                    st.error(f"❌ Connection Failed: {msg}")

        st.info("Default MinIO credentials are often 'minioadmin' / 'minioadmin'.")

if __name__ == "__main__":
    main()
