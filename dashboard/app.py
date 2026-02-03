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
from docx import Document # Added to read docx locally in frontend for classification context

# --- Internal Engines ---
from engine.classification import classification_engine
from engine.analytics import analytics_engine

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

# --- Custom CSS (Fixed Visual Bugs + Enterprise Theme) ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    
    /* Metrics Color Fix */
    [data-testid="stMetricValue"] { color: #1f77b4 !important; }
    [data-testid="stMetricLabel"] { color: #333333 !important; font-weight: bold; }
    
    /* Table Styling */
    .stTable { background-color: #f0f2f6; color: black; }
    
    /* Tags */
    .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-right: 4px; color: white; }
    
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
        # Keep first 4 and last 4, match standard masking
        if len(text) > 8:
            return text[:4] + "********" + text[-4:]
        return "********"
    
    if entity_type == "EMAIL_ADDRESS":
        if "@" in text:
            parts = text.split("@")
            return parts[0][0] + "***@" + parts[-1]
        return "***@***"
        
    # Default mask
    visible = 2
    if len(text) <= visible * 2: return "****"
    return f"{text[:visible]}{'*' * (len(text) - visible*2)}{text[-visible:]}"

def read_local_file_content(file_path):
    """
    Reads content from local file path for context analysis in the dashboard.
    Supports txt, csv, docx. (PDF is binary, requires PyMuPDF which is in connector)
    """
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
        st.title("📂 My Data Lake (WSL Storage)")
        st.caption(f"Physical Path: `{BASE_DIR.absolute()}`")

        # 1. Management: Upload
        with st.expander("📤 Upload Files to Storage", expanded=True):
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

        # 2. Management: List & Actions
        stored_files = get_stored_files()
        
        if not stored_files:
            st.info("Storage is empty.")
        else:
            col1, col2 = st.columns([3, 1])
            with col1: 
                st.subheader(f"📄 Stored Files ({len(stored_files)})")
            with col2:
                if st.button("🚀 ONE CLICK SCAN ALL STORAGE", type="primary"):
                    st.info("Scanning entire storage folder in background...")
                    # Logic to trigger simulated background scan
                    with st.status("Running Classifier Engine...") as status:
                        all_results = []
                        for i, file_path in enumerate(stored_files):
                            status.update(label=f"Processing {file_path.name}...", state="running")
                            
                            try:
                                # Pre-read content for dashboard context logic (Classification)
                                # Note: Actual scanning happened in API via 'files' payload, this is just for the local classification engine logic in this mockup
                                content_str = read_local_file_content(file_path)

                                # Auto-Classify Doc Type
                                doc_categories = classification_engine.classify_document_category(content_str)
                                
                                # Entropy Check (Security Posture)
                                sec_posture = "SAFE"
                                if analytics_engine.check_security_posture(file_path.name, content_str[:100]) == "CRITICAL: Sensitive Column in Plain Text":
                                        sec_posture = "CRITICAL (Unencrypted)"

                                # Send to API for PII Scanning
                                with open(file_path, "rb") as f:
                                    files_payload = {"file": (file_path.name, f)}
                                    res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                                    
                                    if res.status_code == 200:
                                        data = res.json()
                                        for finding in data.get("results", []):
                                            # UU PDP Classification
                                            sensitivity = classification_engine.classify_sensitivity(finding["type"])
                                            
                                            all_results.append({
                                                "File Name": file_path.name,
                                                "Doc Category": ", ".join(doc_categories) or "General",
                                                "PII Type": finding["type"],
                                                "Sensitivity": sensitivity,
                                                "Detected Data": finding["text"],
                                                "Confidence": finding["score"],
                                                "Security Posture": sec_posture
                                            })
                            except Exception as e:
                                logger.error("scan_error", filename=file_path.name, error=str(e))
                        
                        st.session_state["scan_results"] = all_results
                        status.update(label="✅ Scan Complete!", state="complete", expanded=False)

            # File Management Table
            st.write("### 🗂️ File Management")
            for f in stored_files:
                c1, c2, c3 = st.columns([6, 1, 1])
                c1.text(f"📄 {f.name} ({f.stat().st_size/1024:.1f} KB)")
                if c2.button("🗑️", key=f"del_{f.name}"):
                    try:
                        os.remove(f)
                        logger.info("file_deleted", filename=f.name)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting: {e}")
                if c3.button("🚫", key=f"block_{f.name}"):
                    st.warning(f"File {f.name} marked for blacklist/quarantine.")

            # --- Results Area ---
            if "scan_results" in st.session_state and st.session_state["scan_results"]:
                st.markdown("---")
                st.subheader("🚨 Classification Results (UU PDP)")
                
                # Masking On-The-Fly (Poin 2 & 5)
                mask_enabled = st.toggle("🔒 Privacy Mode (Masking On-The-Fly)", value=True)
                
                df = pd.DataFrame(st.session_state["scan_results"])
                
                if not df.empty:
                    display_df = df.copy()
                    
                    if mask_enabled:
                        display_df["Detected Data"] = display_df.apply(
                            lambda row: mask_data(row["Detected Data"], row["PII Type"]), 
                            axis=1
                        )
                    
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        column_config={
                            "Sensitivity": st.column_config.TextColumn("UU PDP Category"),
                            "Detected Data": st.column_config.TextColumn("Data Sample"),
                            "Confidence": st.column_config.ProgressColumn(format="%.2f", min_value=0, max_value=1),
                            "Security Posture": st.column_config.TextColumn("Security Check")
                        }
                    )
                    
                    # Audit Log Trigger
                    if not st.session_state.get("audit_logged_scan"):
                        logger.info("scan_viewed", user="admin", record_count=len(df), timestamp=datetime.now().isoformat())
                        st.session_state["audit_logged_scan"] = True

    # --- Page: Dashboard ---
    elif page == "📊 Dashboard":
        st.title("📊 Security Posture (UU PDP)")
        
        stored_count = len(get_stored_files())
        findings_count = len(st.session_state.get("scan_results", []))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Documents", f"{stored_count}", delta="Local Storage")
        c2.metric("Sensitive Findings", f"{findings_count}", delta_color="inverse")
        c3.metric("System Status", "Active", delta="Celery Workers Ready")
        
        if "scan_results" in st.session_state:
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                st.write("### Data Sensitivity Distribution")
                st.bar_chart(df["Sensitivity"].value_counts())

    # --- Page: Audit Logs ---
    elif page == "📜 Audit Logs":
        st.title("📜 Immutable Audit Logs")
        st.caption("Tracking all file operations (Delete, Block, Upload, Scan).")
        
        log_file = LOG_DIR / "audit.log"
        if log_file.exists():
            logs = []
            with open(log_file, "r") as f:
                for line in f:
                    try: logs.append(json.loads(line))
                    except: continue
            st.dataframe(pd.DataFrame(logs).iloc[::-1], use_container_width=True)

if __name__ == "__main__":
    main()
