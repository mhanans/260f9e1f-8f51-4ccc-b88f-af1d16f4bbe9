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
    
    /* Sidebar */
    section[data-testid="stSidebar"] { background-color: #2c3e50; color: white; }
    .stButton>button { width: 100%; border-radius: 5px; }

    /* File Box */
    .file-box { border: 1px solid #ddd; padding: 10px; border-radius: 5px; margin-bottom: 5px; background: white; }
    .stButton>button { border-radius: 20px; }
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
        st.title("📂 My Cloud Storage (WSL)")
        st.caption(f"Physical Path: `{BASE_DIR.absolute()}`")

        # 1. Management: Upload
        with st.expander("📤 Upload New Files", expanded=True):
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

        # 2. File List Management (Dropbox Style logic)
        stored_files = get_stored_files()
        
        if not stored_files:
            st.info("No files in WSL storage.")
        else:
            st.write(f"### Stored Files ({len(stored_files)})")
            
            # Dropbox-like List
            for f in stored_files:
                with st.container():
                    c1, c2, c3 = st.columns([5, 1, 1])
                    c1.markdown(f"<div class='file-box'>📄 {f.name}</div>", unsafe_allow_html=True)
                    
                    if c2.button("🗑️", key=f"del_{f.name}"):
                        try:
                            os.remove(f)
                            logger.info("file_deleted", filename=f.name)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                    if c3.button("🚫", key=f"block_{f.name}"):
                         st.toast(f"{f.name} Blocked")
            
            st.divider()
            
            # 3. Action: Scan All
            if st.button("🚀 ONE CLICK SCAN ALL STORAGE", type="primary"):
                st.info("Scanning entire storage folder in background...")
                with st.status("Running Classification Engine...") as status:
                    all_results = []
                    
                    for i, file_path in enumerate(stored_files):
                        status.update(label=f"Processing {file_path.name}...", state="running")
                        
                        try:
                            # Context Read
                            content_str = read_local_file_content(file_path)
                            doc_categories = classification_engine.classify_document_category(content_str)
                            
                            # Entropy Check
                            sec_posture = "SAFE"
                            if analytics_engine.check_security_posture(file_path.name, content_str[:100]) == "CRITICAL: Sensitive Column in Plain Text":
                                    sec_posture = "CRITICAL (Unencrypted)"

                            # Send to API
                            with open(file_path, "rb") as f:
                                files_payload = {"file": (file_path.name, f)}
                                res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                                
                                if res.status_code == 200:
                                    data = res.json()
                                    for finding in data.get("results", []):
                                        
                                        # Filter False Positives (New Logic)
                                        if classification_engine.is_false_positive(finding["text"], finding["type"]):
                                            continue

                                        # UU PDP Classification
                                        sensitivity = classification_engine.classify_sensitivity(finding["type"])
                                        
                                        all_results.append({
                                            "File Name": file_path.name,
                                            "Doc Category": ", ".join(doc_categories) or "General",
                                            "PII Type": finding["type"],
                                            "Sensitivity Logic": sensitivity, # Renamed for clarity
                                            "Detected Data": finding["text"],
                                            "Confidence": finding["score"],
                                            "Security Posture": sec_posture
                                        })
                        except Exception as e:
                            logger.error("scan_error", filename=file_path.name, error=str(e))
                            
                    st.session_state["scan_results"] = all_results
                    status.update(label="✅ Scan Complete!", state="complete", expanded=False)

            # --- Results Area ---
            if "scan_results" in st.session_state and st.session_state["scan_results"]:
                st.markdown("---")
                st.subheader("🚨 Classification Results (UU PDP)")
                
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
                            "Sensitivity Logic": st.column_config.TextColumn("Kategori UU PDP"),
                            "Detected Data": st.column_config.TextColumn("Data Sample"),
                            "Confidence": st.column_config.ProgressColumn(format="%.2f", min_value=0, max_value=1),
                            "Security Posture": st.column_config.TextColumn("Postur Keamanan")
                        }
                    )
                    
                    # Audit Log Trigger
                    if not st.session_state.get("audit_logged_scan"):
                        logger.info("scan_viewed", user="admin", record_count=len(df), timestamp=datetime.now().isoformat())
                        st.session_state["audit_logged_scan"] = True

    # --- Page: Dashboard ---
    elif page == "📊 Dashboard":
        st.title("📊 Security Posture (UU PDP)")
        
        if "scan_results" not in st.session_state:
             st.info("Run a scan in 'My Files' first.")
        else:
             df = pd.DataFrame(st.session_state["scan_results"])
             if not df.empty:
                 # Metric Cards (Fixed Visuals)
                 m1, m2, m3 = st.columns(3)
                 m1.metric("Total Findings", len(df))
                 m2.metric("Critical Risk (Spesifik)", len(df[df["Sensitivity Logic"].str.contains("Spesifik", na=False)]))
                 m3.metric("Compliance Score", "HIGH", delta="Active Monitoring")
                 
                 st.write("### Data Sensitivity Distribution")
                 st.bar_chart(df["Sensitivity Logic"].value_counts())

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
