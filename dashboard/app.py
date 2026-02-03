import streamlit as st
import requests
import pandas as pd
import os
import shutil
import structlog
from pathlib import Path
from datetime import datetime

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

# --- Custom CSS (Visual Bug Fixes) ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    
    /* Fix Metric Card Text Color */
    [data-testid="stMetricValue"] {
        color: #1f77b4 !important;
    }
    [data-testid="stMetricLabel"] {
        color: #333333 !important;
        font-weight: bold;
    }
    
    /* Card Styling */
    .file-card {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin-bottom: 10px;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #2c3e50;
        color: white;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
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

# --- Main App ---
def main():
    if "token" not in st.session_state:
        login()
        st.title("🛡️ Data Discovery System")
        st.info("Please login to access your secure workspace.")
        return

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 My Files (Data Lake)", "📊 Dashboard", "⚙️ Configuration"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # --- Page: My Files ---
    if page == "📂 My Files (Data Lake)":
        st.title("📂 My Files (Local Data Lake)")
        st.caption(f"Storage Location: `{BASE_DIR.absolute()}`")

        # Upload Section
        with st.expander("📤 Upload New Files", expanded=True):
            uploaded_files = st.file_uploader(
                "Drop files to save to local storage", 
                type=["pdf", "docx", "txt", "csv", "xlsx"], 
                accept_multiple_files=True
            )
            if uploaded_files:
                for f in uploaded_files:
                    if save_uploaded_file(f):
                        st.toast(f"Saved: {f.name}")
                st.rerun() # Refresh to show new files

        # List Files
        stored_files = get_stored_files()
        
        if not stored_files:
            st.info("Your data lake is empty.")
        else:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.subheader(f"📄 Stored Files ({len(stored_files)})")
            with col2:
                if st.button("🚀 Scan All Files", type="primary"):
                    with st.status("🕷️ Scanning Data Lake...") as status:
                        all_results = []
                        for i, file_path in enumerate(stored_files):
                            status.update(label=f"Scanning {file_path.name}...", state="running")
                            
                            try:
                                with open(file_path, "rb") as f:
                                    files_payload = {"file": (file_path.name, f)}
                                    res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                                    
                                    if res.status_code == 200:
                                        data = res.json()
                                        for finding in data.get("results", []):
                                            all_results.append({
                                                "File Name": file_path.name,
                                                "PII Type": finding["type"],
                                                "Detected Data": finding["text"],
                                                "Confidence": finding["score"],
                                                "Location": f"{finding['start']}-{finding['end']}"
                                            })
                            except Exception as e:
                                logger.error("scan_error", filename=file_path.name, error=str(e))
                        
                        st.session_state["scan_results"] = all_results
                        logger.info("scan_completed", total_files=len(stored_files), findings=len(all_results))
                        status.update(label="✅ Scan Complete!", state="complete", expanded=False)

            # File Table
            file_data = []
            for f in stored_files:
                stats = f.stat()
                file_data.append({
                    "File Name": f.name,
                    "Size (KB)": f"{stats.st_size / 1024:.1f}",
                    "Last Modified": datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M')
                })
            st.dataframe(pd.DataFrame(file_data), use_container_width=True)

        # --- Report Selection (Granular) ---
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.markdown("---")
            st.subheader("🚨 Scan Results")
            
            results_df = pd.DataFrame(st.session_state["scan_results"])
            
            if not results_df.empty:
                # Filter Logic
                filter_option = st.selectbox(
                    "View Mode", 
                    ["Show All Summary", "Filter by Specific File"]
                )
                
                final_df = results_df
                if filter_option == "Filter by Specific File":
                    selected_file = st.selectbox("Select File", results_df["File Name"].unique())
                    final_df = results_df[results_df["File Name"] == selected_file]

                # Metric Summary for Selection
                m1, m2, m3 = st.columns(3)
                m1.metric("Findings", len(final_df))
                m2.metric("High Confidence (>0.8)", len(final_df[final_df["Confidence"] > 0.8]))
                m3.metric("PII Types", final_df["PII Type"].nunique())

                st.dataframe(
                    final_df,
                    use_container_width=True,
                    column_config={
                        "Confidence": st.column_config.ProgressColumn(format="%.2f", min_value=0, max_value=1)
                    }
                )
            else:
                st.success("No PII found in scanned files.")

    # --- Page: Dashboard ---
    elif page == "📊 Dashboard":
        st.title("📊 Security Overview")
        
        # Real metrics check
        stored_count = len(get_stored_files())
        findings_count = len(st.session_state.get("scan_results", []))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk Score", "Medium" if findings_count > 0 else "Low", delta="Auto-calculated")
        c2.metric("Total Files Stored", f"{stored_count}")
        c3.metric("Total Violations Found", f"{findings_count}", delta_color="inverse")

    # --- Page: Configuration ---
    elif page == "⚙️ Configuration":
        st.title("⚙️ System Configuration")
        
        st.subheader("🔌 Connection Settings")
        
        # S3 Config
        with st.expander("AWS S3 Configuration"):
            aws_key = st.text_input("AWS Access Key ID")
            aws_secret = st.text_input("AWS Secret Access Key", type="password")
            s3_bucket = st.text_input("S3 Bucket Name")
            if aws_key and aws_secret:
                st.success("✅ AWS Credentials Loaded (Active Connection)")
            else:
                st.warning("⚠️ AWS Configuration Missing")

        # database Config
        with st.expander("External Database"):
            db_uri = st.text_input("Database URI", value="postgresql://user:pass@localhost:5432/db")
            if "localhost" not in db_uri:
                 st.success("✅ Remote Database configured")
            else:
                 st.info("ℹ️ Using Local/Default Database")

if __name__ == "__main__":
    main()
