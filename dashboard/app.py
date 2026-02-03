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

# --- Internal Engines (Simulating direct access for demo speed, usually via API) ---
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

# --- Custom CSS ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    [data-testid="stMetricValue"] { color: #1f77b4 !important; }
    [data-testid="stMetricLabel"] { color: #333333 !important; font-weight: bold; }
    .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-right: 4px; color: white; }
    .tag-sensitive { background-color: #d32f2f; }
    .tag-general { background-color: #388e3c; }
    .tag-cat { background-color: #1976d2; }
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

def mask_data(text, visible_chars=2):
    if not text or len(text) <= visible_chars * 2: return text
    return f"{text[:visible_chars]}{'*' * (len(text) - visible_chars*2)}{text[-visible_chars:]}"

def read_audit_logs():
    log_file = LOG_DIR / "audit.log"
    if not log_file.exists(): return []
    logs = []
    with open(log_file, "r") as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except: continue
    return logs

# --- Main App ---
def main():
    if "token" not in st.session_state:
        login()
        st.title("🛡️ Data Discovery System")
        st.info("Please login to access your secure workspace.")
        return

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 My Files (Data Lake)", "📊 Dashboard", "📜 Audit Logs", "⚙️ Configuration"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # --- Page: My Files ---
    if page == "📂 My Files (Data Lake)":
        st.title("📂 My Files (Local Data Lake)")
        
        # Upload
        with st.expander("📤 Upload New Files", expanded=True):
            uploaded_files = st.file_uploader(
                "Drop files to save to local storage", 
                type=["pdf", "docx", "txt", "csv", "xlsx"], 
                accept_multiple_files=True
            )
            if uploaded_files:
                for f in uploaded_files:
                    save_uploaded_file(f)
                st.rerun()

        stored_files = get_stored_files()
        
        if stored_files:
            col1, col2 = st.columns([3, 1])
            with col1: st.subheader(f"📄 Stored Files ({len(stored_files)})")
            with col2:
                if st.button("🚀 Scan All Files (Deep Scan)", type="primary"):
                    with st.status("🕷️ Classification Engine Running...") as status:
                        all_results = []
                        for i, file_path in enumerate(stored_files):
                            status.update(label=f"Scanning & Classifying: {file_path.name}...", state="running")
                            
                            try:
                                with open(file_path, "rb") as f:
                                    # 1. Read File Content (simplified manual read for context extraction)
                                    # (Ideally the backend API handles this, but for demo logical visualization we do it here)
                                    content_bytes = f.read()
                                    content_str = content_bytes.decode('utf-8', errors='ignore') # Simple decode for context
                                    
                                    # 2. Determine Document Category (Automated Labeling)
                                    doc_categories = classification_engine.classify_document_category(content_str)
                                    
                                    # 3. Security Posture Check (Simulated)
                                    sec_status = "SAFE"
                                    if "password" in file_path.name.lower():
                                        sec_status = analytics_engine.check_security_posture(file_path.name, content_str[:50])

                                    f.seek(0) # Reset for API
                                    files_payload = {"file": (file_path.name, f)}
                                    res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                                    
                                    if res.status_code == 200:
                                        data = res.json()
                                        for finding in data.get("results", []):
                                            # 4. Sensitivity Classification
                                            sensitivity = classification_engine.classify_sensitivity(finding["type"])
                                            
                                            all_results.append({
                                                "File Name": file_path.name,
                                                "Categories": ", ".join(doc_categories) if doc_categories else "Uncategorized",
                                                "PII Type": finding["type"],
                                                "Sensitivity": sensitivity,
                                                "Detected Data": finding["text"],
                                                "Confidence": finding["score"],
                                                "Security Posture": sec_status
                                            })
                            except Exception as e:
                                logger.error("scan_error", filename=file_path.name, error=str(e))
                        
                        st.session_state["scan_results"] = all_results
                        logger.info("scan_completed", total_files=len(stored_files), findings=len(all_results))
                        status.update(label="✅ Analysis Complete!", state="complete", expanded=False)

            # --- Results Area ---
            if "scan_results" in st.session_state and st.session_state["scan_results"]:
                st.markdown("---")
                st.subheader("🚨 Analysis Results & Classification")
                
                mask_enabled = st.toggle("🔒 Privacy Mode (Masking ON)", value=True)
                
                df = pd.DataFrame(st.session_state["scan_results"])
                
                if not df.empty:
                    display_df = df.copy()
                    if mask_enabled:
                        display_df["Detected Data"] = display_df["Detected Data"].apply(mask_data)
                    
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        column_config={
                            "Sensitivity": st.column_config.TextColumn(
                                "Sensitivity Level", 
                                help="Specific/Sensitive vs General"
                            ),
                            "Categories": st.column_config.ListColumn("Auto-Labels"),
                            "Confidence": st.column_config.ProgressColumn(format="%.2f", min_value=0, max_value=1),
                            "Security Posture": st.column_config.TextColumn("Security Check")
                        }
                    )
                    
                    # Graph / Lineage Placeholder
                    st.write("### 🧬 Data Lineage (Traceability)")
                    dot_source = 'digraph lineage {\n'
                    dot_source += '  rankdir=LR;\n'
                    dot_source += '  node [shape=box, style=filled, fontname="Helvetica"];\n'
                    for idx, row in df.head(5).iterrows():
                        fname = row["File Name"]
                        pii = row["PII Type"]
                        cat = row["Categories"]
                        dot_source += f'  "{fname}" [fillcolor="#E3F2FD"];\n'
                        dot_source += f'  "{pii}" [shape=ellipse, fillcolor="#FFEBEE"];\n'
                        dot_source += f'  "{cat}" [shape=note, fillcolor="#E8F5E9"];\n'
                        dot_source += f'  "{fname}" -> "{pii}" [label="contains"];\n'
                        dot_source += f'  "{pii}" -> "{cat}" [label="classified as"];\n'
                    dot_source += '}'
                    st.graphviz_chart(dot_source)

    # --- Page: Dashboard ---
    elif page == "📊 Dashboard":
        st.title("📊 Security & Analytics Dashboard")
        
        # Metrics
        stored_count = len(get_stored_files())
        findings_count = len(st.session_state.get("scan_results", []))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk Score", "Medium" if findings_count > 0 else "Low")
        c2.metric("Total Files", f"{stored_count}")
        c3.metric("Total Findings", f"{findings_count}")
        
        if "scan_results" in st.session_state:
            df = pd.DataFrame(st.session_state["scan_results"])
            if not df.empty:
                col_a, col_b = st.columns(2)
                
                with col_a:
                    st.subheader("Sensitivity Distribution")
                    # Pie Chart Logic
                    sensitivity_counts = df["Sensitivity"].value_counts()
                    st.markdown("**Sensitive vs General Data**")
                    # Using simple bar char for robustness if matplotlib issues, or st.bar_chart
                    st.bar_chart(sensitivity_counts)
                
                with col_b:
                    st.subheader("Category Distribution")
                    cat_counts = df["Categories"].value_counts()
                    st.bar_chart(cat_counts)

    # --- Page: Audit Logs ---
    elif page == "📜 Audit Logs":
        st.title("📜 Immutable Audit Logs")
        st.caption("Tracking all system events: User Login, File Uploads, Scans.")
        
        logs = read_audit_logs()
        if logs:
            log_df = pd.DataFrame(logs)
            st.dataframe(
                log_df, 
                use_container_width=True, 
                column_config={
                    "timestamp": st.column_config.DatetimeColumn("Timestamp", format="D MMM YYYY, h:mm a")
                }
            )
        else:
            st.info("No logs found.")

    # --- Page: Configuration ---
    elif page == "⚙️ Configuration":
        st.title("⚙️ System Configuration")
        st.subheader("� Data Connectors & Rules")
        
        tab1, tab2 = st.tabs(["Connectors", "Classification Rules"])
        
        with tab1:
            st.write("External Systems:")
            st.checkbox("Oracle DB (JDBC)", disabled=True, value=True, help="Connected via JDBC Driver")
            st.checkbox("Hadoop/BigData", disabled=True)
            st.text_input("Crawler Schedule (Cron)", value="0 2 * * 0", help="Run scan every Sunday at 02:00 AM")
            
        with tab2:
            st.write("Custom Classification Logic:")
            with st.form("new_rule"):
                col1, col2 = st.columns(2)
                with col1: st.text_input("New Category Name")
                with col2: st.text_input("Keywords (comma separated)")
                st.form_submit_button("Add Rule")

if __name__ == "__main__":
    main()
