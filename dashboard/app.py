import streamlit as st
import requests
import pandas as pd
import io

API_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Data Discovery System", page_icon="🛡️", layout="wide")

# Custom CSS for "Dropbox-like" feel
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .file-card {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin-bottom: 10px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .scan-btn {
        width: 100%;
        margin-top: 20px;
    }
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #eee;
    }
</style>
""", unsafe_allow_html=True)

def login():
    st.sidebar.title("🔐 Login")
    username = st.sidebar.text_input("Email", value="admin@example.com")
    password = st.sidebar.text_input("Password", type="password", value="password")
    if st.sidebar.button("Sign In"):
        try:
            res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
            if res.status_code == 200:
                st.session_state["token"] = res.json()["access_token"]
                st.sidebar.success("Logged in successfully!")
                st.rerun()
            else:
                st.sidebar.error("Invalid credentials")
        except Exception as e:
            st.sidebar.error(f"Connection Error: {e}")

def main():
    if "token" not in st.session_state:
        login()
        st.title("🛡️ Data Discovery System")
        st.info("Please login to access your secure workspace.")
        return

    # Sidebar Navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 My Files (Data Lake)", "📊 Dashboard", "⚙️ Configuration"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    if page == "📂 My Files (Data Lake)":
        st.title("📂 My Files")
        st.caption("Upload files to your secure data lake. Scan them instantly to detect sensitive information.")

        # File Uploader (Dropbox Style)
        uploaded_files = st.file_uploader(
            "Drop files here to upload", 
            type=["pdf", "docx", "txt", "csv", "xlsx"], 
            accept_multiple_files=True
        )

        # Store "files" in a simulated session storage
        if "simulated_storage" not in st.session_state:
            st.session_state["simulated_storage"] = []

        # Update storage with new uploads (simplistic approach for demo)
        if uploaded_files:
            st.session_state["simulated_storage"] = uploaded_files

        files = st.session_state["simulated_storage"]

        if not files:
            st.info("👋 Your data lake is empty. Upload some files to get started!")
            return

        # File List View
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"### 📄 Stored Files ({len(files)})")
        with col2:
            if st.button("🚀 One Click Scan", type="primary"):
                with st.spinner("🕷️ Scanning all files for sensitive data..."):
                    all_results = []
                    progress_bar = st.progress(0)
                    
                    for i, file in enumerate(files):
                        # Reset file pointer
                        file.seek(0)
                        files_payload = {"file": (file.name, file.getvalue())}
                        
                        try:
                            # Send to API
                            res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files_payload)
                            
                            if res.status_code == 200:
                                data = res.json()
                                # Flatten results
                                for finding in data.get("results", []):
                                    all_results.append({
                                        "File Name": file.name,
                                        "PII Type": finding["type"],
                                        "Detected Data": finding["text"], # THE PLAIN TEXT
                                        "Confidence": f"{finding['score']:.2f}",
                                        "Location": f"{finding['start']}-{finding['end']}"
                                    })
                            else:
                                st.error(f"Failed to scan {file.name}: {res.text}")
                        except Exception as e:
                            st.error(f"Error processing {file.name}: {e}")
                        
                        progress_bar.progress((i + 1) / len(files))
                    
                    st.session_state["scan_results"] = all_results
                    st.success("✅ Scan Complete!")

        # Display Files Table (Simulating Dropbox list)
        file_data = [{"File Name": f.name, "Size": f"{f.size / 1024:.1f} KB", "Type": f.type} for f in files]
        st.dataframe(pd.DataFrame(file_data), use_container_width=True)

        # Result Section
        if "scan_results" in st.session_state and st.session_state["scan_results"]:
            st.markdown("---")
            st.subheader("🚨 Scan Results")
            
            results_df = pd.DataFrame(st.session_state["scan_results"])
            
            if not results_df.empty:
                # Summary Metrics
                c1, c2, c3 = st.columns(3)
                c1.metric("Files Scanned", len(files))
                c2.metric("Total PII Found", len(results_df))
                c3.metric("Critical Files", results_df["File Name"].nunique())

                # Detailed Table
                st.dataframe(
                    results_df, 
                    use_container_width=True,
                    column_config={
                        "Detected Data": st.column_config.TextColumn(
                            "Detected Data (Plain Text)",
                            help="The actual sensitive content found",
                            width="medium"
                        ),
                        "PII Type": st.column_config.TextColumn(
                            "PII Type",
                            width="small"
                        ),
                        "Confidence": st.column_config.ProgressColumn(
                            "Confidence",
                            format="%.2f",
                            min_value=0,
                            max_value=1
                        ),
                    }
                )
            else:
                st.success("No PII detected in any of the uploaded files! 🎉")

    elif page == "📊 Dashboard":
        st.title("📊 Security Overview")
        st.markdown("Global view of your data security posture.")
        
        # Mock metrics for demo
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk Score", "Low", delta="-5%")
        c2.metric("Total Files Indexed", "1,240")
        c3.metric("Active Violations", "0")
        
    elif page == "⚙️ Configuration":
        st.title("⚙️ System Configuration")
        st.caption("Manage scanning rules and connections.")
        st.warning("⚠️ No external DB connections configured (Demo Mode).")
        
        st.markdown("### Active Scanners")
        st.checkbox("Presidio (PII)", value=True, disabled=True)
        st.checkbox("Regex (Indonesian ID)", value=True, disabled=True)
        st.checkbox("Encryption Check", value=True, disabled=True)

if __name__ == "__main__":
    main()
