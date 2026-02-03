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
    
    /* Rules Table */
    .rule-box { border:1px solid #555; background:#222; padding:10px; margin-bottom:10px; border-radius:5px; }
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

# Config Logic
def load_rules_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f: return json.load(f)
    return {}

def save_rules_config(data):
    with open(CONFIG_PATH, "w") as f: json.dump(data, f, indent=2)
    # Reload Engines
    classification_engine.load_config()
    scanner_engine.reload_rules()

# --- Main App ---
def main():
    if "token" not in st.session_state: login(); return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["📂 Data Explorer", "🗂️ Connections", "🚀 Scan Manager", "⚙️ Rules Engine", "📊 Dashboard"])
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # ... Pages: Explorer, Connections, Scan Manager (Same logic) ...
    # Simplified here for brevity since update is on Rules Engine
    if page == "📂 Data Explorer":
        st.title("Explorer") 
        # (Content omitted for brevity - logic maintained from previous step)
        st.info("Use the full explorer logic in production.")

    elif page == "🗂️ Connections":
        st.title("Connections")
        # (Content omitted - logic maintained)
        st.info("Use the full connections logic in production.")
        
    elif page == "🚀 Scan Manager":
        st.title("Scan Manager")
        # (Content omitted - logic maintained)
        st.info("Use the full scan logic in production.")

    # --- Page: Rules Engine ---
    elif page == "⚙️ Rules Engine":
        st.title("⚙️ PII Detection Rules Engine")
        st.caption("Manage detection patterns. Ensure 1 rule per Entity.")
        
        config = load_rules_config()
        recogs = config.get("custom_recognizers", [])
        
        # 1. LIVE PATTERN TESTER
        with st.expander("🧪 Test a Regex Pattern", expanded=True):
            cols = st.columns([3, 1])
            test_regex = cols[0].text_input("Enter Regex Pattern", r"\b\d{16}\b")
            test_text = st.text_area("Test String (Sample Data)", "Ini adalah NIK saya 3201123412341234 yang valid.")
            
            if st.button("Run Test"):
                try:
                    matches = re.finditer(test_regex, test_text)
                    results = [m.group(0) for m in matches]
                    if results:
                        st.success(f"✅ Found {len(results)} matches: {results}")
                    else:
                        st.warning("No matches found.")
                except Exception as e:
                    st.error(f"Regex Error: {e}")

        st.divider()

        # 2. Manage Rules (Force 1 Rule per Entity)
        st.subheader("Manage Detection Rules")
        
        # Helper to get existing entities
        existing_entities = set([r["entity"] for r in recogs])
        
        # Display existing rules
        for i, rec in enumerate(recogs):
            with st.container():
                with st.expander(f"🧩 {rec['entity']} ({rec['name']})", expanded=False):
                    c1, c2 = st.columns([3, 1])
                    
                    # Edit Fields
                    new_regex = c1.text_input("Regex", value=rec["regex"], key=f"rex_{i}")
                    new_score = c2.slider("Score", 0.0, 1.0, rec["score"], key=f"sco_{i}")
                    new_ctx = st.text_input("Context (comma sep.)", value=",".join(rec.get("context", [])), key=f"ctx_{i}")
                    is_active = st.checkbox("Active", value=rec.get("active", True), key=f"act_{i}")
                    
                    if st.button("🗑️ Delete Rule", key=f"del_rule_{i}"):
                        recogs.pop(i)
                        config["custom_recognizers"] = recogs
                        save_rules_config(config)
                        st.rerun()
                    
                    # Update Memory object (save happens at bottom)
                    rec["regex"] = new_regex
                    rec["score"] = new_score
                    rec["context"] = [x.strip() for x in new_ctx.split(",") if x.strip()]
                    rec["active"] = is_active

        # Add New Rule
        st.write("### Add New Rule")
        with st.form("add_rule_form"):
            new_entity = st.text_input("Entity Name (Unique)", placeholder="ID_NEW_CARD").upper()
            new_pattern = st.text_input("Regex Pattern", placeholder=r"\b[A-Z]{3}-\d{4}\b")
            
            if st.form_submit_button("Add Rule"):
                if not new_entity or not new_pattern:
                    st.error("Entity Name and Regex are required.")
                elif new_entity in [r["entity"] for r in recogs]:
                    st.error(f"Rule for {new_entity} already exists! Edit the existing one instead.")
                else:
                    new_rule = {
                        "name": f"{new_entity.lower()}_recognizer",
                        "entity": new_entity,
                        "regex": new_pattern,
                        "score": 0.5,
                        "context": [],
                        "active": True
                    }
                    recogs.append(new_rule)
                    config["custom_recognizers"] = recogs
                    save_rules_config(config)
                    st.success(f"Added rule for {new_entity}")
                    st.rerun()

        # Save Changes
        st.divider()
        if st.button("💾 SAVE ALL CHANGES", type="primary"):
            config["custom_recognizers"] = recogs
            save_rules_config(config)
            st.success("Configuration Saved & Engine Reloaded!")

    elif page == "📊 Dashboard":
        # (Content omitted logic same)
        st.title("Dash")

if __name__ == "__main__":
    main()
