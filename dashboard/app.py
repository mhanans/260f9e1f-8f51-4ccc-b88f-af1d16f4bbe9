import streamlit as st
import requests
import pandas as pd

API_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="PII Discovery Dashboard", layout="wide")

def login():
    st.sidebar.title("Login")
    username = st.sidebar.text_input("Email", value="admin@example.com")
    password = st.sidebar.text_input("Password", type="password", value="password")
    if st.sidebar.button("Login"):
        res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
        if res.status_code == 200:
            st.session_state["token"] = res.json()["access_token"]
            st.sidebar.success("Logged in!")
            st.rerun()
        else:
            st.sidebar.error("Invalid credentials")

def main():
    if "token" not in st.session_state:
        login()
        st.title("Welcome to Data Discovery System")
        st.write("Please login to access the dashboard.")
        return

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Dashboard", "Scan File", "Configuration", "Audit Logs"])

    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    if page == "Dashboard":
        st.title("Overview")
        st.metric(label="Total Scans", value="42")
        st.metric(label="PII Detected", value="156", delta="12")
        
    elif page == "Scan File":
        st.title("Scan Document")
        uploaded = st.file_uploader("Upload a file", type=["pdf", "docx", "xlsx", "txt"])
        if uploaded and st.button("Scan Now"):
             files = {"file": (uploaded.name, uploaded.getvalue())}
             res = requests.post(f"{API_URL}/scan/file", headers=headers, files=files)
             if res.status_code == 200:
                 data = res.json()
                 st.write(f"Scanned: {data['filename']}")
                 st.write("Found entities:")
                 df = pd.DataFrame(data["results"])
                 st.dataframe(df)
             else:
                 st.error(f"Error: {res.text}")

    elif page == "Audit Logs":
        st.title("Audit Logs")
        st.write("Coming soon...")

if __name__ == "__main__":
    main()
