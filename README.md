# Data Discovery System

## Quick Start (One Click)

### Windows
Double-click `run_windows.bat` in the file explorer.  
This will automatically:
- Create a virtual environment
- Install all dependencies
- Download required models
- Launch the API backend and Dashboard

### Linux / Mac / WSL
Run the following command:
```bash
bash one_click.sh
```

## Manual Setup
1. **Environment**: Ensure you have Python 3.9+ and Docker installed.
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   python -m spacy download en_core_web_lg
   # For OCR, ensure tesseract is installed on your OS (e.g., `apt-get install tesseract-ocr` or Windows installer)
   ```
3. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```
4. **Seed Database**:
   ```bash
   python scripts/seed_db.py
   ```
5. **Run API**:
   ```bash
   uvicorn main:app --reload
   ```
6. **Run Dashboard**:
   ```bash
   streamlit run dashboard/app.py
   ```

## Features Implemented
- **Hybrid Scanning**: Presidio for PII + Regex for Indonesian ID (NIK, NPWP).
- **OCR Support**: Scan images/scanned PDFs using Tesseract.
- **Big Data Ready**: Connectors for S3 and Hive structure.
- **Audit Logging**: Banking-grade logging via specific Middleware and Database storage.
- **ROPA Integration**: `ProcessingPurpose` model linked to scan configs.
- **Similarity Check**: Cosine similarity using `scikit-learn` explicitly added.

## Folder Structure
- `api/`: FastAPI backend (Routers, Models, Middleware).
- `connectors/`: Logic for reading Files, SQL, and Big Data.
- `engine/`: Presidio Scanner and Analytics (Similarity/Encryption).
- `dashboard/`: Streamlit UI.
- `scheduler/`: Celery worker for background jobs.
