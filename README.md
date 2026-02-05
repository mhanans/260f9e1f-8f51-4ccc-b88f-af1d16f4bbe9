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

## Detailed Capabilities & Architecture

### Application Capabilities
1.  **Unified Data Discovery Engine**: 
    -   **Multi-Source Support**: Scans local files (`.txt`, `.csv`, `.docx`, `.pdf`, `.xlsx`), SQL Databases (via **PostgreSQL** protocol), and Object Storage (**S3/MinIO**).
    -   **Hybrid Scanning Intelligence**: Merges **NLP-based detection** (via Microsoft Presidio + Spacy) for generic entities (Person, Location) with **High-Precision Regex** for Indonesian-specific contexts (NIK, NPWP, CIF, Phone Numbers).
    -   **Context-Aware Analysis**: Utilizes "Context Words" (e.g., "cust_id", "mobile", "perjanjian") around a match to boost confidence scores, significantly reducing false positives.

2.  **Granular Object-Level Reporting**:
    -   Unlike generic scanners that just say "File X has PII", this system pinpoints the **exact location**:
        -   **PDFs**: Reports finding + **Page Number**.
        -   **Excel**: Reports finding + **Sheet Name** + **Row Number**.
        -   **Databases**: Reports schema, table, and specific column.
    -   **Metadata Enrichment**: Every finding is enriched with its source location metadata before aggregation.

3.  **Optical Character Recognition (OCR) Pipeline**:
    -   **Intelligent Pre-processing**: Images are converted to Grayscale and Contrast-Enhanced (2.0x factor) to improve accuracy before being fed to Tesseract.
    -   **Multi-Format Support**: Handles raw images (`.png`, `.jpg`), **Scanned PDFs** (via `pdf2image`), and even **Embedded Images** inside `.docx` files.
    -   **Dual-Language Support**: Configured for both `ind` (Indonesian) and `eng` (English) to handle mixed-language documents.

4.  **Advanced Analytics & Validation**:
    -   **Encryption Detection**: Uses **Shannon Entropy** analysis to flag files that appear to be encrypted/compressed (high entropy) where PII scanning might fail.
    -   **Similarity Analysis**: Implements **Cosine Similarity** (via `scikit-learn`) to detect duplicate or near-duplicate sensitive documents.
    -   **Dynamic Rule Management**: Allows admins to update regex patterns, deny-lists, and context rules via the database without restarting the engine.

### Technology Stack & Implementation Logic

| Library | Function | Logic & Implementation Details |
| :--- | :--- | :--- |
| **Microsoft Presidio** (`presidio-analyzer`) | **Core PII Engine** | Instantiated as `AnalyzerEngine`. Configured with a filtered registry (US-specific noise removed). Extended with `PatternRecognizer` for `ID_CIF`, `ID_PHONE_IDN`, and dynamic `ScanRule` loading from Postgres. Uses **spaCy** (`en_core_web_lg`) for NER. |
| **PyMuPDF** (`fitz`) | **PDF Extraction** | Used for *searchable* PDFs. Iterates through the document structure `doc.pages()` to extract text while explicitly preserving the `page_number` for the final report. |
| **pdf2image** + **Tesseract** | **OCR Scanner** | Used for *non-searchable* PDFs. Converts PDF pages to 300 DPI images (`convert_from_bytes`), applies image pre-processing (`ImageEnhance.Contrast`), and runs `pytesseract.image_to_string` with `lang='ind+eng'`. |
| **OpenPyXL** | **Excel Parsing** | Reads `.xlsx` files in `read-only` mode. Iterates `workbook.sheetnames` -> `sheet.iter_rows()` to capture text while maintaining `sheet_name` and `row_index` context. |
| **python-docx** | **Word Processing** | extract text from paragraphs. Also iterates `doc.part.rels` to find and extract **embedded images** (media), sending them to the OCR engine separately. |
| **SQLAlchemy** / **Psycopg2** | **Direct DB Scan** | Connects to PostgreSQL. Performs **Schema Crawling** (`information_schema`) to map all tables/columns. Implements **Delta Scanning** logic by checking for `updated_at` timestamps to scan only new/modified rows. |
| **Scikit-Learn** | **Similarity** | Vectors text inputs (TF-IDF or CountVector) to calculate Cosine Similarity scores, identifying redundant data. |
| **Celery** | **Async Scheduler** | Offloads heavy scanning tasks (e.g., 50MB+ PDFs) to background workers. Uses Redis/RabbitMQ (implied) as broker. |
| **FastAPI** | **API & Middleware** | Provides high-performance Async endpoints. Includes Custom **Audit Middleware** that logs every request/response to the database for compliance (Action Log). |

## Folder Structure
- `api/`: FastAPI backend (Routers, Models, Middleware).
- `connectors/`: Logic for reading Files, SQL, and Big Data.
- `engine/`: Presidio Scanner and Analytics (Similarity/Encryption).
- `dashboard/`: Streamlit UI.
- `scheduler/`: Celery worker for background jobs.
