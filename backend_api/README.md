# Enterprise Data Discovery & Classification System (Privasimu)

## 🚀 Overview
A production-grade, privacy-aware data discovery engine designed for **UU PDP (Indonesian Data Privacy Law)** compliance. It features a **Zero-Hardcode Architecture**, meaning all detection rules, proximity logic, and sensitivity mappings are dynamically loaded from the database, allowing for instant adaptation to new regulations without code changes.

## 🌟 Key Features
*   **4-Phase Discovery**: Dependency Check -> Metadata Risk Profile -> Smart Sampling -> Targeted Full Scan.
*   **Zero-Hardcode Engine**:
    *   **Dynamic Recognizers**: Enable/Disable built-in or custom regex via DB rules.
    *   **Universal Proximity**: Any entity can have context-aware proximity logic configured in DB.
    *   **Smart Filtering**: False positive dictionaries loaded dynamically.
*   **Security & Encryption**:
    *   **Encrypted Connections**: Target database credentials are encrypted at rest (`is_encrypted_path`).
    *   **Strict Masking**: Audit logs and samples are rigorously masked (e.g., `j***@gmail.com`).
*   **Incremental Change Tracking**:
    *   **CDC-Lite**: Detects modified rows in Postgres/S3 and re-scans them.
    *   **Audit Log**: Tracks `DATA_CHANGE` and `METADATA_DRIFT` (Schema changes).
*   **UU PDP Compliance**:
    *   Auto-categorization ("General" vs "Specific").
    *   Indonesian-specific detection (NIK, NPWP, Salaries).

## 🛠 Architecture
*   **Backend**: FastAPI, Celery, SQLModel (Postgres).
*   **Engine**: Microsoft Presidio (Customized), Pandas.
*   **Connectors**: Factory Pattern (Postgres, MySQL, S3, MongoDB).

## 📦 Installation
1.  **Clone & Install**:
    ```bash
    git clone ...
    pip install -r requirements.txt
    ```
2.  **Database Setup**:
    ```bash
    # Run the schema migration
    psql -U user -d privasimu -f sql/schema.sql
    ```
3.  **Run Worker & API**:
    ```bash
    # Terminal 1: API
    uvicorn app.main:app --reload
    
    # Terminal 2: Worker
    celery -A app.scheduler.worker worker --loglevel=info
    ```

## 🔐 Security Configuration
To use encrypted connection strings:
1.  Set `SECRET_KEY` in `.env`.
2.  Encrypt your connection string using `app.core.security.EncryptionUtility`.
3.  Insert into `scanconfig` with `is_encrypted_path=TRUE`.

## � API Documentation

### Scanning & Scheduling
*   `POST /api/v1/scan/config`: Create a scan target.
*   `POST /api/v1/scan/run/{config_id}`: Trigger an immediate scan (Async).
*   `GET /api/v1/scan/status/{task_id}`: Check scan progress.

### Rules & Configuration (Zero-Hardcode)
*   `GET /api/v1/rules`: List all active detection rules.
*   `POST /api/v1/rules`: Add a new Rule (Regex, Proximity, or Disable Built-in).
    *   *Type `DISABLE_DEFAULT`: Remove standard recognizers.*
    *   *Type `false_positive_person`: Add to dynamic dictionary.*

### Reporting & Audit
*   `GET /api/v1/report/summary`: Dashboard overview.
*   `GET /api/v1/report/pii/{config_id}`: Detailed findings (Aggregated).
*   `GET /api/v1/audit`: View Audit Log (Masked changes & Schema Drift).

## 🧪 Testing Zero-Hardcode Logic
1.  **Disable US Passport**:
    ```sql
    INSERT INTO scanrule (name, rule_type, pattern, entity_type) VALUES ('NoUSPass', 'DISABLE_DEFAULT', 'USPassportRecognizer', 'SYSTEM');
    ```
2.  **Add NIK Proximity**:
    ```sql
    INSERT INTO scanrule (name, rule_type, pattern, entity_type, context_keywords) VALUES ('NIK_Context', 'regex', '\\d{16}', 'ID_NIK', '["ktp", "nik", "number"]');
    ```

    *Note: If upgrading, run the following SQL to update `scanconfig`:*
    ```sql
    ALTER TABLE scanconfig ADD COLUMN schedule_cron VARCHAR;
    ALTER TABLE scanconfig ADD COLUMN last_metadata_scan_at TIMESTAMP;
    ALTER TABLE scanconfig ADD COLUMN last_data_scan_at TIMESTAMP;
    ALTER TABLE scanconfig ADD COLUMN scan_scope VARCHAR DEFAULT 'full';
    ```

## 🔒 Security

*   **API Key**: Required in header `X-API-KEY` for ALL requests.
*   **JWT Auth**: Required for protected endpoints (Bearer Token).

## 📖 API Endpoints

Full interactive documentation available at `http://localhost:8000/docs`.

### Authentication
*   `POST /api/v1/auth/token`
    *   **Request**: `username` (email), `password`
    *   **Response**: `{"access_token": "...", "token_type": "bearer"}`

### Scanning
*   `POST /api/v1/scan/text`
    *   **Body**: `{"text": "My NIK is 1234567890123456"}`

*   `POST /api/v1/scan/file`
    *   **Multipart**: `file` (PDF, DOCX, CSV, TXT)

### Metadata Scanning (New)
*   **Preview Metadata**: `POST /api/v1/scan/metadata/preview`
    *   **Body**: `{"target_type": "database", "connection_string": "postgresql://user:pass@host/db"}`
    *   **Response**: Returns list of tables and columns.
*   **Trigger Metadata Scan**: `POST /api/v1/scan/metadata/{config_id}`
    *   **Response**: Triggers background task to update metadata stats.

### Scheduler (New)
*   **Register Schedule**: `POST /api/v1/scheduler/register`
    *   **Body**: `{"config_id": 1, "cron_expression": "*/30 * * * *", "scan_scope": "full"}`
*   **Run Manually**: `POST /api/v1/scheduler/run/{config_id}?scan_scope=metadata`
*   **Remove Schedule**: `POST /api/v1/scheduler/remove/{config_id}`

### Audit & Compliance
*   `GET /api/v1/audit`
*   `GET /api/v1/compliance`

## 🏗 Architecture

*   **Framework**: FastAPI (High Performance)
*   **DB**: PostgreSQL (via SQLModel/SQLAlchemy)
*   **Engine**: Microsoft Presidio + Custom Rules
*   **Task Queue**: Celery + Redis (for Background Scans)
*   **Scheduler**: APScheduler (In-App)
*   **Connectors**: AWS SDK (boto3), PyMongo, MySQL Connector
