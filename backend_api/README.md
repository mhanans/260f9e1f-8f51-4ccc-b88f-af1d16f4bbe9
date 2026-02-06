
# Data Discovery Backend API

A high-performance, secure backend service for scanning and classifying PII, designed to be integrated with a .NET application.

## 🚀 One Click Run

### Linux / WSL
```bash
chmod +x one_click.sh
./one_click.sh
```

### Windows
Double-click `run_api.bat` or run in terminal:
```cmd
run_api.bat
```

## 🛠 Setup & Database (Database First)

1.  **Configuration**: Copy `.env.example` to `.env` and update `DATABASE_URL` and `API_KEY`.
2.  **Database Schema**: Execute the SQL scripts in `sql/` against your PostgreSQL database.
    *   `sql/schema.sql`: Creates tables.
    *   `sql/sample_data.sql`: Seeds data (Default Rules + Admin User).

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
    *   **Header**: `X-API-KEY: <your-key>`, `Authorization: Bearer <token>`
    *   **Body**: `{"text": "My NIK is 1234567890123456"}`
    *   **Response**:
        ```json
        {
          "results": [
            {
              "type": "ID_NIK",
              "start": 10,
              "end": 26,
              "score": 0.85,
              "text": "1234567890123456"
            }
          ],
          "is_encrypted": false
        }
        ```

*   `POST /api/v1/scan/file`
    *   **Multipart**: `file` (PDF, DOCX, CSV, TXT)

### Audit & Compliance
*   `GET /api/v1/audit`
*   `GET /api/v1/compliance`

## 🏗 Architecture

*   **Framework**: FastAPI (High Performance)
*   **DB**: PostgreSQL (via SQLModel/SQLAlchemy)
*   **Engine**: Microsoft Presidio + Custom Rules
*   **Task Queue**: Celery + Redis (for Background Scans)
