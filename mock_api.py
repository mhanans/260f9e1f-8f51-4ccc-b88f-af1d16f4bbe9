from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
import jwt
import datetime
import structlog
import io
import json # ADDED
from pathlib import Path # ADDED

from engine.classification import classification_engine
from engine.scanner import scanner_engine
from engine.ocr import ocr_engine

# --- LOGGING SETUP ---
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
audit_log_file = LOG_DIR / "audit.log"

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.WriteLoggerFactory(file=open(audit_log_file, "a")), # Ensure append mode
)
# ---------------------

logger = structlog.get_logger()
app = FastAPI()

SECRET_KEY = "supersecretkey"

class TokenRequest(BaseModel):
    username: str
    password: str

class TextScanRequest(BaseModel):
    text: str

@app.post("/api/v1/auth/token")
def login(req: TokenRequest):
    if req.username == "admin@example.com" and req.password == "password":
        token = jwt.encode({
            "sub": req.username,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }, SECRET_KEY, algorithm="HS256")
        
        # LOG LOGIN
        logger.info("user_login", username=req.username, status="success")
        
        return {"access_token": token, "token_type": "bearer"}
    
    # LOG FAILED LOGIN
    logger.warning("user_login", username=req.username, status="failed")
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/api/v1/scan/text")
def scan_text(req: TextScanRequest):
    # Log the scan event
    logger.info("scan_text_request", length=len(req.text))
    pdi_results = scanner_engine.analyze_text(req.text)
    
    # Log results summary
    logger.info("scan_text_complete", findings_count=len(pdi_results))
    
    return {"results": pdi_results}

@app.post("/api/v1/scan/file")
async def scan_file(file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename.lower()
    text = ""

    # Log file start
    logger.info("scan_file_request", filename=filename, size=len(content))

    # 1. Determine Extraction Strategy
    if filename.endswith(".pdf"):
        text = ocr_engine.extract_text_from_pdf(content, filename)
    elif filename.endswith(".docx"):
        text = ocr_engine.extract_text_from_docx(content, filename)
    elif filename.endswith(".jpg") or filename.endswith(".png") or filename.endswith(".jpeg"):
        text = ocr_engine.extract_text_from_image(content, filename)
    else:
        # Fallback text decode
        try:
            text = content.decode("utf-8")
        except:
            text = ""
            logger.warning(f"Could not decode text file {filename}")
    
    # Log extraction results
    logger.info("ocr_extraction_complete", filename=filename, extracted_length=len(text))

    # 2. Scan Extracted Text
    pdi_results = scanner_engine.analyze_text(text)
    
    # Log scan findings
    logger.info("scan_file_complete", filename=filename, findings_count=len(pdi_results))

    return {
        "filename": filename,
        "extracted_text_preview": text[:200], # Return larger preview to UI for debug
        "ocr_log": f"Extracted {len(text)} characters.",
        "results": pdi_results
    }
