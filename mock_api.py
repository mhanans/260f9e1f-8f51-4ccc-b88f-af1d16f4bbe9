from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
import jwt
import datetime
import structlog
import io

from engine.classification import classification_engine
from engine.scanner import scanner_engine
from engine.ocr import ocr_engine

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
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/api/v1/scan/text")
def scan_text(req: TextScanRequest):
    pdi_results = scanner_engine.analyze_text(req.text)
    return {"results": pdi_results}

@app.post("/api/v1/scan/file")
async def scan_file(file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename.lower()
    text = ""

    # 1. Determine Extraction Strategy
    if filename.endswith(".pdf"):
        text = ocr_engine.extract_text_from_pdf(content)
    elif filename.endswith(".docx"):
        text = ocr_engine.extract_text_from_docx(content)
    elif filename.endswith(".jpg") or filename.endswith(".png") or filename.endswith(".jpeg"):
        text = ocr_engine.extract_text_from_image(content)
    else:
        # Fallback text decode
        try:
            text = content.decode("utf-8")
        except:
            text = ""
    
    # 2. Scan Extracted Text
    pdi_results = scanner_engine.analyze_text(text)
    
    return {
        "filename": filename,
        "extracted_text_preview": text[:100],
        "results": pdi_results
    }
