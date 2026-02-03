from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlmodel import Session
from pydantic import BaseModel
from api.db import get_session
from api.deps import get_current_user
from api.models import User
from engine.scanner import scanner_engine
from connectors.file_scanner import file_scanner
from engine.analytics import analytics_engine

router = APIRouter()

class ScanRequest(BaseModel):
    text: str

class ScanResponse(BaseModel):
    results: list
    filename: str | None = None
    is_encrypted: bool = False

class SimilarityRequest(BaseModel):
    text1: str
    text2: str

@router.post("/text", response_model=ScanResponse)
def scan_text_direct(request: ScanRequest, current_user: User = Depends(get_current_user)):
    results = scanner_engine.analyze_text(request.text)
    is_encrypted = analytics_engine.check_encryption(request.text)
    return {"results": results, "filename": None, "is_encrypted": is_encrypted}

@router.post("/file", response_model=ScanResponse)
async def scan_file(file: UploadFile = File(...), current_user: User = Depends(get_current_user)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    content = await file.read()
    extracted_text = file_scanner.extract_text(content, file.filename)
    
    if not extracted_text:
        return {"results": [], "filename": file.filename, "is_encrypted": False}

    results = scanner_engine.analyze_text(extracted_text)
    is_encrypted = analytics_engine.check_encryption(extracted_text)
    
    return {"results": results, "filename": file.filename, "is_encrypted": is_encrypted}

@router.post("/similarity")
def check_similarity(request: SimilarityRequest, current_user: User = Depends(get_current_user)):
    score = analytics_engine.calculate_similarity(request.text1, request.text2)
    return {"similarity_score": score}
