from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel
from api.db import get_session
from api.deps import get_current_user
from api.models import User, ScanConfig, ScanResult
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
    
    # Use new metadata aware extraction
    chunks = file_scanner.extract_with_metadata(content, file.filename)
    
    all_results = []
    full_text_for_encryption_check = ""
    
    for chunk in chunks:
        text_segment = chunk.get("text", "")
        metadata = chunk.get("metadata", {})
        
        if not text_segment: continue
        
        full_text_for_encryption_check += text_segment + "\n"
        
        # Analyze chunk
        segment_results = scanner_engine.analyze_text(text_segment)
        
        # Enrich with metadata
        for res in segment_results:
            res["location_metadata"] = metadata
            all_results.append(res)

    is_encrypted = analytics_engine.check_encryption(full_text_for_encryption_check)
    
    return {"results": all_results, "filename": file.filename, "is_encrypted": is_encrypted}

@router.post("/similarity")
def check_similarity(request: SimilarityRequest, current_user: User = Depends(get_current_user)):
    score = analytics_engine.calculate_similarity(request.text1, request.text2)
    return {"similarity_score": score}

# --- Connection Manager & History Endpoints ---

class TagUpdate(BaseModel):
    tags: list[str]

@router.post("/config/{config_id}/tags")
def update_config_tags(config_id: int, tags_update: TagUpdate, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    config = session.get(ScanConfig, config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    
    # Simple JSON serialization for tags
    import json
    # Merge or replace? User asked to "add manual tag". Let's assume replace or add unique.
    # Implementation: Append unique
    current_tags = json.loads(config.tags) if config.tags else []
    
    updated = set(current_tags)
    updated.update(tags_update.tags)
    
    config.tags = json.dumps(list(updated))
    session.add(config)
    session.commit()
    session.refresh(config)
    return {"status": "updated", "tags": list(updated)}

@router.get("/config/{config_id}/history")
def get_scan_history(config_id: int, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    # Return history without sample data (as requested)
    statement = select(ScanResult).where(ScanResult.config_id == config_id).order_by(ScanResult.timestamp.desc())
    results = session.exec(statement).all()
    
    # Filter out sample_data in response if strictly required not to show it?
    # The requirement says "not the sample data, just which found...". 
    # Validating if I should unset it or if the frontend just ignores it.
    # Safe to unset.
    clean_results = []
    for r in results:
        # Create a dict copy
        d = r.dict()
        d.pop('sample_data', None)
        clean_results.append(d)
        
    return clean_results
