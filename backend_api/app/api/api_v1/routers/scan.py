
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel
from typing import List, Optional

from app.core.db import get_session
from app.api.deps import get_current_user
from app.models.all_models import User, ScanConfig, ScanResult
from app.engine.scanner import scanner_engine
from app.connectors.file_scanner import file_scanner
# from app.engine.analytics import analytics_engine # Migrating internal analytics logic if needed or ensuring import exists

router = APIRouter()

class ScanRequest(BaseModel):
    text: str

class ScanResponse(BaseModel):
    results: list
    filename: Optional[str] = None
    is_encrypted: bool = False

class SimilarityRequest(BaseModel):
    text1: str
    text2: str

# Mocking analytics_engine for now until fully migrated, or assuming it will be there.
# I will need to verify if analytics.py exists in source.
# Checked list_dir of engine, it exists. I'll rely on it being migrated.
from app.engine.analytics import analytics_engine

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

class TagUpdate(BaseModel):
    tags: List[str]

@router.post("/config/{config_id}/tags")
def update_config_tags(config_id: int, tags_update: TagUpdate, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    config = session.get(ScanConfig, config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    
    import json
    # Use generic list for JSON handling if needed, or simple string split
    try:
        current_tags = json.loads(config.tags) if config.tags else []
    except:
        current_tags = []
    
    updated = set(current_tags)
    updated.update(tags_update.tags)
    
    config.tags = json.dumps(list(updated))
    session.add(config)
    session.commit()
    session.refresh(config)
    return {"status": "updated", "tags": list(updated)}

@router.get("/config/{config_id}/history")
def get_scan_history(config_id: int, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    statement = select(ScanResult).where(ScanResult.config_id == config_id).order_by(ScanResult.timestamp.desc())
    results = session.exec(statement).all()
    
    clean_results = []
    for r in results:
        d = r.dict()
        d.pop('sample_data', None)
        clean_results.append(d)
        
    return clean_results

# --- Metadata Scanning API ---

from app.connectors.db_connector import db_connector
from app.scheduler.worker import scan_datasource_task

class MetadataPreviewRequest(BaseModel):
    target_type: str
    connection_string: str

@router.post("/metadata/preview")
def preview_metadata(request: MetadataPreviewRequest, current_user: User = Depends(get_current_user)):
    """
    Synchronously fetches metadata (tables, columns, buckets) from the target.
    Useful for previewing what will be scanned.
    """
    results = []
    try:
        # Map target_type to connector type
        type_key = request.target_type
        if type_key == 'database': type_key = 'postgresql'
        
        # Connection string handling
        conn_str = request.connection_string
        
        # If target_path style (conn|table), split it
        if "|" in conn_str and type_key in ['postgresql', 'mysql']:
            conn_str = conn_str.split("|")[0]

        results = db_connector.get_schema_metadata(type_key, conn_str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    return {"status": "success", "metadata": results}

@router.post("/metadata/{config_id}")
def trigger_metadata_scan(config_id: int, current_user: User = Depends(get_current_user)):
    """
    Triggers a background Metadata Scan for a specific config.
    """
    task = scan_datasource_task.delay(config_id, "metadata")
    return {"status": "triggered", "task_id": str(task.id), "scan_scope": "metadata"}

