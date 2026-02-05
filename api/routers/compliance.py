from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from typing import List
from api.db import get_session
from api.models import DetectedData, User
from api.deps import get_current_user

router = APIRouter()

@router.get("/", response_model=List[DetectedData])
def get_detected_data(session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    """Returns all saved compliance data."""
    return session.exec(select(DetectedData).order_by(DetectedData.timestamp.desc())).all()

@router.post("/", response_model=DetectedData)
def save_detected_data(data: DetectedData, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    """Saves a confirmed data finding as metadata."""
    # Check if exactly same finding exists to avoid dupes?
    # Logic: Same Source, Location, Type.
    existing = session.exec(select(DetectedData).where(
        DetectedData.source == data.source,
        DetectedData.location == data.location,
        DetectedData.pii_type == data.pii_type
    )).first()
    
    if existing:
        # Update existing record logic if needed, or return it
        # User might want to update purpose
        existing.purpose = data.purpose or existing.purpose
        existing.confidence_score = data.confidence_score
        existing.status = data.status
        session.add(existing)
        session.commit()
        session.refresh(existing)
        return existing

    session.add(data)
    session.commit()
    session.refresh(data)
    return data

@router.put("/{data_id}", response_model=DetectedData)
def update_detected_data(data_id: int, update_data: DetectedData, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    existing = session.get(DetectedData, data_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Data record not found")
    
    existing.purpose = update_data.purpose
    existing.status = update_data.status
    existing.sensitivity = update_data.sensitivity
    
    session.add(existing)
    session.commit()
    session.refresh(existing)
    return existing

@router.delete("/{data_id}")
def delete_detected_data(data_id: int, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    data = session.get(DetectedData, data_id)
    if not data:
        raise HTTPException(status_code=404, detail="Data record not found")
    session.delete(data)
    session.commit()
    return {"status": "deleted"}
