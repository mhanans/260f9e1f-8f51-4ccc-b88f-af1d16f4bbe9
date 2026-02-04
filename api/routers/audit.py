from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, select
from api.db import get_session
from api.models import AuditLog, User
from api.deps import get_current_user
from typing import List, Optional

router = APIRouter()

@router.get("/", response_model=List[AuditLog])
def get_audit_logs(
    skip: int = 0, 
    limit: int = 100, 
    user_email: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    query = select(AuditLog)
    if user_email:
        query = query.where(AuditLog.user_email == user_email)
    
    query = query.order_by(AuditLog.timestamp.desc()).offset(skip).limit(limit)
    return session.exec(query).all()
