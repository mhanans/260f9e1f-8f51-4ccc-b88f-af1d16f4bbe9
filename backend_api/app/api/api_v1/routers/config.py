
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from app.core.db import get_session
from app.models.all_models import ScanRule, User
from app.api.deps import get_current_user
from app.engine.scanner import scanner_engine
from app.engine.classification import classification_engine

router = APIRouter()

@router.get("/rules")
def get_rules(session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    return session.exec(select(ScanRule)).all()

@router.post("/rules")
def create_rule(rule: ScanRule, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    existing = session.exec(select(ScanRule).where(ScanRule.name == rule.name)).first()
    if existing:
        raise HTTPException(status_code=400, detail="Rule name already exists")
    
    session.add(rule)
    session.commit()
    session.refresh(rule)
    
    scanner_engine.reload_rules()
    classification_engine.load_config()
    
    return rule

@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    rule = session.get(ScanRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    session.delete(rule)
    session.commit()
    
    scanner_engine.reload_rules()
    classification_engine.load_config()
    
    return {"status": "deleted"}

@router.put("/rules/{rule_id}")
def toggle_rule(rule_id: int, is_active: bool, session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    rule = session.get(ScanRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    rule.is_active = is_active
    session.add(rule)
    session.commit()
    session.refresh(rule)
    
    scanner_engine.reload_rules()
    
    return rule
