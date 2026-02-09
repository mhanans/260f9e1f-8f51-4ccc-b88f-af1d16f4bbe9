
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from app.core.db import engine
from app.models.all_models import ScanConfig
from app.scheduler.scheduler_service import scheduler_service
from app.scheduler.worker import scan_datasource_task
from typing import Optional
from pydantic import BaseModel

router = APIRouter()

class ScheduleRequest(BaseModel):
    config_id: int
    cron_expression: str
    scan_scope: str = "full" # metadata, data, full
    timezone: str = "UTC"

@router.post("/register")
def register_schedule(request: ScheduleRequest):
    with Session(engine) as session:
        config = session.get(ScanConfig, request.config_id)
        if not config:
            raise HTTPException(status_code=404, detail="Config not found")
        
        config.schedule_cron = request.cron_expression
        config.scan_scope = request.scan_scope
        config.schedule_timezone = request.timezone
        session.add(config)
        session.commit()
        
        scheduler_service.add_job(config.id, request.cron_expression, request.scan_scope, request.timezone)
        
    return {"status": "success", "message": f"Schedule registered for config {request.config_id}"}

@router.post("/remove/{config_id}")
def remove_schedule(config_id: int):
    with Session(engine) as session:
        config = session.get(ScanConfig, config_id)
        if not config:
            raise HTTPException(status_code=404, detail="Config not found")
        
        config.schedule_cron = None
        session.add(config)
        session.commit()
        
        scheduler_service.remove_job(config_id)

    return {"status": "success", "message": f"Schedule removed for config {config_id}"}

@router.post("/run/{config_id}")
def run_scan_manually(config_id: int, scan_scope: str = "full"):
    # Trigger Celery task directly
    task = scan_datasource_task.delay(config_id, scan_scope)
    return {"status": "triggered", "task_id": str(task.id)}
