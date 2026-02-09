from fastapi import APIRouter

from app.api.api_v1.routers import auth, scan, config, compliance, scheduler, audit, lineage

api_router = APIRouter()
api_router.include_router(auth.router, tags=["Authentication"])
api_router.include_router(scan.router, prefix="/scan", tags=["Discovery & Scanning"])
api_router.include_router(compliance.router, prefix="/report", tags=["Compliance Reports"])
api_router.include_router(config.router, prefix="/config", tags=["Configuration"])
api_router.include_router(scheduler.router, prefix="/job", tags=["Job Scheduler"])
api_router.include_router(audit.router, prefix="/audit", tags=["Audit Log"])
api_router.include_router(lineage.router, prefix="/lineage", tags=["Data Mapping Lineage"])
