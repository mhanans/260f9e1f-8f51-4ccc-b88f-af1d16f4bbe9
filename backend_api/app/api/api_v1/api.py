
from fastapi import APIRouter

from app.api.api_v1.routers import auth, scan, config, audit, compliance

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(scan.router, prefix="/scan", tags=["Scanning"])
api_router.include_router(config.router, prefix="/config", tags=["Configuration"])
api_router.include_router(audit.router, prefix="/audit", tags=["Audit Logs"])
api_router.include_router(compliance.router, prefix="/compliance", tags=["Compliance & Data Catalogue"])
