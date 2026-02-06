
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.api.api_v1.api import api_router
from app.api.middleware import audit_logging_middleware
from app.core.db import init_db

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    description="Backend API for Data Discovery & Classification (.NET Integration)",
    version="2.0.0"
)

# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_middleware(BaseHTTPMiddleware, dispatch=audit_logging_middleware)

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
def on_startup():
    # In a real prod env, use Alembic. 
    # For now, we rely on sql/schema.sql being run manually or by deployment pipeline.
    # init_db() # Optional: Auto-create tables if needed using Code First (user preferred Database First)
    pass

@app.get("/")
def read_root():
    return {"message": "Data Discovery Backend API is Running", "docs": "/docs"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
