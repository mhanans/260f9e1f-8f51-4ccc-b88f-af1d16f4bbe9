from fastapi import FastAPI
from api.routers import auth, scan, config, audit, compliance
from api.middleware import audit_logging_middleware
from starlette.middleware.base import BaseHTTPMiddleware

app = FastAPI(
    title="Data Discovery & Classification API",
    description="API for scanning, classifying, and managing data privacy (PII).",
    version="1.0.0"
)

# Register Middleware
app.add_middleware(BaseHTTPMiddleware, dispatch=audit_logging_middleware)

# Include Routers
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])
app.include_router(scan.router, prefix="/api/v1/scan", tags=["Scanning"])
app.include_router(config.router, prefix="/api/v1/config", tags=["Configuration"])
app.include_router(audit.router, prefix="/api/v1/audit", tags=["Audit Logs"])
app.include_router(compliance.router, prefix="/api/v1/compliance", tags=["Compliance & Detected Data"])


@app.get("/")
def read_root():
    return {"message": "Welcome to the Data Discovery System API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
