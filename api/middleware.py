import time
import structlog
from fastapi import Request
from api.db import get_session, engine
from api.models import AuditLog
from sqlmodel import Session
from datetime import datetime

logger = structlog.get_logger()

async def audit_logging_middleware(request: Request, call_next):
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    process_time = time.time() - start_time
    
    # Extract details
    client_ip = request.client.host
    endpoint = request.url.path
    method = request.method
    
    # Skip logging for health checks or static files if needed
    if endpoint == "/" or endpoint.startswith("/static"):
        return response

    # Mock user extraction (In real app, we decode JWT here or get from request state if set by Auth middleware)
    # Since this middleware runs BEFORE dependency injection in some stacks, we might need to be careful.
    # However, in FastAPI, we can often rely on dependencies. 
    # For a global middleware, we might inspect headers manually if we want to log EVERYTHING.
    user_email = "anonymous"
    auth_header = request.headers.get("Authorization")
    if auth_header:
         # Simple check to see if token exists, decoding properly would require more logic/overhead here
         user_email = "authenticated_user_placeholder"

    # Async logging to DB (Fire and forget or await)
    # Note: Creating a session inside middleware for every request can be heavy. 
    # Use a background task or careful session management.
    try:
        with Session(engine) as session:
            log_entry = AuditLog(
                timestamp=datetime.utcnow(),
                user_email=user_email,
                action=f"{method} {endpoint}",
                endpoint=endpoint,
                ip_address=client_ip,
                details=f"Status: {response.status_code}, Time: {process_time:.4f}s"
            )
            session.add(log_entry)
            session.commit()
    except Exception as e:
        logger.error("audit_log_failed", error=str(e))

    return response
