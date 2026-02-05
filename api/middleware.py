import time
import structlog
from fastapi import Request
from api.db import get_session, engine
from api.models import AuditLog
from sqlmodel import Session
from datetime import datetime
import jwt
from api.utils import SECRET_KEY, ALGORITHM

logger = structlog.get_logger()

def resolve_action_name(method: str, path: str) -> str:
    """Maps API endpoints to human-readable audit actions."""
    if "/auth/token" in path: return "User Login"
    if "/auth/register" in path: return "User Registration"
    
    if "/scan/file" in path: return "Scan File"
    if "/scan/text" in path: return "Scan Text Block"
    
    if "/config/rules" in path:
        if method == "POST": return "Create Detection Rule"
        if method == "DELETE": return "Delete Detection Rule"
        if method == "PUT": return "Update/Toggle Rule"
        if method == "GET": return "View Rules Configuration"
        
    if "/compliance" in path:
        if method == "POST": return "Register Data Asset"
        if method == "PUT": return "Update Asset Status"
        if method == "GET": return "View Data Catalogue"

    if "/audit" in path: return "View Audit Logs"
    
    return f"{method} {path}" # Fallback

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
    if endpoint == "/" or endpoint.startswith("/static") or endpoint.startswith("/favicon"):
        return response

    # 1. Extract User from Token
    user_email = "anonymous"
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            user_email = payload.get("sub", "unknown_user")
        except jwt.ExpiredSignatureError:
            user_email = "expired_token_user"
        except jwt.PyJWTError:
            user_email = "invalid_token_user"

    # 2. Resolve Human-Readable Action
    action_name = resolve_action_name(method, endpoint)

    # 3. Async logging to DB
    try:
        # Only log significant actions or non-GETs to reduce noise? 
        # User requested "audit log", implying ALL significant activity.
        # We'll log everything for now as per request.
        
        with Session(engine) as session:
            log_entry = AuditLog(
                timestamp=datetime.utcnow(),
                user_email=user_email,
                action=action_name,
                endpoint=endpoint,
                ip_address=client_ip,
                details=f"Status: {response.status_code}, Time: {process_time:.3f}s"
            )
            session.add(log_entry)
            session.commit()
    except Exception as e:
        # Don't fail the request just because logging failed
        logger.error("audit_log_failed", error=str(e))

    return response
