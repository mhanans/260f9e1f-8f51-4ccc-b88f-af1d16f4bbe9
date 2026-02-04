from typing import Optional, List
from datetime import datetime
from sqlmodel import Field, SQLModel, Relationship

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    hashed_password: str
    role: str = Field(default="user") # "admin", "dpo", "user"
    is_active: bool = Field(default=True)

class ProcessingPurpose(SQLModel, table=True):
    """
    ROPA: Record of Processing Activities.
    Defines WHY data is being processed (e.g., 'Credit Card Delivery').
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    legal_basis: Optional[str] = None # e.g. "Consent", "Legitimate Interest"

class ScanConfig(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    target_type: str # "database", "filesystem", "s3", "datalake"
    target_path: str 
    active: bool = Field(default=True)
    tags: Optional[str] = None # JSON list of strings e.g. ["CONFIDENTIAL", "GDPR"]
    last_scan_at: Optional[datetime] = None
    purpose_id: Optional[int] = Field(default=None, foreign_key="processingpurpose.id")

class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_id: Optional[int] = Field(default=None, foreign_key="user.id") # Nullable for system events
    user_email: Optional[str] = None # Redundancy for easy reading
    action: str
    endpoint: Optional[str] = None
    ip_address: Optional[str] = None
    details: Optional[str] = None

class ScanResult(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    config_id: int = Field(foreign_key="scanconfig.id")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    item_name: str 
    item_location: str
    pii_type: str 
    count: int
    confidence_score: float = Field(default=0.0)
    sample_data: Optional[str] = None 
    location_metadata: Optional[str] = None # JSON string: {"page": 1, "row": 2, "sheet": "Sheet1"}
    is_encrypted: bool = Field(default=False) # Security check

class ScanRule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    rule_type: str # "regex", "deny_list", "keyword"
    pattern: str # The regex string or keyword
    score: float = Field(default=0.5)
    entity_type: str # "CUSTOM_ID", "CONFIDENTIAL_HEADER"
    is_active: bool = Field(default=True)
    context_keywords: Optional[str] = None # JSON list of context words
