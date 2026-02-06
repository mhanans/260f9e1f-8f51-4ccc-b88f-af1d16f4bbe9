
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
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    legal_basis: Optional[str] = None 

class ScanConfig(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    target_type: str # "database", "filesystem", "s3", "datalake"
    target_path: str 
    active: bool = Field(default=True)
    tags: Optional[str] = None 
    last_scan_at: Optional[datetime] = None
    purpose_id: Optional[int] = Field(default=None, foreign_key="processingpurpose.id")

class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    user_email: Optional[str] = None
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
    location_metadata: Optional[str] = None 
    is_encrypted: bool = Field(default=False)

class ScanRule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    rule_type: str 
    pattern: str 
    score: float = Field(default=0.5)
    entity_type: str 
    is_active: bool = Field(default=True)
    context_keywords: Optional[str] = None

class DetectedData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str 
    location: str 
    pii_type: str 
    sensitivity: str 
    purpose: Optional[str] = None 
    confidence_score: float
    status: str = Field(default="Active")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
