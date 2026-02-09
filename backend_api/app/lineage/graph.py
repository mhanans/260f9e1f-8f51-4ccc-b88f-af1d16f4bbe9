
import structlog
from typing import List, Dict, Any
from sqlmodel import Session, select
from app.core.db import engine
from app.models.all_models import ScanConfig, ScanResult

logger = structlog.get_logger()

class LineageEngine:
    def __init__(self):
        pass

    def get_data_mapping(self) -> List[Dict[str, Any]]:
        """
        Returns a flat list of Data Mapping records for UU PDP compliance.
        Format mirrors: SystemSource, DataSubject, PersonalDataCategory, Table, Field, Encrypted, Stage.
        """
        mapping_records = []
        
        try:
            with Session(engine) as session:
                # Query all configs and their results
                # Efficient: Load all results with config relation?
                # SQLModel supports relations but let's do explicitly join or loop for simplicity if small scale.
                # Or simply select ScanResult and get config lazy.
                
                results = session.exec(select(ScanResult)).all()
                
                # Cache configs to avoid N+1 queries
                configs = {c.id: c for c in session.exec(select(ScanConfig)).all()}
                
                for res in results:
                    config = configs.get(res.config_id)
                    sys_source = config.name if config else "Unknown"
                    target_type = config.target_type if config else "unknown"
                    
                    # 1. Lifecycle Stage Heuristic
                    stage = "Processing"
                    if target_type in ['s3', 'filesystem', 'file']:
                        stage = "Collection"
                    elif target_type in ['api', 'kafka']:
                        stage = "Transfer"
                    elif target_type in ['database', 'postgresql', 'mysql', 'mongo']:
                        stage = "Processing"
                    
                    # 2. Data Subject Heuristic
                    subject = "General"
                    loc_lower = res.item_location.lower()
                    if "user" in loc_lower or "customer" in loc_lower: subject = "Customer"
                    elif "emp" in loc_lower or "staff" in loc_lower: subject = "Employee"
                    
                    record = {
                        "SystemSource": sys_source,
                        "DataSubject": subject,
                        "PersonalDataCategory": res.pii_type,
                        "SourceTableName": res.item_location,
                        "FieldName": res.item_name,
                        "IsEncrypted": res.is_encrypted,
                        "DataLifeCycleStage": stage,
                        "RiskLevel": "High" if res.sensitivity == "Specific" else "Low"
                    }
                    mapping_records.append(record)
                    
        except Exception as e:
            logger.error(f"Error building data mapping: {e}")
            
        return mapping_records

lineage_engine = LineageEngine()
