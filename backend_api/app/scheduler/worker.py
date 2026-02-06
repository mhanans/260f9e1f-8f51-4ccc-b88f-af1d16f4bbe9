
from celery import Celery
import os
import json
from datetime import datetime
from sqlmodel import Session
from app.core.db import engine
from app.models.all_models import ScanConfig, ScanResult, ProcessingPurpose
from app.engine.scanner import scanner_engine
from app.engine.classification import classification_engine
from app.connectors.db_connector import db_connector

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery_app = Celery("scheduler", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

@celery_app.task(name="tasks.scan_datasource")
def scan_datasource_task(config_id: int):
    print(f"Starting scan for config_id: {config_id}...")
    
    with Session(engine) as session:
        config = session.get(ScanConfig, config_id)
        if not config:
            print("Config not found.")
            return {"status": "failed", "detail": "Config not found"}

        last_scan = config.last_scan_at
        scan_data = [] 
        
        try:
            if config.target_type == "database":
                parts = config.target_path.split("|")
                conn_str = parts[0]
                table_name = parts[1] if len(parts) > 1 else "public"
                
                raw_rows = db_connector.scan_target('postgresql', conn_str, table_name, limit=500, last_scan_time=last_scan)
                scan_data = raw_rows
                
            elif config.target_type == "api":
                raw_rows = db_connector.scan_source('api_get', config.target_path, last_scan_time=last_scan)
                scan_data = raw_rows
            
        except Exception as e:
            print(f"Scan specific error: {e}")
            return {"status": "failed", "detail": str(e)}

        pii_count_total = 0
        found_types = set()
        
        import pandas as pd
        
        if not scan_data:
             print("No data to scan.")
             return {"status": "completed", "pii_found": 0}

        df = pd.DataFrame(scan_data)
        df_to_scan = df[['value']]
        
        batch_results = scanner_engine.analyze_dataframe(df_to_scan)
        
        row_idx = 0
        for row_results in batch_results:
             if not row_results: 
                 row_idx += 1
                 continue
                 
             original_item = scan_data[row_idx]
             
             for col, findings in row_results.items():
                 for f in findings:
                    pii_count_total += 1
                    found_types.add(f.entity_type)
                    
                    full_val_str = str(original_item.get("value", ""))
                    detected_val = full_val_str[f.start:f.end]
                    
                    sample_payload = None
                    if f.entity_type == "PERSON":
                        sample_payload = detected_val
                    
                    location_meta = {
                        "container": original_item.get("container"),
                        "field": original_item.get("field"),
                        "row_sample": full_val_str[:50]
                    }
                    
                    result = ScanResult(
                        config_id=config.id,
                        item_name=original_item.get("field", "unknown"),
                        item_location=original_item.get("container", "unknown"),
                        pii_type=f.entity_type,
                        count=1,
                        confidence_score=f.score,
                        sample_data=sample_payload, 
                        location_metadata=json.dumps(location_meta),
                        is_encrypted=False 
                    )
                    session.add(result)
             
             row_idx += 1
        
        # Auto Tagging
        try:
            current_tags = json.loads(config.tags) if config.tags else []
        except:
            current_tags = []
            
        original_len = len(current_tags)
        tag_set = set(current_tags)
        
        is_confidential = False
        for p_type in found_types:
            category = classification_engine.classify_sensitivity(p_type)
            if category in ["High", "Financial", "Health"]:
                is_confidential = True
        
        if "CREDIT_CARD" in found_types or "ID_NIK" in found_types:
             tag_set.add("CONFIDENTIAL")
             tag_set.add("PII_SENSITIVE")
        
        if len(tag_set) > original_len:
            config.tags = json.dumps(list(tag_set))
            session.add(config)

        config.last_scan_at = datetime.utcnow()
        session.add(config)
        session.commit()
        
    print(f"Scan finished. Found {pii_count_total} items.")
    return {"status": "completed", "pii_found": pii_count_total}
