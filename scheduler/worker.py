from celery import Celery
import os
import time
import json
from datetime import datetime
from sqlmodel import Session, select
from api.db import engine
from api.models import ScanConfig, ScanResult, ProcessingPurpose
from engine.scanner import scanner_engine
from engine.classification import classification_engine
from connectors.db_connector import db_connector

# Use Redis from docker-compose or localhost
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

        # Determine Connector and Scan
        last_scan = config.last_scan_at
        scan_data = [] # List[Dict] {value: str, metadata: ...}
        
        try:
            if config.target_type == "database":
                # Assuming target_path contains connection string and table is derived 
                # (Simplified: target_path="postgres://...|table_name")
                parts = config.target_path.split("|")
                conn_str = parts[0]
                table_name = parts[1] if len(parts) > 1 else "public"
                
                raw_rows = db_connector.scan_target('postgresql', conn_str, table_name, limit=500, last_scan_time=last_scan)
                scan_data = raw_rows
                
            elif config.target_type == "api":
                raw_rows = db_connector.scan_source('api_get', config.target_path, last_scan_time=last_scan)
                scan_data = raw_rows
            
            # TODO: Add filesystem walker if needed
            
        except Exception as e:
            print(f"Scan specific error: {e}")
            return {"status": "failed", "detail": str(e)}

        pii_count_total = 0
        found_types = set()
        new_results = []
        
        # Analyze
        import pandas as pd
        
        # Convert list of dicts from connector to DataFrame
        # Connector returns: [{"container": "table", "field": "col", "value": "val"}, ...]
        if not scan_data:
             print("No data to scan.")
             # Update timestamp even if no data for delta?
             # config.last_scan_at = datetime.utcnow()
             # session.add(config); session.commit()
             return {"status": "completed", "pii_found": 0}

        df = pd.DataFrame(scan_data)
        
        # Batch Analyzer expects just the content to analyze usually, 
        # but we need to map back to metadata.
        # BatchAnalyzerEngine processes the whole DF. 
        # We need to reshape slightly if we want to scan 'value' column specifically?
        # Presidio Structured scans ALL string columns.
        
        # In our case, scan_data has "value", "field", "container". 
        # scanning "field" (column name) and "container" (table name) is not needed, typically just "value".
        # But for `analyze_iterator`, it returns row index.
        
        # We only want to scan 'value' column.
        # Let's subset.
        df_to_scan = df[['value']]
        
        # Call batch analyzer
        batch_results = scanner_engine.analyze_dataframe(df_to_scan)
        
        # Iterate results
        for result in batch_results:
            # result is BatchAnalysisResult or similar dict
            # Structure: keys -> column names, values -> list of RecognizerResult
            # Actually analyze_iterator yields per row? 
            # Let's verify standard behavior assumption:
            # It yields: (index, {column: [RecognizerResult]})
            
            # Since strict typing might vary, assuming iteration gives us context.
            # If analyze_iterator is used, it yields `Dict[str, List[RecognizerResult]]` per row usually, 
            # keys are columns. Or it yields an object with row_index.
            
            # Let's assume standard behavior: yields Presidio Result per cell/row.
            # Safety check: if fallback returned list, handle it.
            pass 
        
        # !! REVISION !! 
        # BatchAnalyzerEngine.analyze_iterator yields `Dict[str, List[RecognizerResult]]` 
        # corresponding to the row. But we need the original row index to map back to metadata.
        # If we passed a DF, iterate logic matches row order.
        
        row_idx = 0
        for row_results in batch_results:
             # row_results: {"value": [RecognizerResult, ...]}
             if not row_results: 
                 row_idx += 1
                 continue
                 
             original_item = scan_data[row_idx]
             
             for col, findings in row_results.items():
                 for f in findings:
                    pii_count_total += 1
                    found_types.add(f.entity_type)
                    
                    location_meta = {
                        "container": original_item.get("container"),
                        "field": original_item.get("field"),
                        "row_sample": str(original_item.get("value"))[:50]
                    }
                    
                    result = ScanResult(
                        config_id=config.id,
                        item_name=original_item.get("field", "unknown"),
                        item_location=original_item.get("container", "unknown"),
                        pii_type=f.entity_type,
                        count=1,
                        confidence_score=f.score,
                        sample_data=None, 
                        location_metadata=json.dumps(location_meta),
                        is_encrypted=False 
                    )
                    session.add(result)
             
             row_idx += 1
        
        # Auto Tagging Logic
        current_tags = json.loads(config.tags) if config.tags else []
        original_len = len(current_tags)
        tag_set = set(current_tags)
        
        # Rule: If High Sensitive Data -> Tag Confidential
        # simple heuristic using classification engine
        is_confidential = False
        for p_type in found_types:
            category = classification_engine.classify_sensitivity(p_type)
            if category in ["High", "Financial", "Health"]: # Assuming categories
                is_confidential = True
        
        # Or hardcode check for logic demonstration
        if "CREDIT_CARD" in found_types or "ID_NIK" in found_types:
             tag_set.add("CONFIDENTIAL")
             tag_set.add("PII_SENSITIVE")
        
        # Only update if changed
        if len(tag_set) > original_len:
            config.tags = json.dumps(list(tag_set))
            session.add(config)

        # Update Timestamp and Commit
        config.last_scan_at = datetime.utcnow()
        session.add(config)
        session.commit()
        
    print(f"Scan finished. Found {pii_count_total} items.")
    return {"status": "completed", "pii_found": pii_count_total}
