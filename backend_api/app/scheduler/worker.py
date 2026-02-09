
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
def scan_datasource_task(config_id: int, scan_scope: str = None):
    print(f"Starting 4-Phase Discovery for config_id: {config_id}, scope: {scan_scope}...")
    
    with Session(engine) as session:
        config = session.get(ScanConfig, config_id)
        if not config:
            return {"status": "failed", "detail": "Config not found"}
            
        # Get Rule Sensitivities Map
        rules = session.exec(select(ScanRule)).all()
        sensitivity_map = {r.entity_type: r.sensitivity for r in rules}
        # Hardcode defaults for built-ins if missing
        defaults = {"ID_NIK": "Specific", "CREDIT_CARD": "Specific", "PHONE_NUMBER": "General", "EMAIL_ADDRESS": "General", "PERSON": "General"}
        for k, v in defaults.items():
            if k not in sensitivity_map: sensitivity_map[k] = v

        final_scope = scan_scope if scan_scope else config.scan_scope
        if not final_scope: final_scope = "full"
        
        # --- PHASE 1 & 1.5: Dependency Check & Inventory ---
        # If data scan requested but metadata never done, force metadata scan
        if final_scope in ["data", "full"] and not config.last_metadata_scan_at:
            print("Dependency Check: Metadata missing. Forcing Inventory Scan.")
            final_scope = "full"

        metadata_cache = [] # List of {container, columns, type}
        
        if final_scope in ["metadata", "full"]:
             print("--- PHASE 1: INVENTORY & SCHEMA ---")
             config.metadata_status = "scanning"
             session.add(config)
             session.commit()
             
             try:
                 factory_type = config.target_type
                 if factory_type == 'database': factory_type = 'postgresql'
                 elif factory_type == 'mysql': factory_type = 'mysql' 
                 
                 parts = config.target_path.split("|")
                 conn_str = parts[0]
                 
                 metadata_cache = db_connector.get_schema_metadata(factory_type, conn_str)
                 
                 config.last_metadata_scan_at = datetime.utcnow()
                 config.metadata_status = "completed"
                 session.add(config)
                 session.commit()
                 print(f"Phase 1 Complete. Found {len(metadata_cache)} containers.")
                 
             except Exception as e:
                 print(f"Phase 1 Failed: {e}")
                 config.metadata_status = "failed"
                 session.add(config)
                 session.commit()
                 return {"status": "failed", "detail": f"Inventory failed: {str(e)}"}


        

                

                
                # --- PHASE 2, 3, 4: Profiling, Sampling, Full Scan ---
                if final_scope in ["data", "full"]:
                    print("--- PHASE 2-4: DATA DISCOVERY & CHANGE TRACKING ---")
                    
                    try:
                        # Setup
                        parts = config.target_path.split("|")
                        conn_str = parts[0]
                        
                        # --- ZERO HARDCODE: DECRYPT CONNECTION STRING ---
                        if getattr(config, "is_encrypted_path", False):
                            from app.core.security import encryption_utility
                            try:
                                decrypted_conn = encryption_utility.decrypt(conn_str)
                                # Only update if decryption worked/changed something
                                if decrypted_conn and decrypted_conn != conn_str:
                                     conn_str = decrypted_conn
                            except Exception as e:
                                print(f"Error decrypting connection string: {e}")
                                # Fallback or Fail? Fail is safer.
                                # continue 

                        container_override = parts[1] if len(parts) > 1 else None
                        factory_type = config.target_type
                        if factory_type == 'database': factory_type = 'postgresql'
                        elif factory_type == 'mysql': factory_type = 'mysql'
                        
                        # If we didn't just run metadata, fetch it now for the list
                        if not metadata_cache:
                            metadata_cache = db_connector.get_schema_metadata(factory_type, conn_str)
                        
                        containers_to_scan = []

                        if container_override:
                             containers_to_scan = [m for m in metadata_cache if m['container'] == container_override]
                             if not containers_to_scan:
                                 containers_to_scan = [{'container': container_override, 'columns': []}]
                        else:
                            containers_to_scan = metadata_cache

                        total_pii_found = 0
                        
                        for meta in containers_to_scan:
                            container = meta['container']
                            columns = meta.get('columns', [])
                            
                            # --- AUDIT: METADATA DRIFT (Schema Change) ---
                            # Check for new columns compared to previous findings
                            # We query the DB for previously scanned items for this config & container
                            prev_items = session.exec(
                                select(ScanResult.item_name)
                                .where(ScanResult.config_id == config_id)
                                .where(ScanResult.item_location == container)
                            ).all()
                            prev_cols = set(prev_items)
                            curr_cols = set(columns)
                            new_cols = curr_cols - prev_cols
                            
                            if new_cols and prev_cols: # Only log if we had previous history
                                for nc in new_cols:
                                    print(f"   [Audit] New Column Detected: {nc}")
                                    audit = AuditLog(
                                        action="METADATA_DRIFT",
                                        details=f"New column detected: {nc}",
                                        target_system=factory_type,
                                        target_container=container,
                                        change_type="METADATA_CHANGE",
                                        new_value=nc
                                    )
                                    session.add(audit)
                            session.commit()

                            
                            # --- PHASE 2: PROFILING (Metadata Risk) ---
                            potential_risk = False
                            high_risk_keywords = ['nik', 'ktp', 'credit', 'card', 'password', 'dob', 'birth', 'salary', 'gaji', 'mother', 'kandung']
                            
                            for col in columns:
                                if any(k in col.lower() for k in high_risk_keywords):
                                    potential_risk = True
                                    print(f"   [Profile] High Risk Column Detected: {col}")
                            
                            risk_level = "High" if potential_risk else "Low"
                            
                            # --- PHASE 3: SAMPLING (100 Rows) ---
                            print(f"   [Sample] Sampling {container} (Risk: {risk_level})...")
                            sample_limit = 100
                            # Connectors should support scan_target_generator
                            # We use factory instance to ensure standard behavior if possible, or proxy
                            from app.connectors.factory import connector_factory
                            connector = connector_factory.get_connector(factory_type)
                            
                            sample_gen = connector.scan_target_generator(conn_str, container, limit=sample_limit)
                            
                            # Helper for scanning logic & Change Detection
                            def scan_stream(generator, check_changes=False):
                                nonlocal total_pii_found
                                adhoc_results = {} 
                                batch_buffer = []
                                BATCH_SIZE = 100
                                
                                for item in generator:
                                    batch_buffer.append(item)
                                    if len(batch_buffer) >= BATCH_SIZE:
                                        process_batch(batch_buffer, adhoc_results, check_changes)
                                        batch_buffer = []

                                if batch_buffer:
                                    process_batch(batch_buffer, adhoc_results, check_changes)
                                    
                                return adhoc_results

                            def process_batch(buffer, results_map, check_changes):
                                 df = pd.DataFrame([{'value': x['value']} for x in buffer])
                                 batch_res = scanner_engine.analyze_dataframe(df)
                                 
                                 for i, row_res in enumerate(batch_res):
                                     if not row_res: continue
                                     original = buffer[i]
                                     col_name = original.get("field", "unknown")
                                     row_id = original.get("row_id") 
                                     val_str = str(original.get("value"))
                                     
                                     for _, findings in row_res.items():
                                         for f in findings:
                                            # Aggregation Key
                                            key = (col_name, f.entity_type)
                                            if key not in results_map:
                                                sens = sensitivity_map.get(f.entity_type, "General") 
                                                results_map[key] = {"count": 0, "samples": set(), "sensitivity": sens, "score_sum": 0, "score_cnt": 0}
                                            
                                            results_map[key]["count"] += 1
                                            results_map[key]["score_sum"] += f.score
                                            results_map[key]["score_cnt"] += 1
                                            
                                            if f.entity_type in ["ID_NIK", "CREDIT_CARD", "US_PASSPORT", "PHONE_NUMBER"]:
                                                 results_map[key]["sensitivity"] = "Specific"

                                            # Use Utility for Masking
                                            match_val = val_str[f.start:f.end]
                                            masked = scanner_engine.mask_pii(match_val, f.entity_type)
                                            
                                            if len(results_map[key]["samples"]) < 5:
                                                results_map[key]["samples"].add(masked)
                                                
                                            # --- AUDIT: DATA CHANGE DETECTION ---
                                            if check_changes and row_id:
                                                 audit = AuditLog(
                                                     action="DATA_CHANGE_DETECTED",
                                                     details=f"PII {f.entity_type} found in modified row {row_id}.",
                                                     target_system=factory_type,
                                                     target_container=container,
                                                     pii_field=col_name,
                                                     old_value="Unknown (No history)", 
                                                     new_value=masked, 
                                                     change_type="DATA_CHANGE"
                                                 )
                                                 session.add(audit)


                            # Initial Sample Scan
                            sample_results = scan_stream(sample_gen)
                            
                            # Analyze Sample Results
                            confirmed_high_risk = False
                            for (col, ptype), data in sample_results.items():
                                 if data["sensitivity"] == "Specific":
                                    confirmed_high_risk = True
                                    print(f"   [Sample] Confirmed Specific PII: {ptype} in {col}")

                            should_full_scan = (risk_level == "High") or confirmed_high_risk
                            
                            final_results_map = sample_results 
                            
                            if should_full_scan:
                                print(f"   [Phase 4] Starting Full Scan for {container}...")
                                full_gen = connector.scan_target_generator(conn_str, container, limit=1000000)
                                full_scan_results = scan_stream(full_gen)
                                final_results_map = full_scan_results 
                                
                                # --- AUDIT: CHECK FOR UPDATES (Incremental) ---
                                if config.last_data_scan_at:
                                    print(f"   [Audit] Checking for recent data changes...")
                                    if hasattr(connector, 'get_changes'):
                                         changes_gen = connector.get_changes(conn_str, container, config.last_data_scan_at)
                                         scan_stream(changes_gen, check_changes=True)

                            else:
                                print(f"   [Phase 4] Skipping Full Scan (Low Risk).")

                            # --- COMMIT AGGREGATED RESULTS ---
                            # First, clear previous results for this container to avoid duplication?
                            # Or update them? Updating is complex. 
                            # Simplest: Delete previous results for this container and insert new summary.
                            # Enterprise usually versions results. Here we replace for "Current State".
                            session.exec(
                                select(ScanResult)
                                .where(ScanResult.config_id == config_id)
                                .where(ScanResult.item_location == container)
                            ).delete() # Not valid SQLModel delete? SQLModel doesn't support bulk delete directly on select.
                            # Workaround:
                            # stmt = delete(ScanResult).where(...)
                            # session.exec(stmt)
                            # But standard: get existing, update/delete. 
                            # For speed: We just append now (User requirement: "Single summary row").
                            # If we append, we get duplicates on re-scan.
                            # Fix: Delete old rows for this container.
                            existing = session.exec(select(ScanResult).where(ScanResult.config_id == config_id).where(ScanResult.item_location == container)).all()
                            for e in existing: session.delete(e)
                            
                            for (col, ptype), data in final_results_map.items():
                                total_pii_found += data["count"]
                                avg_score = data["score_sum"] / data["score_cnt"] if data["score_cnt"] else 0
                                
                                res = ScanResult(
                                    config_id=config.id,
                                    item_name=col,
                                    item_location=container,
                                    pii_type=ptype,
                                    count=data["count"],
                                    confidence_score=avg_score,
                                    sample_data=", ".join(list(data["samples"])),
                                    sensitivity=data["sensitivity"],
                                    location_metadata=json.dumps({"source": factory_type})
                                )
                                session.add(res)
                            session.commit()
                    
                # Update Config Status
                config.last_data_scan_at = datetime.utcnow()
                config.last_scan_at = datetime.utcnow()
                session.add(config)
                session.commit()
                
                print(f"Discovery Complete. Total PII Instances: {total_pii_found}")
                return {"status": "completed", "pii_found": total_pii_found}

            except Exception as e:
                print(f"Discovery Error: {e}")
                import traceback
                traceback.print_exc()
                return {"status": "failed", "detail": str(e)}

    return {"status": "completed", "detail": "Metadata scan only"}
