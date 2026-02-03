from celery import Celery
import os
import time
from engine.scanner import scanner_engine

# Use Redis from docker-compose or localhost
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery_app = Celery("scheduler", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

@celery_app.task(name="tasks.scan_database")
def scan_database_task(connection_string, table_name):
    print(f"Starting scan for {table_name}...")
    # Simulation of DB scan
    # In real world: Use sqlalchemy to fetch rows -> loop -> scanner_engine.analyze_text
    time.sleep(5) 
    print(f"Scan finished for {table_name}")
    return {"status": "completed", "files_scanned": 100, "pii_found": 5}
