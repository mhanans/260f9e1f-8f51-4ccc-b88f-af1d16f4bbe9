from sqlmodel import SQLModel
from api.db import engine
# Import all models to ensure they are registered in metadata
from api.models import User, ScanConfig, AuditLog, ScanResult, ScanRule, DetectedData, ProcessingPurpose

def update_schema():
    print("Updating Database Schema...")
    SQLModel.metadata.create_all(engine)
    print("Schema Updated.")

if __name__ == "__main__":
    update_schema()
