import sys
import os
from pathlib import Path

print(f"CWD: {os.getcwd()}")
print(f"Files in CWD: {os.listdir('.')}")

root_path = os.getcwd() # Assuming we run from root
sys.path.insert(0, root_path)

print("Attempting to import connectors...")
try:
    import connectors
    print(f"Imported connectors: {connectors}")
    print(f"Connectors file: {connectors.__file__}")
except ImportError as e:
    print(f"Failed to import connectors: {e}")

print("Attempting to import connectors.s3_connector...")
try:
    from connectors.s3_connector import s3_connector
    print(f"Successfully imported s3_connector: {s3_connector}")
except Exception as e:
    print(f"Failed to import s3_connector: {e}")

print("Attempting to import engine.scanner...")
try:
    from engine.scanner import scanner_engine
    print(f"Successfully imported scanner_engine")
except Exception as e:
    print(f"Failed to import scanner_engine: {e}")
