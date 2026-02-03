import os
import structlog
from pathlib import Path
from engine.scanner import scanner_engine
from connectors.file_scanner import file_scanner # Assuming this exists or we use raw text logic

# Configure logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.WriteLoggerFactory(file=open(LOG_DIR / "local_scan.log", "a")),
)
logger = structlog.get_logger()

BASE_DIR = Path("data_storage")

def scan_local_directory(directory: Path):
    """
    Example function to scan a local directory completely.
    This simulates the logic requested for a background worker.
    """
    if not directory.exists():
        logger.warning("directory_not_found", path=str(directory))
        return

    files = list(directory.glob("*"))
    logger.info("scan_started", total_files=len(files))

    for file_path in files:
        if file_path.is_file():
            try:
                # 1. Read File
                content = file_path.read_bytes()
                
                # 2. Extract Text (Simplified logic - relying on file_scanner connector if available, or just text)
                # For demo, assuming text-based or using the connector we have
                text_content = ""
                try:
                    text_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    # Fallback for binary/pdf would go here, e.g. using pypdf or similar
                    logger.warning("binary_file_skipped_in_simple_example", filename=file_path.name)
                    continue

                # 3. Analyze
                results = scanner_engine.analyze_text(text_content)
                
                # 4. Log Results
                if results:
                    logger.info("pii_detected", filename=file_path.name, count=len(results), findings=results)
                else:
                    logger.info("clean_file", filename=file_path.name)
            
            except Exception as e:
                logger.error("scan_failed", filename=file_path.name, error=str(e))

if __name__ == "__main__":
    # Ensure directory exists for demo
    BASE_DIR.mkdir(exist_ok=True)
    
    print(f"Scanning {BASE_DIR.absolute()}...")
    scan_local_directory(BASE_DIR)
    print("Scan complete. Check logs/local_scan.log")
