
from pathlib import Path
import re
import logging
from app.engine.scanner import scanner_engine
from app.connectors.file_scanner import file_scanner
from app.engine.aggregator import ScanResultAggregator

logger = logging.getLogger(__name__)

class UnifiedScanner:
    def __init__(self):
        self.aggregator = ScanResultAggregator()

    def _extract_context_from_name(self, name: str) -> list[str]:
        """Derives context keywords from filenames, sheet names, or column headers."""
        if not name: return []
        tokens = re.split(r'[^a-zA-Z0-9]', name)
        return [t.lower() for t in tokens if len(t) > 2]

    def reset_aggregator(self):
        self.aggregator = ScanResultAggregator()

    def scan_file(self, file_path_str: str):
        path = Path(file_path_str)
        filename = path.name
        
        # 1. Context from Filename
        file_context = self._extract_context_from_name(filename)
        
        try:
            with open(file_path_str, "rb") as f:
                content = f.read()
            
            # 2. Extract content with metadata (Object Level support)
            chunks = file_scanner.extract_with_metadata(content, filename)
            
            for chunk in chunks:
                text = chunk.get("text", "")
                metadata = chunk.get("metadata", {})
                
                # 3. Analyze with Context
                chunk_context = file_context.copy()
                if "sheet" in metadata:
                    chunk_context.extend(self._extract_context_from_name(str(metadata["sheet"])))
                
                findings = scanner_engine.analyze_text(text, context=chunk_context)
                
                for f in findings:
                    # 4. Aggregate
                    location = {
                        "source": "file",
                        "file_path": str(file_path_str),
                        "file_name": filename,
                    }
                    location.update(metadata)
                    
                    self.aggregator.add_finding(f["type"], f["text"], location)
                    
        except Exception as e:
            logger.error(f"Error scanning file {file_path_str}: {e}")

    def scan_database_row(self, row_data: dict, table_name: str, db_name: str):
        table_context = self._extract_context_from_name(table_name)
        
        for col, val in row_data.items():
            if not val: continue
            val_str = str(val)
            
            col_context = table_context + self._extract_context_from_name(col)
            
            findings = scanner_engine.analyze_text(val_str, context=col_context)
            
            for f in findings:
                 location = {
                     "source": "database",
                     "database": db_name,
                     "table": table_name,
                     "field": col,
                 }
                 self.aggregator.add_finding(f["type"], f["text"], location)

    def get_results(self):
        return self.aggregator.get_report()

    def get_json_results(self):
        return self.aggregator.to_json()
