
from collections import defaultdict
from typing import List, Dict, Any
import json

class ScanResultAggregator:
    def __init__(self):
        # Structure: { PII_TYPE: { "total": 0, "samples": set(), "locations": [] } }
        self.results = defaultdict(lambda: {"total": 0, "samples": set(), "locations": []})
        self.findings_count = 0

    def add_finding(self, pii_type: str, text: str, location: Dict[str, Any]):
        """
        Adds a single finding to the report.
        location: dict with keys like 'source', 'path', 'sheet', 'row', 'table', 'column'
        """
        entry = self.results[pii_type]
        entry["total"] += 1
        
        # Keep up to 5 unique samples for display
        if len(entry["samples"]) < 5: 
            entry["samples"].add(text)
        
        entry["locations"].append(location)
        self.findings_count += 1

    def get_report(self) -> Dict[str, Any]:
        # Convert sets to lists for JSON serialization
        final_report = {}
        for pii_type, data in self.results.items():
            final_report[pii_type] = {
                "total_count": data["total"],
                "unique_identifiers": list(data["samples"]),
                "locations": data["locations"] # This contains the "capture at object level"
            }
        return final_report

    def to_json(self):
        return json.dumps(self.get_report(), indent=2)
